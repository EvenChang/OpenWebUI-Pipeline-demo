"""
title: FAISS + Ollama RAG Pipeline
author: even
date: 2025-09-11
version: 1.1
license: MIT
description: RAG pipeline with Ollama LLM fallback and session management for missing API parameters.
requirements: langchain-community, langchain-huggingface, sentence-transformers, faiss-cpu, requests
"""
from typing import List, Union, Generator, Iterator

from openai import OpenAI
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from pydantic import BaseModel
import os, requests, json
from langchain.memory import ConversationBufferWindowMemory

class Pipeline:

    class Valves(BaseModel):
        pass

    def __init__(self):
        self.valves = self.Valves()
        self.vector_store = None
        self.memory = ConversationBufferWindowMemory(k=1)

    async def on_startup(self):
        print("on-startup")
        try:
            embedding = HuggingFaceEmbeddings(model_name="DMetaSoul/Dmeta-embedding-zh-small")
            index_path = "/app/pipelines/faiss_index"

            if not os.path.exists(index_path):
                print(f"找不到向量索引: {index_path}")
                self.vector_store = None
            else:
                self.vector_store = FAISS.load_local(
                    index_path,
                    embedding,
                    allow_dangerous_deserialization=True,
                )
                print("on_startup")
        except Exception as e:
            print(f"on_startup 執行錯誤: {e}")
            return f"發生錯誤: {e}"

    async def on_shutdown(self):
        pass


    def call_ollama(self, system_prompt: str, user_message: str):
        """
        呼叫本地 Ollama LLM
        """
        client = OpenAI(
            base_url='http://192.168.42.200:11434/v1',
            api_key='ollama',  # required, but unused
        )
        history = self.memory.load_memory_variables({}).get("history", [])
        print("history", history)

        messages = [{"role": "system", "content": system_prompt}]
        if history:
            messages.append({"role": "user", "content": history})
        messages.append({"role": "user", "content": user_message})

        resp = client.chat.completions.create(
            model="llama3.2:latest",
            messages=messages,
            temperature=0,
            # messages=[
            #     {"role": "system", "content": system_prompt, "history": history},
            #     {"role": "user", "content": user_message},
            # ]
        )

        try:
            if resp and resp.choices and len(resp.choices) > 0:
                llm_content = resp.choices[0].message.content
                print("Ollama 回覆:", llm_content)

                self.memory.chat_memory.add_user_message(user_message)
                self.memory.chat_memory.add_ai_message(llm_content)

                try:
                    parsed_content = json.loads(llm_content)

                    if "missing_params" in parsed_content:
                        return parsed_content

                    if "api" in parsed_content and "params" in parsed_content:
                        api_path = parsed_content["api"]
                        payload = parsed_content.get("params", {})
                        return self.call_llm_api(api_path, payload)

                    return {"error": "Unexpected LLM response format", "raw": parsed_content}


                except json.JSONDecodeError:
                    return llm_content

        except Exception as e:
            return f"Ollama 呼叫錯誤: {e}"

    def call_llm_api(self, api_path, payload=None):

        print("call llm api")
        url = f"http://192.168.42.200:38001{api_path}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer eventest"
        }
        payload = payload or {}
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"response.json : {response.json()}")
            return response.json()
        return f"API 呼叫失敗: {response.status_code}, {response.text}"


    def call_api(self, api_path, payload=None, user_message=None):
        url = f"http://192.168.42.200:38001{api_path}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer eventest"
        }
        payload = payload or {}
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()

        try:
            error_data = response.json()
        except Exception:
            return f"API 呼叫失敗: {response.status_code}, {response.text}"

        if response.status_code == 422 and "detail" in error_data:
            print("detail:", error_data["detail"])
            system_prompt = (
                f"You are a smart assistant that helps the user call an MCP API.\n"
                "Analyze the 'detail' message to figure out if the context has arguments for the API.\n"
                f"API path: {api_path}\n"
                f"Error data: {json.dumps(error_data)}\n"
                "Your task:\n"
                "1. If you can infer all required parameters, return JSON in the format:\n"
                '{ "api": "<api_path>", "params": {"<param_name>": "<value>"}} \n'
                "2. If you cannot infer the parameter values, DO NOT return empty strings or placeholders.\n"
                "   Instead, return JSON in this format:\n"
                '{ "missing_params": ["<param1>", "<param2>", ...], "message": "Please provide these parameters." }\n'
                "Rules:\n"
                "- Never invent or guess parameter values.\n"
                "- Never return empty string values.\n"
                "- Only return valid JSON (no markdown, no explanation).\n"
            )

            print(f"system_prompt: {system_prompt}")


            ollama_response = self.call_ollama(system_prompt, user_message)
            print(f"ollama_response: {ollama_response}")
            return ollama_response

        return f"API 呼叫失敗: {response.status_code}, {response.text}"

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        try:

            print("pipe called")
            if self.vector_store is None:
                return "⚠️ 向量索引尚未初始化，無法搜尋知識庫。"

            retrieved_docs = self.vector_store.similarity_search(user_message, k=1)
            if not retrieved_docs:
                return f"沒有找到相關知識，無法回答: {user_message}"

            doc = retrieved_docs[0]
            api_path = doc.metadata["api"]

            payload = body.get("payload", {})  # 假設你讓 UI 傳 payload
            print(f"api_path: {api_path}, payload: {payload}, user_message: {user_message}")
            response = self.call_api(api_path, payload, user_message)

            return str(response)


        except Exception as e:
            print(f"pipe 執行錯誤: {e}")
            return f"發生錯誤: {e}"

