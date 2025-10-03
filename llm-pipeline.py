"""
title: FAISS + Ollama RAG Pipeline
author: even
date: 2025-09-11
version: 1.1
license: MIT
description: RAG pipeline with Ollama LLM fallback and session management for missing API parameters.
requirements: langchain-community, langchain-huggingface, sentence-transformers, faiss-cpu, requests, langchain-chroma
"""
from typing import List, Union, Generator, Iterator

from openai import OpenAI
# from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from pydantic import BaseModel, Field
import os, requests, json, copy
from langchain.memory import ConversationBufferWindowMemory
from langchain_chroma import Chroma
from chromadb.config import Settings


class Pipeline:

    class Valves(BaseModel):
        MCP_SERVER: str = Field(default="http://192.168.42.200:38001",
                                description="MCP Server address, e.g. 'http://192.168.42.200:38001'")
        MCP_SERVER_API_KEY: str = Field(default="eventest",
                                        description="MCP Server API KEY, e.g. 'apikey'")
        OPENAI_BASE_URL: str = Field(default="http://192.168.42.200:11434/v1",
                                     description="OpenAI base URL, e.g. 'http://192.168.42.200:11434/v1'")
        OPENAI_API_KEY: str = Field(default="ollama",
                                    description="OpenAI API key, e.g. 'ollama'."
                                                "If use Ollama the OpenAI API key is required but unused.")
        MODEL: str = Field(default="llama3.2:latest", description="Model name, e.g. 'gpt-oss:20b'")

    def __init__(self):

        # Initialize valve parameters
        self.valves = self.Valves(
            **{k: os.getenv(k, v.default) for k, v in self.Valves.model_fields.items()}
        )

        self.vector_store = None
        self.memory = ConversationBufferWindowMemory(k=1)

        self.type = "manifold"   # 宣告這是一個「多路」管線
        self.name = "Manifold: " # UI 上顯示的名稱 prefix

        self.pipelines = [
            {"id": "ollama", "name": "Ollama"},
            {"id": "deepseek", "name": "DeepSeek"},
            {"id": "openai", "name": "OpenAI"},
        ]

    async def on_startup(self):
        print("on-startup")
        try:
            embedding = HuggingFaceEmbeddings(model_name="DMetaSoul/Dmeta-embedding-zh-small")

            settings = Settings(
                chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
                chroma_client_auth_credentials="my-secret-token",
            )

            self.vector_store = Chroma(
                collection_name="documents_api_collection",
                embedding_function=embedding,
                host="192.168.40.112",
                port=7777,
                client_settings=settings,
            )

            print("on_startup")
        except Exception as e:
            print(f"on_startup 執行錯誤: {e}")
            return f"發生錯誤: {e}"

    async def on_shutdown(self):
        pass


    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:

        print("model_id:", model_id)

        if model_id == "ollama":
            try:
                print("pipe called")
                if self.vector_store is None:
                    return "⚠️ 向量索引尚未初始化，無法搜尋知識庫。"

                # retrieved_docs = self.vector_store.similarity_search(user_message, k=1)

                retrieved_docs = self.vector_store.similarity_search_with_score(user_message, k=1)
                print("retrieved_docs:", retrieved_docs)

                if not retrieved_docs:
                    return str(self.call_original_llm(user_message))

                doc, score = retrieved_docs[0]
                if score > 0.75:
                    print(f"相似度太低 ({score})，跳過 RAG，直接交給 LLM")
                    return str(self.call_original_llm(user_message))

                api_path = doc.metadata["endpoint"]

                payload = body.get("payload", {})  # 假設你讓 UI 傳 payload
                response = self.call_api(api_path, payload, user_message)

                if isinstance(response, (dict, list)):
                    return json.dumps(response, ensure_ascii=False, indent=2)
                return str(response)

            except Exception as e:
                print(f"pipe 執行錯誤: {e}")
                return str(self.call_original_llm(user_message))

        return f"LLM {model_id} not supported"

    def call_original_llm(self, user_message: str):
        openapi_schema = None
        try:
            openapi_url = self.valves.MCP_SERVER + "/openapi.json"
            response = requests.get(openapi_url)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            openapi_schema = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching OpenAPI schema: {e}")

        converter = OpenAPIConverter()
        tool_payload = converter.convert_openapi_to_tool_payload(openapi_schema)

        client = OpenAI(
            base_url=self.valves.OPENAI_BASE_URL,
            api_key=self.valves.OPENAI_API_KEY,  # required, but unused
        )
        history = self.memory.load_memory_variables({}).get("history", [])
        print("history", history)

        system_prompt = "You are a ChatGPT, you can discovery tools to answer user's questions."
        messages = [{"role": "system", "content": system_prompt}]
        if history:
            messages.append({"role": "user", "content": history})
        messages.append({"role": "user", "content": user_message})

        resp = client.chat.completions.create(
            model=self.valves.MODEL,
            messages=messages,
            tools=tool_payload,
            temperature=0,
        )

        print("resp:", resp)
        resp = self.call_tool_if_needed(client, resp, messages, tool_payload)

        llm_content = resp.choices[0].message.content
        self.memory.chat_memory.add_user_message(user_message)
        self.memory.chat_memory.add_ai_message(llm_content)
        print("call original LLM : ", llm_content)

        return llm_content

    def call_tool_if_needed(self, client, response, messages, tool_payload):
        final_text = list(messages)
        content = response.choices[0]

        try:
            # if the content including call tools message.
            if tool_calls := content.message.tool_calls:
                print(f"Processing {len(tool_calls)} tool calls")

                for tool_idx, tool_call in enumerate(tool_calls):

                    print(f"\n--- Processing tool call {tool_idx + 1}/{len(tool_calls)} ---")

                    try:
                        tool_name = tool_call.function.name
                        # OpenAI API use
                        api_path = tool_name.removeprefix("tool_").removesuffix("_post")
                        print(f"Tool name: {tool_name}")
                        args = json.loads(tool_call.function.arguments)
                        result = self.call_api(api_path, args)

                        final_text.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps(result)
                        })

                        resp = client.chat.completions.create(
                            model=self.valves.MODEL,
                            messages=final_text,
                            tools=tool_payload,
                        )

                        print("resp final text:", resp)

                        return resp

                    except Exception as e:
                        print(f"Error processing tool call: {str(e)}")
                    continue

        except Exception as e:
            print(f"Error in main processing loop: {str(e)}")

        # Return original response
        return response

    def call_ollama(self, system_prompt: str, user_message: str):

        """
        呼叫本地 Ollama LLM
        """

        try:
            openapi_url = self.valves.MCP_SERVER + "/openapi.json"
            response = requests.get(openapi_url)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching OpenAPI schema: {e}")

        client = OpenAI(
            base_url=self.valves.OPENAI_BASE_URL,
            api_key=self.valves.OPENAI_API_KEY,  # required, but unused
        )
        history = self.memory.load_memory_variables({}).get("history", [])
        print("Chat history : ", history)

        messages = [{"role": "system", "content": system_prompt}]
        if history:
            messages.append({"role": "user", "content": history})
        messages.append({"role": "user", "content": user_message})

        resp = client.chat.completions.create(
            model=self.valves.MODEL,
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
                        response_status_code, response_json = self.call_llm_api(api_path, payload)
                        if response_status_code == 200:
                            return response_json

                    return self.call_original_llm(user_message)

                except Exception as e:
                    print(f"LLM 回覆解析錯誤，fallback: {e}")
                    return self.call_original_llm(user_message)

        except Exception as e:
            return f"Ollama 呼叫錯誤: {e}"

    def call_llm_api(self, api_path, payload=None):

        print("call llm api")
        url = self.valves.MCP_SERVER + api_path
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.valves.MCP_SERVER_API_KEY
        }
        payload = payload or {}
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"response.json : {response.json()}")
            return response.status_code, response.json()

        return response.status_code, None

    def call_api(self, api_path, payload=None, user_message=None):

        if not api_path.startswith("/"):
            api_path = "/" + api_path

        url = self.valves.MCP_SERVER + api_path
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.valves.MCP_SERVER_API_KEY
        }
        payload = payload or {}
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"response.json : {response.json()}")
            return response.json()

        try:
            error_data = response.json()
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

                ollama_response = self.call_ollama(system_prompt, user_message)
                print(f"ollama_response: {ollama_response}")
                return ollama_response

        except Exception as e:
            print(f"API 呼叫失敗: {e}, {response.text}")

        return self.call_original_llm(user_message)


class OpenAPIConverter:
    def convert_openapi_to_tool_payload(self, openapi_spec):
        """
        Converts an OpenAPI specification into a custom tool payload structure.

        Args:
            openapi_spec (dict): The OpenAPI specification as a Python dict.

        Returns:
            list: A list of tool payloads.
        """
        tool_payload = []

        for path, methods in openapi_spec.get("paths", {}).items():
            for method, operation in methods.items():
                if operation.get("operationId"):
                    tool = {
                        "name": operation.get("operationId"),
                        "description": operation.get(
                            "description",
                            operation.get("summary", "No description available."),
                        ),
                        "parameters": {"type": "object", "properties": {}, "required": []},
                    }

                    # Extract path and query parameters
                    for param in operation.get("parameters", []):
                        param_name = param["name"]
                        param_schema = param.get("schema", {})
                        description = param_schema.get("description", "")
                        if not description:
                            description = param.get("description") or ""
                        if param_schema.get("enum") and isinstance(
                                param_schema.get("enum"), list
                        ):
                            description += (
                                f". Possible values: {', '.join(param_schema.get('enum'))}"
                            )
                        param_property = {
                            "type": param_schema.get("type"),
                            "description": description,
                        }

                        # Include items property for array types (required by OpenAI)
                        if param_schema.get("type") == "array" and "items" in param_schema:
                            param_property["items"] = param_schema["items"]

                        tool["parameters"]["properties"][param_name] = param_property
                        if param.get("required"):
                            tool["parameters"]["required"].append(param_name)

                    # Extract and resolve requestBody if available
                    request_body = operation.get("requestBody")
                    if request_body:
                        content = request_body.get("content", {})
                        json_schema = content.get("application/json", {}).get("schema")
                        if json_schema:
                            resolved_schema = self.resolve_schema(
                                json_schema, openapi_spec.get("components", {})
                            )

                            if resolved_schema.get("properties"):
                                tool["parameters"]["properties"].update(
                                    resolved_schema["properties"]
                                )
                                if "required" in resolved_schema:
                                    tool["parameters"]["required"] = list(
                                        set(
                                            tool["parameters"]["required"]
                                            + resolved_schema["required"]
                                        )
                                    )
                            elif resolved_schema.get("type") == "array":
                                tool["parameters"] = (
                                    resolved_schema  # special case for array
                                )
                    # wrapped to
                    wrapped_tool = {
                        "type": "function",
                        "function": tool
                    }

                    tool_payload.append(wrapped_tool)

        return tool_payload

    def resolve_schema(self, schema, components):
        """
        Recursively resolves a JSON schema using OpenAPI components.
        """
        if not schema:
            return {}

        if "$ref" in schema:
            ref_path = schema["$ref"]
            ref_parts = ref_path.strip("#/").split("/")
            resolved = components
            for part in ref_parts[1:]:  # Skip the initial 'components'
                resolved = resolved.get(part, {})
            return self.resolve_schema(resolved, components)

        resolved_schema = copy.deepcopy(schema)

        # Recursively resolve inner schemas
        if "properties" in resolved_schema:
            for prop, prop_schema in resolved_schema["properties"].items():
                resolved_schema["properties"][prop] = self.resolve_schema(
                    prop_schema, components
                )

        if "items" in resolved_schema:
            resolved_schema["items"] = self.resolve_schema(resolved_schema["items"], components)

        return resolved_schema

    def wrap_openapi_tool(self, tool):
        return {
            "type": "function",
            "function": tool
        }