from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
import requests, json, yaml

# 建立 Embedding 模型
embedding = HuggingFaceEmbeddings(model_name="DMetaSoul/Dmeta-embedding-zh-small")

# # 載入 YAML
def open_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # # 轉成 Document
    api_docs = [
        Document(page_content=doc["content"], metadata={"api": doc["api"]})
        for doc in data["api_docs"]
    ]
    return  api_docs

# 假設你的文件（從 config 轉來的）
# api_docs = [
#     Document(page_content="列出所有資料庫", metadata={"api": "/list_databases"}),
#     Document(page_content="列出所有資料表", metadata={"api": "/list_tables"}),
#     Document(page_content="描述某個資料表的 schema", metadata={"api": "/describe_table"}),
#     Document(page_content="查詢資料表資料", metadata={"api": "/query_table"}),
#     Document(page_content="刪除某個資料庫", metadata={"api": "/delete_database"}),
#     Document(page_content="統計漏洞總數及依嚴重性分類", metadata={"api": "/query_vulnerability_summary"}),
#     Document(page_content="列出高嚴重性漏洞的檔案", metadata={"api": "/query_high_severity_files"}),
#     Document(page_content="統計 CWE 資料", metadata={"api": "/query_cwe_summary"}),
#     Document(page_content="根據 check_id 說明漏洞細節", metadata={"api": "/explain_vulnerability_by_check_id"}),
#     Document(page_content="列出指定 CWE 的所有漏洞檔案及數量", metadata={"api": "/query_vulnerabilities_by_cwe"}),
#     Document(page_content="建議漏洞修復方式", metadata={"api": "/suggest_fix"}),
# ]

api_docs = open_file("api_docs.yaml")
# 建立 VectorStore
vector_store = FAISS.from_documents(api_docs, embedding)



def call_api(api_path, payload=None):
    url = f"http://192.168.42.200:38001{api_path}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eventest"
    }
    payload = payload or {}

    while True:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()

        try:
            error_data = response.json()
        except Exception:
            return f"API 呼叫失敗: {response.status_code}, {response.text}"

        # 專門處理缺少參數錯誤
        if response.status_code == 422 and "detail" in error_data:
            missing_fields = [
                d["loc"][-1]
                for d in error_data["detail"]
                if d.get("type") == "missing"
            ]
            if missing_fields:
                print("API 缺少必要參數，請補充以下欄位：")
                for field in missing_fields:
                    value = input(f"請輸入 {field}: ")
                    payload[field] = value
                # loop 會再自動重試
                continue

        # 其他錯誤就直接回傳
        return f"API 呼叫失敗: {response.status_code}, {response.text}"


while True:
    query = input("請輸入你的問題 (輸入 exit 離開)：")
    if query.lower() == "exit":
        break

    if query.lower() == "add file":
        fileName = input("請輸入檔案名稱: ")
        api_docs = open_file(fileName)
        vector_store.add_documents(api_docs)
        continue

    # RAG 檢索最相關文件
    retrieved_docs = vector_store.similarity_search(query, k=1)
    api_path = retrieved_docs[0].metadata["api"]
    print("api_path:", api_path)
    # 這裡可以加邏輯決定 payload
    payload = {}

    result = call_api(api_path, payload)
    print("結果:", json.dumps(result, ensure_ascii=False, indent=2))

# # 測試輸入問題
# query = "我需要CWE 資料"
# retrieved_docs = vector_store.similarity_search(query, k=1)
#
# api_path = retrieved_docs[0].metadata["api"]
# # 回傳 API
# print("找到的 API:", api_path)
#
# # 如果找到的是 MCPO tool 的 API，呼叫並帶 Authorization
# if api_path == "/query_cwe_summary":
#     url = f"http://192.168.42.200:38001{api_path}"
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": "Bearer eventest",  # 你的 token
#     }
#     response = requests.post(url, headers=headers, json={})
#     if response.status_code == 200:
#         data = response.json()
#         print("CWE 資料:", data)
#     else:
#         print(f"API 呼叫失敗: {response.status_code}, {response.text}")
