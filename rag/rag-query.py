from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
import requests, json, yaml

# 建立 Embedding 模型
embedding = HuggingFaceEmbeddings(model_name="DMetaSoul/Dmeta-embedding-zh-small")

# 建立 VectorStore
vector_store = FAISS.load_local("faiss_index", embedding, allow_dangerous_deserialization=True)

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

    # if query.lower() == "Add file":
    #     fileName = input("請輸入檔案名稱: ")
    #     api_docs = open_file(fileName)
    #     vector_store.add_documents(api_docs)
    #     continue

    # RAG 檢索最相關文件
    retrieved_docs = vector_store.similarity_search(query, k=1)
    api_path = retrieved_docs[0].metadata["api"]
    required_fields = retrieved_docs[0].metadata.get("required_fields", [])

 # 提示用戶輸入缺少的欄位
    payload = {}
    for field in required_fields:
        while field not in payload or not payload[field]:
            value = input(f"請輸入 {field}: ")
            if value.strip():  # 確保有輸入值
                payload[field] = value

    print("api_path:", api_path)
    print("payload:", payload)


    result = call_api(api_path, payload)
    print("結果:", json.dumps(result, ensure_ascii=False, indent=2))


    # # 載入 YAML
# def open_file(filename):
#     with open(filename, "r", encoding="utf-8") as f:
#         data = yaml.safe_load(f)

#     # # 轉成 Document
#     api_docs = [
#         Document(page_content=doc["content"], metadata={"api": doc["api"]})
#         for doc in data["api_docs"]
#     ]
#     return  api_docs