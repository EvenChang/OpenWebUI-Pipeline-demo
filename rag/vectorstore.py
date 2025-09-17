from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
import yaml

# 建立 Embedding 模型
embedding = HuggingFaceEmbeddings(model_name="DMetaSoul/Dmeta-embedding-zh-small")

# # 載入 YAML
def open_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    api_docs = []
    for doc in data["api_docs"]:
        metadata = {
            "api": doc["api"],
        }
        api_docs.append(Document(page_content=doc["content"], metadata=metadata))
    return api_docs

api_docs = open_file("api_docs.yaml")
# 建立 VectorStore
vector_store = FAISS.from_documents(api_docs, embedding)
vector_store.save_local("faiss_index")


# 如果是要多個file，有幾種方式
# 1. 掃描整個資料夾，寫入到VectorStore裡面
# 2. 一個while process 等待 file input and save into VS
