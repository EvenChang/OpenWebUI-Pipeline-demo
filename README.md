# OpenWebUI Pipeline: RAG and MCP Tool Integration
This repository demonstrates how to use the OpenWebUI Pipeline to retrieve user answers through RAG.

After a RAG similarity search, the pipeline may return a similarity API call such as:
```
{
  "api": "/describe_table",
  "params": {"table": "scan_results"}
}
```

The pipeline then uses this response to call the Semgrep MCP Server, fetches the result, and returns it to the user via OpenWebUI.

If the MCP Server responds with an error message about missing arguments, the error will be redirected to the LLM, which will prompt the user to provide the missing arguments.

After the user provides the missing arguments, the pipeline combines the previous and current context, sends it to the LLM, and then uses the corrected response to call the Semgrep MCP Server.


## Feature
1. Supports multi-turn conversations to fill in missing arguments
2. Reduces token usage
3. Easily extendable RAG vector store via a YAML file


# MCP Tool (Use Semgrep as example)
This Semgrep MCP Server uses **Apache Iceberg** to structure Semgrep scan data and store metadata in **MinIO**.

There are two Python files:

1. **semgrep-to-iceberg.py**: Reads the `semgrep-json-report.json` file, structures the data, and stores it in MinIO using Apache Iceberg.  
2. **semgrep-mcp.py**: Provides MCP tools to query relevant information from the structured Semgrep data.  

## Requirements

1. Python >= 3.11

## Download and Installation


Please edit the `semgrep-to-iceberg.py`, `semgrep-mcp.py` to replace the minio ip address (s3.endpoint) with your own.

Please follow these steps:

1. Install required dependencies
```pip install -r requirements.txt```

2. Structure and store data into Mino

Please edit the `semgrep-to-iceberg.py` and replace the Minio ip address (s3.endpoint) to yours.

```python3 semgrep-to-iceberg.py```

3.Install uv
```curl -LsSf https://astral.sh/uv/install.sh | sh```

4. Execute `semgrep.sh` to create a virutal environment for running the Semgrep MCP Server
```
./semgrep.sh
```
5. Run MCP Server
```
cd semgrep
uvx mcpo --port <MCP_SERVER_PORT> --api-key <YOUR_API_KEY> -- uv run semgrep-mcp.py
ex:
uvx mcpo --port 38001 --api-key "eventest" -- uv run semgrep-mcp.py
```

# OpenWebUI

1. Add Open WebUI Helm Repository:
```
helm repo add open-webui https://open-webui.github.io/helm-charts
helm repo update
```
2.Install Open WebUI Chart:

```helm install openwebui open-webui/open-webui```

3.Verify Installation:

```kubectl get pods```

# RAG

Use `api_docs.yaml` to define the context for similarity search. The `vectorstore.py` loads api_docs.yaml, generates a vector store, and saves it in the faiss_index directory.

## Run and Installation
```
cd rag
pip install -r requirements.txt
python3 vectorstore.py
```

Make sure to copy the `faiss_index` directory into the pipeline process folder before running the pipeline.

```sudo kubectl cp faiss_index open-webui-pipelines-9749797b8-pv6jp:/app/pipelines```




# Pipeline

`llm-pipeline.py` is the OpenWebUI pipeline file. You can upload it through the OpenWebUI website.

** Note: Make sure to edit this file to replace the MCP server IP address with your own. **
