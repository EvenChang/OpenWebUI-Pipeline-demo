import json
import os
from pyspark import SparkConf
from pyspark import SparkContext
from mcp.server.fastmcp import FastMCP, Context
from pyspark.sql import SparkSession



# Initialize FastMCP server
mcp = FastMCP("semgrep-mcp")


def load_spark():
    # adding iceberg configs
    conf = (
        SparkConf()
        .set("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") # Use Iceberg with Spark
        .set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .set("spark.sql.catalog.demo.warehouse", "s3a://iceberg/warehouse/")
        .set("spark.sql.catalog.demo.s3.endpoint", "http://10.233.50.134:9000")
        .set("spark.sql.defaultCatalog", "demo") # Name of the Iceberg catalog
        .set("spark.sql.catalog.demo.defaultDatabase", "semgrep")
        .set("spark.sql.catalog.demo.type", "hadoop") # Iceberg catalog type
        .set("spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "org.apache.hadoop:hadoop-common:3.3.4",
                "software.amazon.awssdk:s3:2.32.21",
                "software.amazon.awssdk:sts:2.32.21"
            ])
        )
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Disable below line to see INFO logs
    spark.sparkContext.setLogLevel("ERROR")

    load_config(spark.sparkContext)

    return spark


def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "openpitrixminioaccesskey"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key",
                                                 os.getenv("AWS_SECRET_ACCESS_KEY", "openpitrixminiosecretkey"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://10.233.50.134:9000"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")


spark = None
def get_spark():
    global spark
    if spark is None:
        spark = load_spark()
        spark.catalog.setCurrentDatabase("semgrep")
    return spark

@mcp.tool()
def list_databases() -> list:
    """
    List all existing Spark/Iceberg databases.
    """
    spark = get_spark()
    return spark.sql("SHOW NAMESPACES IN demo").collect()

@mcp.tool()
def list_tables() -> list[str]:
    """
    Purpose: Discover which result tables exist (for example: semgrep_results, scan_summary, scan_metadata).

    Output: List of table names.

    Use case: Check what data sources are available before querying.
    """
    spark = get_spark()
    databases = [db.name for db in spark.catalog.listDatabases()]
    tables = []
    for db in databases:
        db_tables = [f"{db}.{t.name}" for t in spark.catalog.listTables(db)]
        tables.extend(db_tables)
    return tables


@mcp.tool()
def describe_table(table: str) -> str:
    """
    Purpose: Provide schema information for a given table.

    Args: Table name

    Output: Column names, types, and descriptions.

    Use case: Knows which fields it can query (e.g., file_path, severity, cwe_id, check_id, message).
    """
    spark = get_spark()
    df = spark.table(table)
    schema_json = df.schema.jsonValue()
    return json.dumps(schema_json, indent=2)

@mcp.tool()
def query_table(sql: str) -> list[dict]:
    """
       Purpose:
           Execute an SQL query against Iceberg tables through Spark
           and return the results in a JSON-like list of dictionaries.

       Args:
           sql (str): A valid Spark SQL query string.

       Output:
           list[dict]: A list where each element represents a row of the query
           result as a dictionary.
           - Keys: column names
           - Values: corresponding row values

       Use case:
           - Allows LLMs or external tools to run flexible queries on Iceberg tables.
           - Useful for analyzing scan results, filtering vulnerabilities,
             or aggregating metrics from structured data stored in Iceberg.
    """
    spark = get_spark()
    df = spark.sql(sql)
    return [row.asDict() for row in df.collect()]

@mcp.tool()
def delete_database(db_name: str):
    spark = get_spark()

    tables = spark.sql(f"SHOW TABLES IN {db_name}").toPandas()

    namespace_str = ",".join(tables["namespace"])
    tableName_str = ",".join(tables["tableName"])

    for t in tables["tableName"]:
        spark.sql(f"DROP TABLE {db_name}.{t}")

    # Step 3: 刪 namespace
    spark.sql(f"DROP DATABASE IF EXISTS {db_name}")

    return {
        "message": f"Database {db_name} deleted",
        "debug_data": tables.to_dict(orient="records"),  # 把 DataFrame 轉成 list of dict
        "namespaces": namespace_str,
        "tableNames": tableName_str
    }

@mcp.tool()
def query_vulnerability_summary():

    """
    Function: Summarize vulnerabilities in the Semgrep results.
    Output:
        - total: Total number of vulnerabilities.
        - by_severity: List of {severity, count}, grouped by severity level.
    Usage:
        For user questions like:
            - "How many vulnerabilities are there in total?"
            - "How many vulnerabilities are High / Medium / Low severity?"
    """
    spark = get_spark()

    # 統計總數
    total_sql = "SELECT COUNT(*) as total FROM scan_results"
    total_df = spark.sql(total_sql)
    total = total_df.collect()[0]["total"]

    # 根據 severity 分組
    severity_sql = """
        SELECT severity, COUNT(*) as count
        FROM scan_results
        GROUP BY severity
        ORDER BY count DESC
    """
    severity_df = spark.sql(severity_sql)
    by_severity = [row.asDict() for row in severity_df.collect()]

    return {
        "total": total,
        "severity": by_severity
    }


@mcp.tool()
def query_high_severity_files(top_n=50):
    """
    Function: Identify files with high-severity vulnerabilities.

    Output:
        - summary: Natural language summary, answering:
            * "Which files have issues?"
            * "Which file has the most vulnerabilities?"
        - files: List of top N files with high-severity counts, sorted descending.

    Usage: For questions like "Which files have problems?" or "Which file has the most vulnerabilities?"
    """
    sql = f"""
    SELECT path, COUNT(*) as high_count
    FROM scan_results
    WHERE security_severity = 'High'
    GROUP BY path
    ORDER BY high_count DESC
    LIMIT {top_n}
    """
    spark = get_spark()
    df = spark.sql(sql)
    results = [row.asDict() for row in df.collect()]

    if not results:
        summary = "There are no files with high-severity vulnerabilities."
    else:
        most_vul_file = results[0]["path"]
        summary = (
            f"There are {len(results)} files with high-severity vulnerabilities. "
            f"The file with the most issues is '{most_vul_file}'."
        )

    return {
        "summary": summary,
        "files": results
    }

@mcp.tool()
def query_cwe_summary():

    """
    Function: Summarize the number of vulnerabilities by CWE type in the Semgrep results.

    Output: Each CWE type with its corresponding count (cnt), sorted by count.

    Usage: For user questions like "What CWE types are present?" or "Vulnerability counts by type?"

    """
    sql = """
    SELECT cwe, COUNT(*) as cnt
    FROM scan_results
    GROUP BY cwe
    ORDER BY cnt DESC
    """
    spark = get_spark()
    df = spark.sql(sql)
    return [row.asDict() for row in df.collect()]


@mcp.tool()
def explain_vulnerability_by_check_id(check_id: str) -> dict:
    """
    Purpose: Explain a specific vulnerability identified by its check_id.

    Args:
        check_id: The identifier of the vulnerability.

    Output:
        {
            "check_id": "<check_id>",
            "message": "<description of the vulnerability>",
            "occurrences": [
                {
                    "path": "<file path>",
                    "start": {"line": int, "col": int, "offset": int},
                    "end": {"line": int, "col": int, "offset": int}
                },
                ...
            ]
        }

    Usage: Allows LLM or user to understand what the vulnerability is and where it appears.
    """
    spark = get_spark()
    sql = f"""
    SELECT path, message, start_line, start_col, end_line, end_col
    FROM scan_results
    WHERE check_id = '{check_id}'
    """
    df = spark.sql(sql)
    rows = df.collect()

    if not rows:
        return {
            "check_id": check_id,
            "message": "No information found for this check_id.",
            "occurrences": []
        }

    message = rows[0]["message"]  # All rows share the same message
    occurrences = [
        {
            "path": row["path"],
            "start_line": row["start_line"],
            "start_col": row["start_col"],
            "end_line": row["end_line"],
            "end_col": row["end_col"]
        }
        for row in rows
    ]

    return {
        "check_id": check_id,
        "message": message,
        "occurrences": occurrences
    }

@mcp.tool()
def query_vulnerabilities_by_cwe(cwe_id: str) -> dict:
    """
    Purpose: List all files and counts of vulnerabilities for a given CWE ID.

    Args:
        cwe_id: The CWE identifier (e.g., "CWE-190").

    Output:
        {
            "cwe_id": "<cwe_id>",
            "total_occurrences": <total number of occurrences>,
            "files": [
                {
                    "path": "<file path>",
                    "count": <number of occurrences in this file>
                },
                ...
            ]
        }

    Usage: For questions like "Which files have CWE-190 vulnerabilities?" or
           "How many occurrences of this CWE are in each file?"
    """
    spark = get_spark()
    sql = f"""
    SELECT path, COUNT(*) as count
    FROM scan_results
    WHERE cwe = '{cwe_id}'
    GROUP BY path
    ORDER BY count DESC
    """
    df = spark.sql(sql)
    rows = df.collect()

    files = [{"path": row["path"], "count": row["count"]} for row in rows]
    total_count = sum(row["count"] for row in rows)

    return {
        "cwe_id": cwe_id,
        "total_count": total_count,
        "files": files
    }

@mcp.tool()
def suggest_fix(check_id: str):
    """
    Suggest fix for a vulnerability using its message.
    Args:
        check_id: The vulnerability identifier.
    Returns:
        check_id, message, and shortDescription.
    Usage:
        LLM reads these fields and generates a fix recommendation dynamically.
    """
    sql = f"""
    SELECT check_id, shortDescription, message 
    FROM scan_results
    WHERE check_id = '{check_id}'
    """
    spark = get_spark()
    df = spark.sql(sql)
    results = [row.asDict() for row in df.collect()]

    if not results:
        return {"check_id": check_id, "message": "Not found", "shortDescription": ""}

    return results[0]

@mcp.resource("data://semgrep-json-report.json")
def semgrep_report_file():
    """
    Returns the path to the Semgrep JSON report file.
    """
    report_path = "semgrep-json-report.json"
    if not os.path.exists(report_path):
        raise FileNotFoundError(f"{report_path} not found.")
    return report_path

@mcp.prompt(
    name="semgrep_tool",
    description="tool call generator",
)
def ask_semgrep_prompt():

    system_prompt = ("You are a cybersecurity assistant specializing in code analysis using Semgrep."
                     "You have access to structured scan data via MCP tools."
                     "Your goal is to help the user understand vulnerabilities, prioritize them, and suggest fixes.")
    return system_prompt


if __name__ == "__main__":
    # Initialize and run the server
     mcp.run(transport='stdio')
    #mcp.settings.host = "192.168.42.200"
    #mcp.settings.port = 38002
    #mcp.run(transport="streamable-http")
