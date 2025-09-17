import logging
import os
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MinIOSparkJob")


# adding iceberg configs
conf = (
    SparkConf()
    .set("spark.sql.extensions",
         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") # Use Iceberg with Spark
    .set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .set("spark.sql.catalog.demo.warehouse", "s3a://iceberg/warehouse/")
    .set("spark.sql.catalog.demo.s3.endpoint", "http://10.233.55.6:9000")
    .set("spark.sql.defaultCatalog", "demo") # Name of the Iceberg catalog
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


def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "openpitrixminioaccesskey"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key",
                                                 os.getenv("AWS_SECRET_ACCESS_KEY", "openpitrixminiosecretkey"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://10.233.55.6:9000"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

load_config(spark.sparkContext)

df = spark.read.json("semgrep-json-report.json", multiLine=True)

spark.sql("CREATE DATABASE IF NOT EXISTS demo.semgrep")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.semgrep.scan_results (
    check_id STRING,
    path STRING,
    start_line BIGINT,
    start_col BIGINT,
    start_offset BIGINT,
    end_line BIGINT,
    end_col BIGINT,
    end_offset BIGINT,
    message STRING,
    severity STRING,
    fingerprint STRING,
    category STRING,
    cwe STRING,
    owasp ARRAY<STRING>,
    security_severity STRING,
    shortDescription STRING,
    engine_kind STRING,
    validation_state STRING,
    lines STRING
) USING iceberg;
""")

results_df = df.withColumn("r", explode("results")).select(
    col("r.check_id").alias("check_id"),
    col("r.path").alias("path"),
    col("r.start.line").alias("start_line"),
    col("r.start.col").alias("start_col"),
    col("r.start.offset").alias("start_offset"),
    col("r.end.line").alias("end_line"),
    col("r.end.col").alias("end_col"),
    col("r.end.offset").alias("end_offset"),
    col("r.extra.message").alias("message"),
    col("r.extra.severity").alias("severity"),
    col("r.extra.fingerprint").alias("fingerprint"),
    col("r.extra.metadata.category").alias("category"),
    col("r.extra.metadata.cwe").alias("cwe"),
    col("r.extra.metadata.owasp").alias("owasp"),
    col("r.extra.metadata.`security-severity`").alias("security_severity"),
    col("r.extra.metadata.shortDescription").alias("shortDescription"),
    col("r.extra.engine_kind").alias("engine_kind"),
    col("r.extra.validation_state").alias("validation_state"),
    col("r.extra.lines").alias("lines")
)
results_df.writeTo("demo.semgrep.scan_results").append()


spark.sql("""
CREATE TABLE IF NOT EXISTS demo.semgrep.scan_error_spans (
    code BIGINT,
    file STRING,
    start_line BIGINT,
    start_col BIGINT,
    start_offset BIGINT,
    end_line BIGINT,
    end_col BIGINT,
    end_offset BIGINT
) USING iceberg;

""")

spans_df = df.withColumn("e", explode("errors")) \
             .withColumn("s", explode("e.spans")) \
             .select(
                 col("e.code").alias("code"),
                 col("s.file").alias("file"),
                 col("s.start.line").alias("start_line"),
                 col("s.start.col").alias("start_col"),
                 col("s.start.offset").alias("start_offset"),
                 col("s.end.line").alias("end_line"),
                 col("s.end.col").alias("end_col"),
                 col("s.end.offset").alias("end_offset")
             )
spans_df.writeTo("demo.semgrep.scan_error_spans").append()


spark.sql("""
CREATE TABLE IF NOT EXISTS demo.semgrep.scan_errors (
    code BIGINT,
    level STRING,
    message STRING,
    path STRING
) USING iceberg;
""")


errors_df = df.withColumn("e", explode("errors")).select(
    col("e.code").alias("code"),
    col("e.level").alias("level"),
    col("e.message").alias("message"),
    col("e.path").alias("path")
)
errors_df.writeTo("demo.semgrep.scan_errors").append()

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.semgrep.scan_summary (
    version STRING,
    scanned ARRAY<STRING>,
    skipped_rules ARRAY<STRING>
) USING iceberg;
""")

summary_df = df.select(
    col("version"),
    col("paths.scanned").alias("scanned"),
    col("skipped_rules")
)
summary_df.writeTo("demo.semgrep.scan_summary").append()



spark.sql("SELECT severity, COUNT(*) FROM demo.semgrep.scan_results GROUP BY severity").show()
