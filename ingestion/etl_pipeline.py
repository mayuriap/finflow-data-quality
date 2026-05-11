import argparse
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, BooleanType
)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),  True),
    StructField("customer_id",      StringType(),  True),
    StructField("transaction_type", StringType(),  True),
    StructField("amount",           DoubleType(),  True),
    StructField("currency",         StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("counterparty",     StringType(),  True),
    StructField("reference_number", StringType(),  True),
    StructField("transaction_date", StringType(),  True),
    StructField("settlement_date",  StringType(),  True),
    StructField("description",      StringType(),  True),
    StructField("fee_amount",       DoubleType(),  True),
    StructField("fee_currency",     StringType(),  True),
    StructField("source_system",    StringType(),  True),
    StructField("batch_id",         StringType(),  True),
    StructField("created_at",       StringType(),  True),
    StructField("updated_at",       StringType(),  True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",  StringType(),  True),
    StructField("first_name",   StringType(),  True),
    StructField("last_name",    StringType(),  True),
    StructField("email",        StringType(),  True),
    StructField("country_code", StringType(),  True),
    StructField("account_type", StringType(),  True),
    StructField("risk_rating",  StringType(),  True),
    StructField("created_at",   StringType(),  True),
    StructField("is_active",    StringType(),  True),
])

VALID_CURRENCIES = ["USD","GBP","EUR","JPY","CHF","AUD","CAD","HKD"]
VALID_STATUSES   = ["COMPLETED","PENDING","FAILED","CANCELLED","PROCESSING"]
VALID_TX_TYPES   = ["PAYMENT","TRANSFER","WITHDRAWAL","DEPOSIT","FX_TRADE","SETTLEMENT"]


def create_spark():
    print("Starting Spark session...")
    spark = (
        SparkSession.builder
        .appName("FinFlow-Bronze-ETL")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") 
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark {spark.version} ready\n")
    return spark


def add_audit_columns(df: DataFrame, source_file: str) -> DataFrame:
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file",         F.lit(source_file))
        .withColumn("_pipeline_layer",      F.lit("bronze"))
        .withColumn("_pipeline_version",    F.lit("1.0.0"))
        .withColumn("_run_id",              F.lit(datetime.utcnow().strftime("%Y%m%d_%H%M%S")))
    )


def ingest_transactions(spark: SparkSession, source_path: str) -> DataFrame:
    print(f"Reading transactions from: {source_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .schema(TRANSACTION_SCHEMA)
        .csv(source_path)
    )
    print(f"  Raw rows loaded: {df.count():,}")

    # Parse timestamps
    
    
    df = (
        df
        .withColumn("transaction_date_ts",
                    F.to_timestamp("transaction_date"))
        .withColumn("settlement_date_ts",
                    F.to_timestamp("settlement_date"))
        .withColumn("created_at_ts",
                    F.to_timestamp("created_at"))
        .withColumn("partition_year",  F.year("transaction_date_ts"))
        .withColumn("partition_month", F.month("transaction_date_ts"))
        .withColumn("partition_day",   F.dayofmonth("transaction_date_ts"))
    )

    # DQ flag columns — Bronze keeps ALL data, just flags issues
    df = (
        df
        .withColumn("dq_null_transaction_id", F.col("transaction_id").isNull())
        .withColumn("dq_null_customer_id",    F.col("customer_id").isNull())
        .withColumn("dq_null_amount",         F.col("amount").isNull())
        .withColumn("dq_negative_amount",     F.col("amount") < 0)
        .withColumn("dq_invalid_currency",    ~F.col("currency").isin(VALID_CURRENCIES))
        .withColumn("dq_invalid_status",      ~F.col("status").isin(VALID_STATUSES))
        .withColumn("dq_has_issues",
            F.col("dq_null_transaction_id") |
            F.col("dq_null_customer_id")    |
            F.col("dq_null_amount")         |
            F.col("dq_negative_amount")     |
            F.col("dq_invalid_currency")    |
            F.col("dq_invalid_status")
        )
    )

    df = add_audit_columns(df, source_path)
    return df


def ingest_customers(spark: SparkSession, source_path: str) -> DataFrame:
    print(f"Reading customers from: {source_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .schema(CUSTOMER_SCHEMA)
        .csv(source_path)
    )
    print(f"  Raw rows loaded: {df.count():,}")
    df = (
        df
        .withColumn("dq_null_customer_id", F.col("customer_id").isNull())
        .withColumn("dq_null_email",        F.col("email").isNull())
        .withColumn("dq_has_issues",
            F.col("dq_null_customer_id") | F.col("dq_null_email"))
    )
    df = add_audit_columns(df, source_path)
    return df


def print_dq_summary(df: DataFrame, entity: str):
    total  = df.count()
    issues = df.filter(F.col("dq_has_issues")).count()
    clean  = total - issues
    print(f"\n── DQ Summary: {entity} ──")
    print(f"  Total rows    : {total:>8,}")
    print(f"  Clean rows    : {clean:>8,}  ({clean/total*100:.1f}%)")
    print(f"  Rows w/ issues: {issues:>8,}  ({issues/total*100:.1f}%)")


def write_bronze(df: DataFrame, output_path: str, partition_cols=None):
    print(f"\nWriting Bronze → {output_path}")
    
    # Convert to pandas and write CSV — avoids winutils/Hadoop native lib issue on Windows
    # In production (Linux/cloud) this would be Parquet — same logic, different format
    pandas_df = df.toPandas()
    
    output = Path(output_path)
    output.mkdir(parents=True, exist_ok=True)
    
    if partition_cols:
        # Write one CSV per partition value
        for partition_val, group in pandas_df.groupby(partition_cols):
            if isinstance(partition_val, tuple):
                partition_str = "_".join(str(v) for v in partition_val)
            else:
                partition_str = str(partition_val)
            file_path = output / f"part_{partition_str}.csv"
            group.to_csv(file_path, index=False)
        print(f"✓ Written {len(pandas_df):,} rows across partitions → {output}")
    else:
        file_path = output / "part_0.csv"
        pandas_df.to_csv(file_path, index=False)
        print(f"✓ Written {len(pandas_df):,} rows → {file_path}")


def run_pipeline(source_dir: str, bronze_dir: str):
    start = time.time()
    print("=" * 55)
    print("  FinFlow ETL Pipeline — Bronze Ingestion")
    print("=" * 55)

    source = Path(source_dir)
    bronze = Path(bronze_dir)
    bronze.mkdir(parents=True, exist_ok=True)

    spark = create_spark()

    # Transactions
    tx_path = str(source / "transactions.csv")
    if Path(tx_path).exists():
        tx_df = ingest_transactions(spark, tx_path)
        print_dq_summary(tx_df, "Transactions")
        write_bronze(
            tx_df,
            str(bronze / "transactions"),
            partition_cols=["partition_year", "partition_month"]
        )

    # Customers
    cust_path = str(source / "customers.csv")
    if Path(cust_path).exists():
        cust_df = ingest_customers(spark, cust_path)
        print_dq_summary(cust_df, "Customers")
        write_bronze(cust_df, str(bronze / "customers"))

    elapsed = time.time() - start
    print(f"\n{'='*55}")
    print(f"  ✓ Bronze ingestion complete in {elapsed:.1f}s")
    print(f"  Output: {bronze.resolve()}")
    print(f"{'='*55}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", default="./data/raw")
    parser.add_argument("--bronze-dir", default="./data/bronze")
    args = parser.parse_args()
    run_pipeline(args.source_dir, args.bronze_dir)