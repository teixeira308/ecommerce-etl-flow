import sys
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, floor

# ==========================================
# CONFIGURATION
# ==========================================

BQ_TABLE = "fintech_prod.analytics.transactions_v2"
ROWS_PER_FILE = 1_000_000


# ==========================================
# LOGGING SETUP
# ==========================================

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("spark-bq-export")


# ==========================================
# ARGUMENT PARSER
# ==========================================

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--recovery", type=bool, default=False)
    parser.add_argument("--recovery_date", type=str, default=None)
    parser.add_argument("--start_date", type=str, required=False)
    parser.add_argument("--end_date", type=str, required=False)
    parser.add_argument("--s3_bucket", type=str, required=True)
    parser.add_argument("--s3_prefix", type=str, required=True)

    return parser.parse_args()


# ==========================================
# QUERY BUILDER
# ==========================================

def build_query(args, logger):

    if args.recovery:
        if not args.recovery_date:
            raise ValueError("recovery_date must be provided if recovery=True")

        logger.info(f"Running in RECOVERY mode from {args.recovery_date}")

        query = f"""
        SELECT *
        FROM `{BQ_TABLE}`
        WHERE updated_at >= TIMESTAMP('{args.recovery_date}')
        """

    else:
        if not args.start_date or not args.end_date:
            raise ValueError("start_date and end_date are required in normal mode")

        logger.info(
            f"Running in NORMAL mode from {args.start_date} to {args.end_date}"
        )

        query = f"""
        SELECT *
        FROM `{BQ_TABLE}`
        WHERE settlement_date BETWEEN DATE('{args.start_date}')
                                  AND DATE('{args.end_date}')
        """

    return query


# ==========================================
# SPARK SESSION
# ==========================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("BigQuery-to-S3-Streaming-Export")
        .config("viewsEnabled", "true")
        .config("materializationDataset", "temp_dataset")
        .getOrCreate()
    )

    return spark


# ==========================================
# ADD FILE PARTITION STRATEGY
# ==========================================

def repartition_by_row_count(df, logger):

    logger.info("Applying row-based partition strategy")

    window_spec = Window.orderBy("transaction_timestamp")

    df_with_rownum = df.withColumn(
        "row_num", row_number().over(window_spec)
    )

    df_partitioned = df_with_rownum.withColumn(
        "file_partition",
        floor(col("row_num") / ROWS_PER_FILE)
    )

    return df_partitioned.drop("row_num")


# ==========================================
# MAIN EXECUTION
# ==========================================

def main():

    logger = setup_logging()
    args = parse_args()

    spark = create_spark()

    logger.info("Building query...")
    query = build_query(args, logger)

    logger.info("Reading data from BigQuery...")

    df = (
        spark.read.format("bigquery")
        .option("query", query)
        .load()
    )

    total_rows = df.count()
    logger.info(f"Total rows fetched: {total_rows}")

    logger.info("Partitioning dataset...")
    df_partitioned = repartition_by_row_count(df, logger)

    output_path = f"s3a://{args.s3_bucket}/{args.s3_prefix}"

    logger.info(f"Writing data to S3: {output_path}")

    (
        df_partitioned
        .repartition("file_partition")
        .write
        .mode("overwrite")
        .partitionBy("file_partition")
        .parquet(output_path)
    )

    logger.info("Export completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
