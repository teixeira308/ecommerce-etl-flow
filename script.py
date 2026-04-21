import os
import io
import json
import uuid
import time
import logging
import psutil
import boto3
from datetime import datetime
from google.cloud import bigquery
from botocore.exceptions import BotoCoreError, ClientError


# ==========================================================
# CONFIG
# ==========================================================

class Config:
    PROJECT_ID = os.getenv("PROJECT_ID")
    DATASET_ID = "analytics"
    TABLE_ID = "order_events"

    EXECUTION_DATE = os.getenv("EXECUTION_DATE")

    IS_RECOVERY = os.getenv("IS_RECOVERY", "false").lower() == "true"
    RECOVERY_START_DATE = os.getenv("RECOVERY_START_DATE")
    RECOVERY_END_DATE = os.getenv("RECOVERY_END_DATE")

    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_PREFIX = os.getenv("S3_PREFIX", "exports/order_events")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000000"))
    BQ_PAGE_SIZE = int(os.getenv("BQ_PAGE_SIZE", "100000"))

    S3_MIN_PART_SIZE = 5 * 1024 * 1024
    MAX_RETRIES = 5
    RETRY_BASE_DELAY = 2

    MAX_RECOVERY_RANGE_DAYS = 30


# ==========================================================
# LOGGER
# ==========================================================

class JsonLogger:
    def __init__(self, execution_id):
        self.execution_id = execution_id
        logging.basicConfig(level=logging.INFO)

    def log(self, level, event, **kwargs):
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "execution_id": self.execution_id,
            "event": event,
            **kwargs
        }
        logging.log(getattr(logging, level), json.dumps(payload))

    def info(self, event, **kwargs):
        self.log("INFO", event, **kwargs)

    def error(self, event, **kwargs):
        self.log("ERROR", event, **kwargs)


# ==========================================================
# METRICS
# ==========================================================

class Metrics:
    def __init__(self):
        self.start_time = time.time()
        self.total_rows = 0
        self.total_chunks = 0
        self.total_parts = 0

    def snapshot(self):
        elapsed = time.time() - self.start_time
        return {
            "total_rows": self.total_rows,
            "total_chunks": self.total_chunks,
            "total_parts": self.total_parts,
            "elapsed_seconds": elapsed,
            "rows_per_second": self.total_rows / elapsed if elapsed else 0,
            "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024
        }


# ==========================================================
# EXPORT JOB
# ==========================================================

class ExportJob:

    def __init__(self):
        self.execution_id = str(uuid.uuid4())
        self.logger = JsonLogger(self.execution_id)
        self.metrics = Metrics()
        self.config = Config()

        self._validate_recovery()

        self.bq_client = bigquery.Client(project=self.config.PROJECT_ID)

    # ------------------------------------------------------

    def _validate_recovery(self):

        if self.config.IS_RECOVERY:
            if not self.config.RECOVERY_START_DATE or not self.config.RECOVERY_END_DATE:
                raise ValueError("Recovery requires start and end date")

            start = datetime.strptime(self.config.RECOVERY_START_DATE, "%Y-%m-%d")
            end = datetime.strptime(self.config.RECOVERY_END_DATE, "%Y-%m-%d")

            if (end - start).days > self.config.MAX_RECOVERY_RANGE_DAYS:
                raise ValueError("Recovery range too large")

            self.logger.info(
                "recovery_mode_enabled",
                start=self.config.RECOVERY_START_DATE,
                end=self.config.RECOVERY_END_DATE
            )
        else:
            if not self.config.EXECUTION_DATE:
                raise ValueError("EXECUTION_DATE required when not in recovery")

    # ------------------------------------------------------

    def run(self):
        self.logger.info("job_started")

        iterator = self._stream_bigquery()

        chunk_number = 1
        rows_in_chunk = 0
        streamer = self._new_streamer(chunk_number)

        for row in iterator:
            serialized = self._serialize(row)
            streamer.write(serialized)

            self.metrics.total_rows += 1
            rows_in_chunk += 1

            if rows_in_chunk >= self.config.CHUNK_SIZE:
                streamer.finalize()
                self.metrics.total_chunks += 1

                chunk_number += 1
                rows_in_chunk = 0
                streamer = self._new_streamer(chunk_number)

        streamer.finalize()
        self.metrics.total_chunks += 1

        self.logger.info("job_finished", **self.metrics.snapshot())

    # ------------------------------------------------------

    def _build_query(self):

        base_query = """
        SELECT
            event_id,
            order_id,
            user_id,
            seller_id,
            event_type,
            event_timestamp,
            event_date,
            total_amount,
            currency,
            payment_method,
            installment_count,
            risk_score,
            device_type,
            country,
            metadata
        FROM `analytics.order_events`
        """

        if self.config.IS_RECOVERY:
            base_query += f"""
            WHERE event_date BETWEEN '{self.config.RECOVERY_START_DATE}'
            AND '{self.config.RECOVERY_END_DATE}'
            """
        else:
            base_query += f"""
            WHERE event_date = '{self.config.EXECUTION_DATE}'
            """

        return base_query

    # ------------------------------------------------------

    def _stream_bigquery(self):
        query = self._build_query()
        self.logger.info("query_built", query=query)

        job = self.bq_client.query(query)
        return job.result(page_size=self.config.BQ_PAGE_SIZE)

    # ------------------------------------------------------

    def _serialize(self, row):
        return (
            ",".join("" if v is None else str(v) for v in row.values()) + "\n"
        ).encode("utf-8")

    # ------------------------------------------------------

    def _new_streamer(self, chunk_number):

        if self.config.IS_RECOVERY:
            date_partition = f"{self.config.RECOVERY_START_DATE}_to_{self.config.RECOVERY_END_DATE}"
        else:
            date_partition = self.config.EXECUTION_DATE

        key = (
            f"{self.config.S3_PREFIX}/"
            f"dt={date_partition}/"
            f"part_{chunk_number:05d}.csv"
        )

        return S3MultipartStreamer(
            config=self.config,
            key=key,
            logger=self.logger,
            metrics=self.metrics
        )


# ==========================================================
# S3 STREAMER
# ==========================================================

class S3MultipartStreamer:

    def __init__(self, config, key, logger, metrics):
        self.config = config
        self.logger = logger
        self.metrics = metrics
        self.key = key

        self.s3 = boto3.client("s3", region_name=config.AWS_REGION)
        self.multipart = self.s3.create_multipart_upload(
            Bucket=config.S3_BUCKET,
            Key=key
        )
        self.upload_id = self.multipart["UploadId"]

        self.parts = []
        self.part_number = 1
        self.buffer = io.BytesIO()

        self.logger.info("multipart_started", key=key)

    def write(self, data):
        self.buffer.write(data)
        if self.buffer.tell() >= self.config.S3_MIN_PART_SIZE:
            self._upload_part()

    def _upload_part(self):
        self.buffer.seek(0)

        for attempt in range(self.config.MAX_RETRIES):
            try:
                response = self.s3.upload_part(
                    Bucket=self.config.S3_BUCKET,
                    Key=self.key,
                    PartNumber=self.part_number,
                    UploadId=self.upload_id,
                    Body=self.buffer
                )

                self.parts.append({
                    "PartNumber": self.part_number,
                    "ETag": response["ETag"]
                })

                self.metrics.total_parts += 1
                self.part_number += 1
                self.buffer = io.BytesIO()
                return

            except (BotoCoreError, ClientError):
                time.sleep(self.config.RETRY_BASE_DELAY ** attempt)

        raise RuntimeError("Failed uploading part")

    def finalize(self):
        if self.buffer.tell() > 0:
            self._upload_part()

        self.s3.complete_multipart_upload(
            Bucket=self.config.S3_BUCKET,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": self.parts}
        )
