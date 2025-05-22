from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
import boto3
import json
import logging
import os
from decimal import Decimal

# 環境変数読み込み
TABLE_NAMES = os.environ.get("TABLE_NAMES", "").split(",")
GET_ALL_DATA_TABLE = os.environ.get("GET_ALL_DATA_TABLE")
S3_BUCKET = os.environ.get("S3_BUCKET")
DATABRICKS_JOB_ID_LOAD_TO_RAW = int(os.environ.get("DATABRICKS_JOB_ID_LOAD_TO_RAW"))
DATABRICKS_CONN_ID = os.environ.get("DATABRICKS_CONN_ID")


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


@dag(tags=['dynamodb', 's3', 'deltalake'])
def load_to_raw_pitching_data_flow():

    @task
    def fetch_and_upload(table_name: str):
        now = datetime.utcnow()
        one_hour_ago = int((now - timedelta(hours=1)).timestamp() * 1000)

        try:
            # DynamoDB接続
            dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
            table = dynamodb.Table(table_name)
            scan_kwargs = {}

            items = []
            done = False
            start_key = None

            while not done:
                if start_key:
                    scan_kwargs['ExclusiveStartKey'] = start_key
                response = table.scan(**scan_kwargs)
                items.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None

            if not items:
                logging.warning(f"[{table_name}] データが0件でした。スキップ。")
                return

            # S3へ保存
            s3_key_prefix = f"dynamodb_exports/{table_name}/{now.strftime('%Y%m%d_%H')}"
            s3_key = f"{s3_key_prefix}/export_until_{now.strftime('%Y%m%d_%H')}.json"

            s3 = boto3.client('s3')
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(items, indent=2, default=decimal_default,ensure_ascii=False).encode('utf-8'),
                ContentType='application/json; charset=utf-8'
            )

            logging.info(f"[{table_name}] S3にアップロード成功: s3://{S3_BUCKET}/{s3_key}")

        except (BotoCoreError, ClientError) as aws_err:
            logging.error(f"[{table_name}] AWSエラー: {aws_err}")
            raise

        except Exception as e:
            logging.exception(f"[{table_name}] 予期しないエラー")
            raise

    # すべてのテーブルに対してタスクを作成して実行
    for table_name in TABLE_NAMES:
        s3_path = f"s3://{S3_BUCKET}/dynamodb_exports/{table_name}/{{{{ execution_date.strftime('%Y%m%d_%H') }}}}/export_until_{{{{ execution_date.strftime('%Y%m%d_%H') }}}}.json"        

        fetch_task = fetch_and_upload.override(task_id=f'fetch_{table_name}')(table_name)

        db_task = DatabricksRunNowOperator(
            task_id=f"load_{table_name}_to_delta",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=DATABRICKS_JOB_ID_LOAD_TO_RAW,
            python_params=[table_name, s3_path]
        )
 
        fetch_task >> db_task
       


dag = load_to_raw_pitching_data_flow()
