from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import boto3
import json
import logging
import os
from decimal import Decimal

# 環境変数読み込み
TABLE_NAMES = os.environ.get("TABLE_NAMES", "").split(",")
GET_ALL_DATA_TABLE = os.environ.get("GET_ALL_DATA_TABLE")
S3_BUCKET = os.environ.get("S3_BUCKET")
DATABRICKS_JOB_ID = int(os.environ.get("DATABRICKS_JOB_ID"))
DATABRICKS_CONN_ID = os.environ.get("DATABRICKS_CONN_ID")


# Decimal型が含まれていてもJSON変換できるようにする関数
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


# DAG定義
@dag(start_date=datetime(2024, 1, 1), schedule_interval='@hourly', catchup=False, tags=['dynamodb', 's3', 'deltalake'])
def export_recent_data_of_source_dynamodb_to_load_to_raw():

    # 各テーブルごとのタスクをまとめて生成する関数
    def create_tasks_for_table(table_name):

        # DynamoDBからデータを取得しS3にアップロードするタスク
        @task(task_id=f'fetch_{table_name}')
        def fetch_and_upload():
            now = datetime.utcnow()
            one_hour_ago = now - timedelta(hours=1)
            # ISO形式の時刻文字列（ミリ秒付き）を作成
            one_hour_ago_iso = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

            try:
                # DynamoDB接続
                dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
                table = dynamodb.Table(table_name)

                # 更新時刻が1時間以内のレコードを対象とする条件
                scan_kwargs = {
                    'FilterExpression': 'updatedAt >= :time',
                    'ExpressionAttributeValues': {
                        ':time': one_hour_ago_iso
                    }
                }

                # 特定テーブルは全件取得（updatedAtなし)
                if table_name == GET_ALL_DATA_TABLE:
                    scan_kwargs = {}

                items = []
                start_key = None

                # ページネーション対応のスキャン
                while True:
                    if start_key:
                        scan_kwargs['ExclusiveStartKey'] = start_key
                    response = table.scan(**scan_kwargs)
                    items.extend(response.get('Items', []))
                    start_key = response.get('LastEvaluatedKey')
                    if not start_key:
                        break

                if not items:
                    logging.warning(f"[{table_name}] データが0件でした。スキップ。")
                    return False

                # S3キー生成
                s3_key_prefix = f"dynamodb_exports/{table_name}/{now.strftime('%Y%m%d_%H')}"
                s3_key = f"{s3_key_prefix}/export_until_{now.strftime('%Y%m%d_%H')}.json"

                # S3へアップロード
                s3 = boto3.client('s3')
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=json.dumps(items, indent=2, default=decimal_default, ensure_ascii=False).encode('utf-8'),
                    ContentType='application/json; charset=utf-8'
                )

                logging.info(f"[{table_name}] S3にアップロード成功: s3://{S3_BUCKET}/{s3_key}")
                return True

            except Exception as e:
                logging.exception(f"[{table_name}] エラー: {e}")
                raise

        # fetchの結果（True/False）で処理を分岐するタスク
        @task.branch(task_id=f'branch_{table_name}')
        def decide_next_step(**context):
            ti = context['ti']
            result = ti.xcom_pull(task_ids=f'fetch_{table_name}')
            return f"load_{table_name}_to_delta" if result else f"skip_{table_name}"

        # タスク実行
        fetch_task = fetch_and_upload()
        branch_task = decide_next_step()

        # S3にアップされたパス
        s3_path = f"s3://{S3_BUCKET}/dynamodb_exports/{table_name}/{{{{ execution_date.strftime('%Y%m%d_%H') }}}}/export_until_{{{{ execution_date.strftime('%Y%m%d_%H') }}}}.json"

        # Databricksのジョブ実行タスク
        db_task = DatabricksRunNowOperator(
            task_id=f"load_{table_name}_to_delta",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=DATABRICKS_JOB_ID,
            python_params=[table_name, s3_path],
            trigger_rule=TriggerRule.ALL_SUCCESS  # fetchが成功したときだけ動かす
        )

        # スキップ用の空タスク
        skip_task = EmptyOperator(task_id=f'skip_{table_name}')

        # タスクの接続関係を構築
        fetch_task >> branch_task >> [db_task, skip_task]

    # すべてのテーブルに対して処理を生成
    for t_name in TABLE_NAMES:
        create_tasks_for_table(t_name)


# DAGインスタンス
dag = export_recent_data_of_source_dynamodb_to_load_to_raw()
