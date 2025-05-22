from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import boto3
import json
import logging
import os
from decimal import Decimal

# ==== 環境変数の読み込み ====
TABLE_NAMES = os.environ.get("TABLE_NAMES", "").split(",")
GET_ALL_DATA_TABLE = os.environ.get("GET_ALL_DATA_TABLE")
S3_BUCKET = os.environ.get("S3_BUCKET")
DATABRICKS_JOB_ID_LOAD_TO_RAW = int(os.environ.get("DATABRICKS_JOB_ID_LOAD_TO_RAW"))
DATABRICKS_JOB_ID_CREATE_PITCHING_DATAMART_FROM_RAW = int(os.environ.get("DATABRICKS_JOB_ID_CREATE_PITCHING_DATAMART_FROM_RAW"))
DATABRICKS_CONN_ID = os.environ.get("DATABRICKS_CONN_ID")

# ==== Decimal型をfloatに変換してJSON化できるようにする ====
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


# ==== データをDynamoDBから取得し、S3に保存するタスク ====
@task
def fetch_and_upload(table_name: str, get_all_data_table: str, s3_bucket: str) -> bool:
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)
    one_hour_ago_iso = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
        table = dynamodb.Table(table_name)

        # 更新時刻でのフィルタ（特定テーブルは除く）
        scan_kwargs = {
            'FilterExpression': 'updatedAt >= :time',
            'ExpressionAttributeValues': {':time': one_hour_ago_iso}
        }
        if table_name == get_all_data_table:
            scan_kwargs = {}

        items = []
        start_key = None

        # ページネーション処理
        while True:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey')
            if not start_key:
                break

        # データがなければFalseを返す
        if not items:
            logging.warning(f"[{table_name}] データが0件でした。スキップ。")
            return False

        # S3キー生成
        s3_key_prefix = f"dynamodb_exports/{table_name}/{now.strftime('%Y%m%d_%H')}"
        s3_key = f"{s3_key_prefix}/export_until_{now.strftime('%Y%m%d_%H')}.json"

        # S3に保存
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json.dumps(items, indent=2, default=decimal_default, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json; charset=utf-8'
        )

        logging.info(f"[{table_name}] S3にアップロード成功: s3://{s3_bucket}/{s3_key}")
        return True

    except Exception as e:
        logging.exception(f"[{table_name}] エラー: {e}")
        raise


# ==== fetch結果に応じて次に進むかスキップするかを分岐 ====
@task.branch
def decide_next_step(table_name: str, ti=None):
    result = ti.xcom_pull(task_ids=f'fetch_{table_name}')
    return f"load_{table_name}_to_delta" if result else f"skip_{table_name}"


# ==== DAG定義 ====
@dag(start_date=datetime(2024, 1, 1), schedule_interval='@hourly', catchup=False, tags=['dynamodb', 's3', 'deltalake'])
def upsert_pitching_datamart_flow():
    load_tasks = []

    # 各テーブルごとにタスク構築
    for table_name in TABLE_NAMES:
        # fetchタスク（タスクIDを個別指定）
        fetch_task = fetch_and_upload.override(task_id=f'fetch_{table_name}')(
            table_name, GET_ALL_DATA_TABLE, S3_BUCKET
        )

        # 分岐タスク（成功ならロード、失敗ならスキップ）
        branch_task = decide_next_step.override(task_id=f'branch_{table_name}')(table_name)

        # S3からロードするパスをDatabricksジョブへ渡す
        s3_path = f"s3://{S3_BUCKET}/dynamodb_exports/{table_name}/{{{{ execution_date.strftime('%Y%m%d_%H') }}}}/export_until_{{{{ execution_date.strftime('%Y%m%d_%H') }}}}.json"

        # Databricksへロードジョブ（rawレイヤー）
        load_from_s3_to_raw_layer = DatabricksRunNowOperator(
            task_id=f"load_{table_name}_to_delta",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=DATABRICKS_JOB_ID_LOAD_TO_RAW,
            python_params=[table_name, s3_path],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        # データがない場合にスキップされる空タスク
        skip_task = EmptyOperator(task_id=f'skip_{table_name}')

        # 依存関係の構築
        fetch_task >> branch_task >> [load_from_s3_to_raw_layer, skip_task]
        load_tasks.append(load_from_s3_to_raw_layer)

    # 全テーブルロード成功後に起動する中継タスク
    all_loads_complete = EmptyOperator(
        task_id="all_loads_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # 各ロードタスクが終わってから中継タスクへ
    for task in load_tasks:
        task >> all_loads_complete

    # datamartレイヤーの作成まで実行するDatabricksジョブ（最終処理）
    create_pitching_datamart_from_raw = DatabricksRunNowOperator(
        task_id="create_pitching_datamart_from_raw",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID_CREATE_PITCHING_DATAMART_FROM_RAW,
        python_params=[],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # 全ロード完了後に staging ジョブを起動
    all_loads_complete >> create_pitching_datamart_from_raw


# DAGインスタンスの生成
dag = upsert_pitching_datamart_flow()
