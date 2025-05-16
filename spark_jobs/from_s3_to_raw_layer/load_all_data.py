from pyspark.sql import SparkSession
import sys
import re

table_name = sys.argv[1]
s3_path = sys.argv[2]

safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
table_name_with_prefix = f"raw_{safe_table_name}"

spark = SparkSession.builder.getOrCreate()

# df = spark.read.json(s3_path)
# JSON読込み：不正行も含めて確認できるよう設定
df = (
    spark.read
    .option("multiLine", "true")
    .json(s3_path)
)
# `_corrupt_record`しかないデータでの保存を防ぐ
if "_corrupt_record" in df.columns and len(df.columns) == 1:
    raise ValueError(f"{table_name} の読み込みに失敗しました。壊れたJSONの可能性があります: {s3_path}")

# Managed Tableとして保存（保存先は自動管理）
df.write.mode("overwrite").format("delta").saveAsTable(f"`{table_name_with_prefix}`")

# タグ（テーブルプロパティ）を追加
spark.sql(f"""
    ALTER TABLE {table_name_with_prefix}
    SET TBLPROPERTIES ('layer' = 'raw')
""")