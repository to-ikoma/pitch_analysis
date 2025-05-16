from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, lit, when
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

# `_corrupt_record`しかないデータでの保存を防ぐ
if "_corrupt_record" in df.columns and len(df.columns) == 1:
    raise ValueError(f"{table_name} の読み込みに失敗しました。壊れたJSONの可能性があります: {s3_path}")

# 運用を容易にするため、利用しないはネストの深いStruct型カラムは除外（もしかしたらApache Icebergなら柔軟なスキーマハンドリングが可能かも）
exclude_columns = ["firstAttackMember", "secondAttackMember", "detailsForNextBatter","pitchInfo"]
columns_to_use = [c for c in df.columns if c not in exclude_columns]

# 除外したカラムで新しいDataFrame作成
exclued_df = df.select(columns_to_use)

# emailが存在するなら、空文字でない場合だけハッシュ化（SHA-256）
if "email" in exclued_df.columns:
    exclued_df = exclued_df.withColumn(
        "email",
        when(col("email") != "", sha2(col("email"), 256)).otherwise(lit(""))  # 空文字はそのまま
    )

if "user" in table_name_with_prefix.lower():
    # ユーザ系テーブルは上書き
    exclued_df.write.mode("overwrite").format("delta").saveAsTable(f"`{table_name_with_prefix}`")
else:
    if not spark.catalog.tableExists(table_name_with_prefix):
        exclued_df.limit(0).write.mode("overwrite").format("delta").saveAsTable(f"`{table_name_with_prefix}`")

    exclued_df.createOrReplaceTempView("incoming")
    spark.sql(f"""
        MERGE INTO `{table_name_with_prefix}` t
        USING incoming s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)