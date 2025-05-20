{{ config(
    materialized='incremental'
) }}

WITH base AS (
  SELECT
    -- 試合日キー（数値：20240708）
    CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) AS game_day_key,
    date,
    YEAR(date) AS year,
    MONTH(date) AS month,
    'いいえ' AS is_missing,
    current_timestamp() AS created_at,
    'dbt/model' AS created_by,
    current_timestamp() AS updated_at,
    'dbt/model' AS updated_by
  FROM {{ ref('pitching_event') }}
  WHERE GROUP BY date
)

SELECT * FROM base 

{% if is_incremental() %}
WHERE game_day_key NOT IN (
  SELECT game_day_key FROM {{ this }}
)
{% endif %}