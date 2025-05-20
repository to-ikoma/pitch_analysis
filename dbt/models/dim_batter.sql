{{
    config(
        materialized='incremental'
    )
}}

WITH base AS (
  SELECT
    owner,
    batter,
    bat,
    top_or_bottom,
    second_attack_team,
    first_attack_team,
    CASE
      WHEN top_or_bottom = 'top' THEN first_attack_team
      WHEN top_or_bottom = 'bottom' THEN second_attack_team
      ELSE ''
    END AS team_for_key
  FROM {{ ref('pitching_event') }}
),
grouped AS (
    SELECT
    -- サロゲートキー
    md5(concat_ws('||', owner, batter, bat, team_for_key)) AS batter_key,

    owner,
    batter AS batter_name,
    bat,
    team_for_key AS team,
    'NO' AS is_missing,
    current_timestamp() AS created_at,
    'dbt/model' AS created_by,
    current_timestamp() AS updated_at,
    'dbt/model' AS updated_by
    FROM base
    GROUP BY owner, batter, bat, team_for_key
)
SELECT * FROM grouped
{% if is_incremental() %}

	where "batter_key" not in (select "batter_key" from {{ this }})
    
{% endif %}