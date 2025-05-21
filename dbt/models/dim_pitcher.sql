{{
    config(
        materialized='incremental',
        unique_key='pitcher_key'
    )
}}

WITH base AS (
  SELECT
    owner,
    pitcher,
    throw,
    top_or_bottom,
    second_attack_team,
    first_attack_team,
    CASE
      WHEN top_or_bottom = 'top' THEN second_attack_team
      WHEN top_or_bottom = 'bottom' THEN first_attack_team
      ELSE ''
    END AS team_for_key
  FROM {{ ref('pitching_event') }}
),
grouped AS (
  SELECT
    -- サロゲートキー
    md5(concat_ws('||', owner, pitcher, throw, team_for_key)) AS pitcher_key,
    owner,
    pitcher AS pitcher_name,
    throw,
    team_for_key AS pitcher_team,
    'NO' AS is_missing,
    current_timestamp() AS created_at,
    'dbt/model' AS created_by,
    current_timestamp() AS updated_at,
    'dbt/model' AS updated_by
  FROM base
  GROUP BY owner, pitcher, throw, team_for_key
)
SELECT * FROM grouped
{% if is_incremental() %}
	where pitcher_key not in (select pitcher_key from {{ this }})
{% endif %}