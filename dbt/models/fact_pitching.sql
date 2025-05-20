{{ config(
    materialized='incremental'
) }}

SELECT
  pe.id AS pitching_id,
  dp.pitcher_key,
  db.batter_key,
  do.owner_key,
  pt.dim_pitching_type_key,
  dgd.game_day_key
  di.inning_key,
  dpc.pitching_count_key,
  doc.out_count_key,
  dr.runner_key,
  dpr.pitching_result_key,
  pn.pitching_number_key,
  pe.speed,
  pe.pitchXPosition,
  pe.pitchYPosition,
  pe.title,
  current_timestamp() AS created_at,
  'dbt/model' AS created_by

FROM {{ ref('pitching_event') }} pe
LEFT JOIN {{ ref('dim_pitcher') }} dp
  ON pe.owner = dp.owner AND pe.pitcher = dp.pitcher_name AND (pe.throw = dp.throw OR (pe.throw IS NULL AND dp.throw IS NULL)) AND pe.team_for_pitcher = dp.team
LEFT JOIN {{ ref('dim_batter') }} db
  ON pe.owner = db.owner AND pe.batter = db.batter_name AND pe.bat = db.bat AND pe.team_for_batter = db.team
LEFT JOIN {{ ref('dim_owner') }} do
  ON pe.owner = do.owner_id
LEFT JOIN {{ ref('dim_pitching_types') }} dpt ON pe.breaking_ball = dpt.pitching_type_id
LEFT JOIN {{ ref('dim_game_day') }} dgd ON pe.date = dgd.date
LEFT JOIN {{ ref('dim_inning') }} di ON pe.inning = di.inning AND pe.top_or_bottom = i.top_bottom
LEFT JOIN {{ ref('dim_pitching_counts') }} dpc ON pe.strikes = dpc.strike AND pe.balls = dpc.ball
LEFT JOIN {{ ref('dim_out_counts') }} doc ON pe.out_counts = doc.out_counts
LEFT JOIN {{ ref('dim_runners') }} dr ON pe.runner = dr.runner_code
LEFT JOIN {{ ref('dim_pitching_results') }} dpr ON pe.pitch_result = dpr.pitching_result_id
LEFT JOIN {{ ref('dim_pitching_numbers') }} dpn ON pe.sequence = dpn.pitching_number

{% if is_incremental() %}
WHERE pitching_id NOT IN (
  SELECT pitching_id FROM {{ this }}
)
{% endif %}