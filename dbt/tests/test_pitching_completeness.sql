WITH raw_pitching_count AS (
    SELECT COUNT(*) AS cnt FROM {{ ref('raw_pitching_event') }}
),
mart_pitching_count AS (
    SELECT COUNT(*) AS cnt FROM {{ ref('mart_pitching') }}
)
SELECT 1
FROM raw_pitching_count, mart_pitching_count
WHERE (mart_pitching_count.cnt / raw_pitching_count.cnt) < 0.8