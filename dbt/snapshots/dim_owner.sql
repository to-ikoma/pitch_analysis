{% snapshot dim_owner %}
{{
  config(
    target_schema='default',
    unique_key='owner_id',
    strategy='check',
    check_cols=["email","username"]
  )
}}
SELECT
  -- サロゲートキー
  md5(concat_ws('||', owner, email, username)) AS owner_key,
  owner AS owner_id,
  email,
  username,
  current_timestamp() AS created_at_snapshot,
  'dbt/snapshot' AS created_by,
  current_timestamp() AS updated_at_snapshot,
  'dbt/snapshot' AS updated_by,
  'NO' AS is_missing

FROM {{ source('raw','raw_user_jqdkca4vcbf6nh7ojsquhjo4ai_staging') }}
WHERE coalesce(email, '') <> '' OR coalesce(username, '') <> ''

{% endsnapshot %}