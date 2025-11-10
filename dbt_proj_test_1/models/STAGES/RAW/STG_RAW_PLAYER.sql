{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT PK_PLAYER PRIMARY KEY (PLAYERID)"
        materialized='incremental',
        unique_key='PLAYERID',
        incremental_strategy='merge'
    )
}}

WITH source_data AS (
    SELECT
        {{ dbt_utils.star(from=source('t20_database', 'players')) }},
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS _inserted_at_
    FROM {{ source('t20_database', 'players') }}
),

deduped AS (
    SELECT
        PLAYERID,
        ANY_VALUE(PLAYERNAME) AS PLAYERNAME,
        ANY_VALUE(FILENAME) AS FILENAME,
        ANY_VALUE(LOAD_TIMESTAMP) AS LOAD_TIMESTAMP,
        MAX(_inserted_at_) AS _inserted_at_
    FROM source_data
    GROUP BY PLAYERID
)

SELECT *
FROM deduped

{% if is_incremental() %}
  WHERE _inserted_at_ > (SELECT MAX(_inserted_at_) FROM {{ this }})
{% endif %}
