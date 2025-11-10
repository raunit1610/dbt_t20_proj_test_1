{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT PK_PLAYER PRIMARY KEY (PLAYERID)"
    )
}}

WITH source_data AS (
    SELECT
        {{ dbt_utils.star(from=source('t20_database', 'players')) }},
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS _inserted_at_
    FROM {{ source('t20_database', 'players') }}
)

SELECT *
FROM source_data
