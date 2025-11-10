WITH combined AS (
    {{ dbt_utils.union_relations(
        relations = [
            ref('STG_RAW_PLAYER'),
            ref('STG_BRONZE_PLAYERSTATS')
        ],
        source_column_name = 'source_layer'
    ) }}
),
matched AS (
    SELECT 
        PLAYERID,
        COUNT(DISTINCT source_layer) AS layer_count
    FROM combined
    GROUP BY PLAYERID
)
SELECT 
    PLAYERID
FROM matched
WHERE layer_count < 2