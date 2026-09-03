

SELECT 
    game_id::INT AS game_id, 
    game_name::VARCHAR(100) AS game_name, 
    provider_id::INT AS provider_id
FROM "datastack"."raw"."games_map"