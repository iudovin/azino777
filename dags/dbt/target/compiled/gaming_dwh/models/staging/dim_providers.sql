

SELECT 
    provider_id::INT AS provider_id, 
    provider_name::VARCHAR(100) AS provider_name
FROM "datastack"."raw"."providers_map"