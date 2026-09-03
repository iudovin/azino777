
  
    

  create  table "datastack"."dds"."dim_players__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
    id::INT AS player_id,
    registration_date::DATE AS registration_date,
    registration_type::VARCHAR(50) AS registration_type,
    country::VARCHAR(10) AS country
FROM "datastack"."raw"."players"
  );
  