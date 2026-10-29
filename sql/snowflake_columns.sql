-- Snowflake column metadata (what SnowflakeSource runs). Useful as a view other tools can read.
SELECT table_schema, table_name, column_name, ordinal_position, data_type,
       character_maximum_length, numeric_precision, numeric_scale, is_nullable, collation_name
FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema IN ('PUBLIC', 'STAGE');
