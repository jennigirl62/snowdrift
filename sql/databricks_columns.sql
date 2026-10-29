-- Databricks (Unity Catalog) column metadata (what DatabricksSource runs).
-- Note: ordinal_position starts at 0, and integer precision is reported in bits; the agent handles both.
SELECT table_schema, table_name, column_name, ordinal_position, full_data_type,
       character_maximum_length, numeric_precision, numeric_scale, is_nullable
FROM main.information_schema.columns
WHERE lower(table_schema) IN ('landscape', 'stage');
