-- Teradata column metadata. Schedule this as an export to CSV if Databricks can't reach Teradata,
-- then point a `teradata_export` source at the file. Replace the IN lists with your own names.
SELECT DatabaseName, TableName, ColumnName, ColumnId, ColumnType, ColumnLength,
       DecimalTotalDigits, DecimalFractionalDigits, Nullable, CharType
FROM DBC.ColumnsV
WHERE DatabaseName IN ('SALES')
  AND TableName IN ('CUSTOMERS', 'JOBS', 'INVOICES');
