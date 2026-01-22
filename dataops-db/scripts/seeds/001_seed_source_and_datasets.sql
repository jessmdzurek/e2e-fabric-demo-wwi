/* ============================================================
   Seed: DataOps Control Tables (MVP) - WideWorldImporters (OLTP)
   Target: Fabric SQL Database (DataOps DB)

   What this does:
   - Seeds one Source System record for WWI OLTP via your on-prem gateway
   - Seeds 5 datasets (Sales.*) with standardized Bronze target names
   - Safe to re-run (idempotent) using MERGE

   Naming approach (Bronze):
     brz_<source>_<schema>_<object>
   Example:
     Sales.Orders -> brz_wwi_sales_orders
   ============================================================ */

SET NOCOUNT ON;

BEGIN TRAN;

---------------------------------------------------------------
-- 1) Seed Source System
---------------------------------------------------------------
MERGE ref.SourceSystem AS tgt
USING (VALUES
  -- SourceSystemName     GatewayName             Notes
  ('sqlsrv_wwi_oltp',     'fwg-dev-westus3-demo', 'WideWorldImporters OLTP (SQL Server) via Fabric data gateway')
) AS src (SourceSystemName, GatewayName, Notes)
ON tgt.SourceSystemName = src.SourceSystemName
WHEN MATCHED THEN
  UPDATE SET
    tgt.GatewayName = src.GatewayName,
    tgt.Notes       = src.Notes,
    tgt.IsActive    = 1
WHEN NOT MATCHED THEN
  INSERT (SourceSystemName, GatewayName, Notes, IsActive)
  VALUES (src.SourceSystemName, src.GatewayName, src.Notes, 1);

DECLARE @SourceSystemId int =
(
  SELECT SourceSystemId
  FROM ref.SourceSystem
  WHERE SourceSystemName = 'sqlsrv_wwi_oltp'
);

---------------------------------------------------------------
-- 2) Seed Datasets (Sales.*)
--    MVP choice: FULL loads (fastest). Add INCR later.
--    MVP choice: Bronze targets only (DoSilver=0).
---------------------------------------------------------------
MERGE cfg.Dataset AS tgt
USING (VALUES
  -- SourceSchema, SourceObject,       LoadType, WatermarkColumn, TargetBronzeTable,           DoSilver, TargetSilverTable, PrimaryKeyJson, Notes
  ('Sales',       'Orders',            'FULL',   NULL,            'brz_wwi_sales_orders',      0,       NULL,            NULL,           'WWI OLTP Sales.Orders (Bronze source-aligned)'),
  ('Sales',       'Invoices',          'FULL',   NULL,            'brz_wwi_sales_invoices',    0,       NULL,            NULL,           'WWI OLTP Sales.Invoices (Bronze source-aligned)'),
  ('Sales',       'OrderLines',        'FULL',   NULL,            'brz_wwi_sales_orderlines',  0,       NULL,            NULL,           'WWI OLTP Sales.OrderLines (Bronze source-aligned)'),
  ('Sales',       'InvoiceLines',      'FULL',   NULL,            'brz_wwi_sales_invoicelines',0,       NULL,            NULL,           'WWI OLTP Sales.InvoiceLines (Bronze source-aligned)'),
  ('Sales',       'Customers',         'FULL',   NULL,            'brz_wwi_sales_customers',   0,       NULL,            NULL,           'WWI OLTP Sales.Customers (Bronze source-aligned)')
) AS src (SourceSchema, SourceObject, LoadType, WatermarkColumn, TargetBronzeTable, DoSilver, TargetSilverTable, PrimaryKeyJson, Notes)
ON  tgt.SourceSystemId = @SourceSystemId
AND tgt.SourceSchema   = src.SourceSchema
AND tgt.SourceObject   = src.SourceObject
WHEN MATCHED THEN
  UPDATE SET
    tgt.LoadType          = src.LoadType,
    tgt.WatermarkColumn   = src.WatermarkColumn,
    tgt.TargetBronzeTable = src.TargetBronzeTable,
    tgt.DoSilver          = src.DoSilver,
    tgt.TargetSilverTable = src.TargetSilverTable,
    tgt.PrimaryKeyJson    = src.PrimaryKeyJson,
    tgt.Notes             = src.Notes,
    tgt.IsActive          = 1
WHEN NOT MATCHED THEN
  INSERT
  (
    SourceSystemId, SourceSchema, SourceObject,
    LoadType, WatermarkColumn,
    TargetBronzeTable, DoSilver, TargetSilverTable,
    PrimaryKeyJson, Notes, IsActive
  )
  VALUES
  (
    @SourceSystemId, src.SourceSchema, src.SourceObject,
    src.LoadType, src.WatermarkColumn,
    src.TargetBronzeTable, src.DoSilver, src.TargetSilverTable,
    src.PrimaryKeyJson, src.Notes, 1
  );

COMMIT TRAN;

---------------------------------------------------------------
-- 3) Quick verification queries
---------------------------------------------------------------
SELECT *
FROM ref.SourceSystem
WHERE SourceSystemName = 'sqlsrv_wwi_oltp';

SELECT
  d.DatasetId,
  s.SourceSystemName,
  d.SourceSchema,
  d.SourceObject,
  d.LoadType,
  d.TargetBronzeTable,
  d.DoSilver,
  d.TargetSilverTable,
  d.IsActive
FROM cfg.Dataset d
JOIN ref.SourceSystem s ON d.SourceSystemId = s.SourceSystemId
WHERE s.SourceSystemName = 'sqlsrv_wwi_oltp'
ORDER BY d.DatasetId;
