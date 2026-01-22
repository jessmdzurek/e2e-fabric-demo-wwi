
CREATE VIEW dbo.vw_ActiveDatasets
AS
SELECT
  s.SourceSystemName,
  s.GatewayName,
  d.DatasetId,
  d.SourceSchema,
  d.SourceObject,
  d.LoadType,
  d.WatermarkColumn,
  d.TargetBronzeTable,
  d.DoSilver,
  d.TargetSilverTable,
  d.PrimaryKeyJson,
  dc.EnforcementMode,
  dc.RequiredColumnsJson
FROM cfg.Dataset d
JOIN ref.SourceSystem s ON d.SourceSystemId = s.SourceSystemId
LEFT JOIN dc.DataContract dc ON dc.DatasetId = d.DatasetId AND dc.IsActive = 1
WHERE d.IsActive = 1 AND s.IsActive = 1;

GO

