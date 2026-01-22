CREATE   PROCEDURE log.sp_IngestRun_End
(
    @PipelineRunId varchar(100),
    @DatasetId int,
    @Status varchar(20),
    @RowsRead bigint = NULL,
    @RowsWritten bigint = NULL,
    @Message nvarchar(2000) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE log.IngestRun
    SET
        EndUtc      = SYSUTCDATETIME(),
        Status      = @Status,
        RowsRead    = COALESCE(@RowsRead, RowsRead),
        RowsWritten = COALESCE(@RowsWritten, RowsWritten),
        Message     = COALESCE(@Message, Message)
    WHERE PipelineRunId = @PipelineRunId
      AND DatasetId = @DatasetId
      AND Status = 'STARTED';  -- optional guardrail: update the active row

    IF @@ROWCOUNT = 0
        THROW 50002, 'No STARTED row found for this PipelineRunId/DatasetId.', 1;
END;

GO

