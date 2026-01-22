CREATE   PROCEDURE log.sp_IngestRun_Start
(
    @PipelineRunId varchar(100),
    @DatasetId int,
    @Message nvarchar(2000) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.IngestRun
    (
        RunId,           -- uniqueidentifier PK (existing)
        PipelineRunId,   -- new string column
        DatasetId,
        StartUtc,
        Status,
        Message
    )
    VALUES
    (
        NEWID(),
        @PipelineRunId,
        @DatasetId,
        SYSUTCDATETIME(),
        'STARTED',
        @Message
    );
END;

GO

