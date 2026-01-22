CREATE TABLE [log].[IngestRun] (
    [RunId]          UNIQUEIDENTIFIER NOT NULL,
    [DatasetId]      INT              NOT NULL,
    [StartUtc]       DATETIME2 (3)    DEFAULT (sysutcdatetime()) NOT NULL,
    [EndUtc]         DATETIME2 (3)    NULL,
    [Status]         VARCHAR (20)     NOT NULL,
    [RowsRead]       BIGINT           NULL,
    [RowsWritten]    BIGINT           NULL,
    [WatermarkStart] NVARCHAR (200)   NULL,
    [WatermarkEnd]   NVARCHAR (200)   NULL,
    [Message]        NVARCHAR (2000)  NULL,
    PRIMARY KEY CLUSTERED ([RunId] ASC),
    CONSTRAINT [CK_IngestRun_Status] CHECK ([Status]='FAILED' OR [Status]='WARNED' OR [Status]='SUCCEEDED' OR [Status]='STARTED'),
    CONSTRAINT [FK_IngestRun_Dataset] FOREIGN KEY ([DatasetId]) REFERENCES [cfg].[Dataset] ([DatasetId])
);


GO

