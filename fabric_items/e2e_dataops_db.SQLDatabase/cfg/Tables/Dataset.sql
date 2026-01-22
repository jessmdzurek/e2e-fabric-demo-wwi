CREATE TABLE [cfg].[Dataset] (
    [DatasetId]         INT            IDENTITY (1, 1) NOT NULL,
    [SourceSystemId]    INT            NOT NULL,
    [SourceSchema]      NVARCHAR (128) NOT NULL,
    [SourceObject]      NVARCHAR (128) NOT NULL,
    [LoadType]          VARCHAR (10)   NOT NULL,
    [WatermarkColumn]   NVARCHAR (128) NULL,
    [TargetBronzeTable] NVARCHAR (200) NOT NULL,
    [DoSilver]          BIT            DEFAULT ((0)) NOT NULL,
    [TargetSilverTable] NVARCHAR (200) NULL,
    [PrimaryKeyJson]    NVARCHAR (MAX) NULL,
    [Notes]             NVARCHAR (500) NULL,
    [IsActive]          BIT            DEFAULT ((1)) NOT NULL,
    [CreatedUtc]        DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([DatasetId] ASC),
    CONSTRAINT [CK_Dataset_IncrementalWatermark] CHECK ([LoadType]<>'INCR' OR [WatermarkColumn] IS NOT NULL),
    CONSTRAINT [CK_Dataset_LoadType] CHECK ([LoadType]='INCR' OR [LoadType]='FULL'),
    CONSTRAINT [CK_Dataset_SilverTarget] CHECK ([DoSilver]=(0) OR [TargetSilverTable] IS NOT NULL),
    CONSTRAINT [FK_Dataset_SourceSystem] FOREIGN KEY ([SourceSystemId]) REFERENCES [ref].[SourceSystem] ([SourceSystemId])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Dataset_SourceObject]
    ON [cfg].[Dataset]([SourceSystemId] ASC, [SourceSchema] ASC, [SourceObject] ASC);


GO

