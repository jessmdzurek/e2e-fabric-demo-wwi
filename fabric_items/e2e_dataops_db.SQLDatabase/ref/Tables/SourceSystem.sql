CREATE TABLE [ref].[SourceSystem] (
    [SourceSystemId]   INT            IDENTITY (1, 1) NOT NULL,
    [SourceSystemName] NVARCHAR (100) NOT NULL,
    [GatewayName]      NVARCHAR (200) NULL,
    [Notes]            NVARCHAR (500) NULL,
    [IsActive]         BIT            DEFAULT ((1)) NOT NULL,
    [CreatedUtc]       DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([SourceSystemId] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_SourceSystem_Name]
    ON [ref].[SourceSystem]([SourceSystemName] ASC);


GO

