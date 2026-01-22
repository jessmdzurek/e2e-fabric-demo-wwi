CREATE TABLE [dc].[DataContract] (
    [ContractId]          INT            IDENTITY (1, 1) NOT NULL,
    [DatasetId]           INT            NOT NULL,
    [ContractVersion]     VARCHAR (20)   DEFAULT ('1.0') NOT NULL,
    [EnforcementMode]     VARCHAR (10)   DEFAULT ('WARN') NOT NULL,
    [RequiredColumnsJson] NVARCHAR (MAX) NOT NULL,
    [Notes]               NVARCHAR (500) NULL,
    [IsActive]            BIT            DEFAULT ((1)) NOT NULL,
    [CreatedUtc]          DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([ContractId] ASC),
    CONSTRAINT [CK_DataContract_Mode] CHECK ([EnforcementMode]='BLOCK' OR [EnforcementMode]='WARN'),
    CONSTRAINT [FK_DataContract_Dataset] FOREIGN KEY ([DatasetId]) REFERENCES [cfg].[Dataset] ([DatasetId])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_DataContract_Dataset]
    ON [dc].[DataContract]([DatasetId] ASC);


GO

