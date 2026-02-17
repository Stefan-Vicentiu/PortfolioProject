CREATE OR ALTER PROCEDURE [admin_logging].[pr_initialize_logging]
AS
BEGIN
    -- deactivate counting for performance
    SET NOCOUNT ON;

    -- only create the schema if it doesn't exist
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'admin_logging')
    BEGIN
        EXEC('CREATE SCHEMA [admin_logging]');
    END;


    IF NOT EXISTS (
        SELECT * 
        FROM sys.tables t 
            JOIN sys.schemas s 
                ON t.schema_id = s.schema_id 
        WHERE t.name = 'ExecutionLogs' 
            AND s.name = 'admin_logging'
        )

    BEGIN
        CREATE TABLE [admin_logging].[ExecutionLogs] (
            LogID BIGINT IDENTITY NOT NULL,
            SchemaName VARCHAR(50),
            ProcedureName VARCHAR(100),
            StartTime DATETIME2(0),
            EndTime DATETIME2(0),
            RowsInserted BIGINT,
            RowsUpdated BIGINT,
            Status VARCHAR(50),
            ErrorMessage VARCHAR(MAX)
        );
    END;
END;