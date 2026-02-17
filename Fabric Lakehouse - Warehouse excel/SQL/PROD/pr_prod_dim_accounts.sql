-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prod_data')
BEGIN
    EXEC('CREATE SCHEMA [prod_data]');
END
GO

CREATE OR ALTER PROCEDURE [prod_data].[pr_prod_dim_accounts]
AS
BEGIN
    -- deactivate counting for performance
    SET NOCOUNT ON;

    -- declared logs variable
    DECLARE @StartTime DATETIME2(6) = SYSDATETIME();
    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @Status VARCHAR(50) = 'Success';
    DECLARE @ErrorMessage VARCHAR(MAX) = NULL;

BEGIN TRY
    -- check if the table exists in the prod_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Accounts' 
            AND s.name = 'prod_data'
    )
    BEGIN
        -- create Accounts dimension in prod_data
        CREATE TABLE [prod_data].[Accounts] (
            [Account Code] VARCHAR(255) NOT NULL,
            [Account Name] VARCHAR(255),
            [Account Type] VARCHAR(100),
            [Financial Statement] VARCHAR(100)
        );
    END

    -- >>>>>   Full Refresh Strategy: Truncate and Reload   <<<<<
    TRUNCATE TABLE [prod_data].[Accounts];

    -- insert all rows from test_data to prod_data
    INSERT INTO [prod_data].[Accounts] (
        [Account Code],
        [Account Name],
        [Account Type],
        [Financial Statement]
    )
    SELECT 
        Account_ID,
        Account_Name,
        Account_Type,
        Financial_Statement
        
    FROM [test_data].[Accounts];

    -- set the count of inserted rows
    SET @RowsInserted = @@ROWCOUNT;

END TRY
BEGIN CATCH
    SET @Status = 'Error';
    SET @ErrorMessage = ERROR_MESSAGE();
END CATCH

    -- send logs to the ExecutionLogs table
    INSERT INTO [admin_logging].[ExecutionLogs] (   
        SchemaName, 
        ProcedureName,
        StartTime,
        EndTime,
        RowsInserted,
        RowsUpdated,
        Status,
        ErrorMessage
    )
    VALUES (
        'prod_data',
        'pr_prod_dim_accounts',
        @StartTime,
        SYSDATETIME(),
        @RowsInserted,
        @RowsUpdated,
        @Status,
        @ErrorMessage
    );

    -- clean logs older than 40 days
    DELETE FROM [admin_logging].[ExecutionLogs]
    WHERE StartTime < DATEADD(DAY, -40, SYSDATETIME());

END;
GO