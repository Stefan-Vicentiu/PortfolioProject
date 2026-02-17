-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prod_data')
BEGIN
    EXEC('CREATE SCHEMA [prod_data]');
END
GO

CREATE OR ALTER PROCEDURE [prod_data].[pr_prod_dim_suppliers]
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
        WHERE t.name = 'Suppliers' 
            AND s.name = 'prod_data'
    )
    BEGIN
        -- create Suppliers dimension in prod_data
        CREATE TABLE [prod_data].[Suppliers] (
            [Supplier Code] VARCHAR(255) NOT NULL,
            [Supplier Name] VARCHAR(255),
            [Supplier Type] VARCHAR(100),
            Country VARCHAR(100),
            [Payment Terms] VARCHAR(100),
            [Quality Rating] INT,
            [Is Active] BIT
        );
    END

    -- >>>>>   Full Refresh Strategy: Truncate and Reload   <<<<<
    TRUNCATE TABLE [prod_data].[Suppliers];

    -- Insert all rows from test_data to prod_data
    INSERT INTO [prod_data].[Suppliers] (
        [Supplier Code],
        [Supplier Name],
        [Supplier Type],
        Country,
        [Payment Terms],
        [Quality Rating],
        [Is Active]
    )
    SELECT 
        Supplier_ID,
        Supplier_Name,
        Supplier_Type,
        Country,
        Payment_Terms,
        Quality_Rating,
        Is_Active

    FROM [test_data].[Suppliers];

    -- set the count of inserted rows
    SET @RowsInserted = @@ROWCOUNT;

END TRY

BEGIN CATCH
        SET @Status = 'Error';
        SET @ErrorMessage = LEFT(ERROR_MESSAGE(), 4000);
END CATCH

    BEGIN TRY
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
			'pr_prod_dim_suppliers',
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
    END TRY
	
    BEGIN CATCH
        -- Send Error to the pipeline
        DECLARE @LogErr VARCHAR(MAX) = ERROR_MESSAGE();
        RAISERROR(@LogErr, 16, 1);
    END CATCH

    IF @Status = 'Error'
    BEGIN
        RAISERROR(@ErrorMessage, 16, 1);
    END
END;