
-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dev_data')
BEGIN
    EXEC('CREATE SCHEMA [dev_data]');
END
GO


CREATE OR ALTER PROCEDURE [dev_data].[pr_dev_dim_suppliers]
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
    -- check if the table exists in the dev_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Suppliers' 
            AND s.name = 'dev_data'
    )
    BEGIN
        -- create Suppliers dimension
        CREATE TABLE [dev_data].[Suppliers] (
            Supplier_ID VARCHAR(255) NOT NULL,
            Supplier_Name VARCHAR(255),
            Supplier_Type VARCHAR(100),
            Country VARCHAR(100),
            Payment_Terms VARCHAR(100),
            Quality_Rating INT,
            Is_Active BIT
        );
    END

    -- >>>>>   Full Refresh Strategy: Truncate and Reload   <<<<<
    TRUNCATE TABLE [dev_data].[Suppliers];

    -- Insert all rows from Lakehouse
    INSERT INTO [dev_data].[Suppliers] (
        Supplier_ID,
        Supplier_Name,
        Supplier_Type,
        Country,
        Payment_Terms,
        Quality_Rating,
        Is_Active
    )
    SELECT 
        Supplier_ID,
        Supplier_Name,
        Supplier_Type,
        Country,
        Payment_Terms,
        Quality_Rating,
        Is_Active
    FROM [Lakehouse_RawData].[dbo].[dim_suppliers];

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
			'dev_data',
			'pr_dev_dim_suppliers',
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