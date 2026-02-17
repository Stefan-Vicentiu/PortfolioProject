-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test_data')
BEGIN
    EXEC('CREATE SCHEMA [test_data]');
END
GO


CREATE OR ALTER PROCEDURE [test_data].[pr_test_dim_documents]
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
    -- check if the table exists in the test_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Documents' 
            AND s.name = 'test_data'
    )
    BEGIN
        -- create Documents dimension
        CREATE TABLE [test_data].[Documents] (
            Document_ID VARCHAR(255) NOT NULL,
            Document_Number VARCHAR(255),
            Document_Type VARCHAR(100),
            Document_Date DATE,
            Status VARCHAR(50)
        );
    END

    -- >>>>>   Full Refresh Strategy: Truncate and Reload   <<<<<
    TRUNCATE TABLE [test_data].[Documents];

    -- Insert all rows from Lakehouse
    INSERT INTO [test_data].[Documents] (
        Document_ID,
        Document_Number,
        Document_Type,
        Document_Date,
        Status
    )
    SELECT 
        Document_ID,
        Document_Number,
        Document_Type,
        Document_Date,
        Status
    FROM [Lakehouse_RawData].[dbo].[dim_documents];

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
			'test_data',
			'pr_test_dim_documents',
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