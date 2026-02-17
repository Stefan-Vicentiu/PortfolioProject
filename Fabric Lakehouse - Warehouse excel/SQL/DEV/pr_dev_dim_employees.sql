
-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dev_data')
BEGIN
    EXEC('CREATE SCHEMA [dev_data]');
END
GO


CREATE OR ALTER PROCEDURE [dev_data].[pr_dev_dim_employees]
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
        WHERE t.name = 'Employees' 
            AND s.name = 'dev_data'
    )
    BEGIN
        -- create Employees dimension
        CREATE TABLE [dev_data].[Employees] (
            Employee_ID VARCHAR(255) NOT NULL,
            Employee_Name VARCHAR(255),
            Department VARCHAR(100),
            Position VARCHAR(100),
            Location VARCHAR(100),
            Hire_Date DATE,
            Salary DECIMAL(18,2),
            Is_Active BIT
        );
    END

    -- >>>>>   Full Refresh Strategy: Truncate and Reload   <<<<<
    TRUNCATE TABLE [dev_data].[Employees];

    -- Insert all rows from Lakehouse
    INSERT INTO [dev_data].[Employees] (
        Employee_ID,
        Employee_Name,
        Department,
        Position,
        Location,
        Hire_Date,
        Salary,
        Is_Active
    )
    SELECT 
        Employee_ID,
        Employee_Name,
        Department,
        Position,
        Location,
        Hire_Date,
        Salary,
        Is_Active
    FROM [Lakehouse_RawData].[dbo].[dim_employees];

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
			'pr_dev_dim_employees',
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