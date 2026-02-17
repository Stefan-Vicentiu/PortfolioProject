-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test_data')
BEGIN
    EXEC('CREATE SCHEMA [test_data]');
END
GO


CREATE OR ALTER PROCEDURE [test_data].[pr_test_fact_budget]
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
    -- Check if the table exists in the test_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Budget' 
            AND s.name = 'test_data'
    )
    BEGIN
        -- Create Budget fact
        CREATE TABLE [test_data].[Budget] (
            Budget_ID INT NOT NULL,
            Date_ID INT,
            Budget_Period DATE,
            Account_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Employee_ID VARCHAR(255),
            Budget_Version VARCHAR(255),
            Budget_Type VARCHAR(255),
            Budget_Category VARCHAR(255),
            Cost_Center VARCHAR(255),
            Project_Code VARCHAR(255),
            Budgeted_Amount DECIMAL(38, 2),
            Actual_Amount DECIMAL(38, 2),
            Committed_Amount DECIMAL(38, 2),
            Forecast_Amount DECIMAL(38, 2),
            Budget_Owner VARCHAR(255),
            Approval_Status VARCHAR(255),
            Approved_By VARCHAR(255),
            Notes VARCHAR(1999),
            Approval_Date DATE,
            Last_Modified_Date DATE,
            Variance DECIMAL(38, 2),
            Variance_Pct DECIMAL(38, 10),
            Available_Budget DECIMAL(38, 2),
            Utilization_Pct DECIMAL(38, 10),
            Forecast_Variance DECIMAL(38, 2)
        );
    END

    -- Count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [Lakehouse_RawData].[dbo].[fact_budget] AS source
    WHERE NOT EXISTS (
        SELECT 1 FROM [test_data].[Budget] AS target 
        WHERE target.Budget_ID = source.Budget_ID
    )
        AND source.Last_Modified_Date >= (SELECT ISNULL(MAX(Last_Modified_Date), '1900-01-01') FROM [test_data].[Budget]);

    -- Count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [test_data].[Budget] AS target
    INNER JOIN [Lakehouse_RawData].[dbo].[fact_budget] AS source
        ON target.Budget_ID = source.Budget_ID
    WHERE (
        target.Last_Modified_Date <> source.Last_Modified_Date
        OR target.Actual_Amount <> source.Actual_Amount
        OR target.Budgeted_Amount <> source.Budgeted_Amount
        OR ISNULL(target.Approval_Status, '') <> ISNULL(source.Approval_Status, '')
    )
        AND source.Last_Modified_Date >= (SELECT ISNULL(MAX(Last_Modified_Date), '1900-01-01') FROM [test_data].[Budget]);

    -- Incremental Load
    MERGE INTO [test_data].[Budget] AS To_Target 
        USING (
            SELECT
                Budget_ID,
                Date_ID,
                Budget_Period,
                Account_ID,
                Business_Line_ID,
                Employee_ID,
                Budget_Version,
                Budget_Type,
                Budget_Category,
                Cost_Center,
                Project_Code,
                CAST(Budgeted_Amount AS DECIMAL(38, 2)) AS Budgeted_Amount,
                CAST(Actual_Amount AS DECIMAL(38, 2)) AS Actual_Amount,
                CAST(Committed_Amount AS DECIMAL(38, 2)) AS Committed_Amount,
                CAST(Forecast_Amount AS DECIMAL(38, 2)) AS Forecast_Amount,
                Budget_Owner,
                Approval_Status,
                Approved_By,
                Notes,
                Approval_Date,
                Last_Modified_Date,
                CAST(Variance AS DECIMAL(38, 2)) AS Variance,
                CAST(Variance_Pct AS DECIMAL(38, 10)) AS Variance_Pct,
                CAST(Available_Budget AS DECIMAL(38, 2)) AS Available_Budget,
                CAST(Utilization_Pct AS DECIMAL(38, 10)) AS Utilization_Pct,
                CAST(Forecast_Variance AS DECIMAL(38, 2)) AS Forecast_Variance

            FROM [Lakehouse_RawData].[dbo].[fact_budget]
            WHERE Last_Modified_Date >= ( SELECT ISNULL(MAX(Last_Modified_Date), '1900-01-01') FROM [test_data].[Budget] )
        ) AS From_Source
            ON ( To_Target.Budget_ID = From_Source.Budget_ID)

        -- Row exists, but data change (UPDATE)
        WHEN MATCHED AND (
            To_Target.Last_Modified_Date <> From_Source.Last_Modified_Date
            OR To_Target.Actual_Amount <> From_Source.Actual_Amount
            OR To_Target.Budgeted_Amount <> From_Source.Budgeted_Amount
            OR ISNULL(To_Target.Approval_Status, '') <> ISNULL(From_Source.Approval_Status, '')
        )
        THEN UPDATE SET
            To_Target.Date_ID = From_Source.Date_ID,
            To_Target.Budget_Period = From_Source.Budget_Period,
            To_Target.Account_ID = From_Source.Account_ID,
            To_Target.Business_Line_ID = From_Source.Business_Line_ID,
            To_Target.Employee_ID = From_Source.Employee_ID,
            To_Target.Budget_Version = From_Source.Budget_Version,
            To_Target.Budget_Type = From_Source.Budget_Type,
            To_Target.Budget_Category = From_Source.Budget_Category,
            To_Target.Cost_Center = From_Source.Cost_Center,
            To_Target.Project_Code = From_Source.Project_Code,
            To_Target.Budgeted_Amount = From_Source.Budgeted_Amount,
            To_Target.Actual_Amount = From_Source.Actual_Amount,
            To_Target.Committed_Amount = From_Source.Committed_Amount,
            To_Target.Forecast_Amount = From_Source.Forecast_Amount,
            To_Target.Budget_Owner = From_Source.Budget_Owner,
            To_Target.Approval_Status = From_Source.Approval_Status,
            To_Target.Approved_By = From_Source.Approved_By,
            To_Target.Notes = From_Source.Notes,
            To_Target.Approval_Date = From_Source.Approval_Date,
            To_Target.Last_Modified_Date = From_Source.Last_Modified_Date,
            To_Target.Variance = From_Source.Variance,
            To_Target.Variance_Pct = From_Source.Variance_Pct,
            To_Target.Available_Budget = From_Source.Available_Budget,
            To_Target.Utilization_Pct = From_Source.Utilization_Pct,
            To_Target.Forecast_Variance = From_Source.Forecast_Variance

        -- Row does not exist (INSERT)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT(
                Budget_ID,
                Date_ID,
                Budget_Period,
                Account_ID,
                Business_Line_ID,
                Employee_ID,
                Budget_Version,
                Budget_Type,
                Budget_Category,
                Cost_Center,
                Project_Code,
                Budgeted_Amount,
                Actual_Amount,
                Committed_Amount,
                Forecast_Amount,
                Budget_Owner,
                Approval_Status,
                Approved_By,
                Notes,
                Approval_Date,
                Last_Modified_Date,
                Variance,
                Variance_Pct,
                Available_Budget,
                Utilization_Pct,
                Forecast_Variance
            )
            VALUES (
                From_Source.Budget_ID,
                From_Source.Date_ID,
                From_Source.Budget_Period,
                From_Source.Account_ID,
                From_Source.Business_Line_ID,
                From_Source.Employee_ID,
                From_Source.Budget_Version,
                From_Source.Budget_Type,
                From_Source.Budget_Category,
                From_Source.Cost_Center,
                From_Source.Project_Code, 
                From_Source.Budgeted_Amount,
                From_Source.Actual_Amount,
                From_Source.Committed_Amount,
                From_Source.Forecast_Amount, 
                From_Source.Budget_Owner,
                From_Source.Approval_Status,
                From_Source.Approved_By,
                From_Source.Notes,
                From_Source.Approval_Date,
                From_Source.Last_Modified_Date,
                From_Source.Variance,
                From_Source.Variance_Pct,
                From_Source.Available_Budget,
                From_Source.Utilization_Pct,
                From_Source.Forecast_Variance
            );
        

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
			'pr_test_fact_budget',
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