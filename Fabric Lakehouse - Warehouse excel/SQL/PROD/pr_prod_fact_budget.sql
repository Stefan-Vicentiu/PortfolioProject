-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prod_data')
BEGIN
    EXEC('CREATE SCHEMA [prod_data]');
END
GO

CREATE OR ALTER PROCEDURE [prod_data].[pr_prod_fact_budget]
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
    -- Check if the table exists in the prod_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Budget' 
            AND s.name = 'prod_data'
    )
    BEGIN
        -- Create Budget fact in prod_data
        CREATE TABLE [prod_data].[Budget] (
            Budget_ID INT NOT NULL,
            Date_ID INT,
            [Budget Period] DATE,
            Account_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Employee_ID VARCHAR(255),
            [Budget Version] VARCHAR(255),
            [Budget Type] VARCHAR(255),
            [Budget Category] VARCHAR(255),
            [Cost Center] VARCHAR(255),
            [Project Code] VARCHAR(255),
            [Budgeted Amount] DECIMAL(38, 2),
            [Actual Amount] DECIMAL(38, 2),
            [Committed Amount] DECIMAL(38, 2),
            [Forecast Amount] DECIMAL(38, 2),
            [Budget Owner] VARCHAR(255),
            [Approval Status] VARCHAR(255),
            [Approved By] VARCHAR(255),
            Notes VARCHAR(1999),
            [Approval Date] DATE,
            [Last Modified Date] DATE,
            Variance DECIMAL(38, 2),
            [Variance Pct] DECIMAL(38, 10),
            [Available Budget] DECIMAL(38, 2),
            [Utilization Pct] DECIMAL(38, 10),
            [Forecast Variance] DECIMAL(38, 2)
        );
    END

    DECLARE @MaxModifiedDate DATE = (SELECT ISNULL(MAX([Last Modified Date]), '1900-01-01') FROM [prod_data].[Budget]);

    -- Count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [test_data].[Budget] AS From_Source
    WHERE NOT EXISTS (
        SELECT 1 FROM [prod_data].[Budget] AS To_Target 
        WHERE To_Target.Budget_ID = From_Source.Budget_ID
    )
    AND From_Source.Last_Modified_Date >= @MaxModifiedDate;

    -- Count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [prod_data].[Budget] AS To_Target
    INNER JOIN [test_data].[Budget] AS From_Source
        ON To_Target.Budget_ID = From_Source.Budget_ID
    WHERE (
        To_Target.[Last Modified Date] <> From_Source.Last_Modified_Date
        OR To_Target.[Actual Amount] <> From_Source.Actual_Amount
        OR To_Target.[Budgeted Amount] <> From_Source.Budgeted_Amount
        OR ISNULL(To_Target.[Approval Status], '') <> ISNULL(From_Source.Approval_Status, '')
    )
    AND From_Source.Last_Modified_Date >= @MaxModifiedDate;

    -- Incremental Load from test_data to prod_data
    MERGE INTO [prod_data].[Budget] AS To_Target 
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
            FROM [test_data].[Budget]
            WHERE Last_Modified_Date >= @MaxModifiedDate
        ) AS From_Source
            ON (To_Target.Budget_ID = From_Source.Budget_ID)

        -- Row exists, but data change (UPDATE)
        WHEN MATCHED AND (
            To_Target.[Last Modified Date] <> From_Source.Last_Modified_Date
            OR To_Target.[Actual Amount] <> From_Source.Actual_Amount
            OR To_Target.[Budgeted Amount] <> From_Source.Budgeted_Amount
            OR ISNULL(To_Target.[Approval Status], '') <> ISNULL(From_Source.Approval_Status, '')
        )
        THEN UPDATE SET
            To_Target.Date_ID = From_Source.Date_ID,
            To_Target.[Budget Period] = From_Source.Budget_Period,
            To_Target.Account_ID = From_Source.Account_ID,
            To_Target.Business_Line_ID = From_Source.Business_Line_ID,
            To_Target.Employee_ID = From_Source.Employee_ID,
            To_Target.[Budget Version] = From_Source.Budget_Version,
            To_Target.[Budget Type] = From_Source.Budget_Type,
            To_Target.[Budget Category] = From_Source.Budget_Category,
            To_Target.[Cost Center] = From_Source.Cost_Center,
            To_Target.[Project Code] = From_Source.Project_Code,
            To_Target.[Budgeted Amount] = From_Source.Budgeted_Amount,
            To_Target.[Actual Amount] = From_Source.Actual_Amount,
            To_Target.[Committed Amount] = From_Source.Committed_Amount,
            To_Target.[Forecast Amount] = From_Source.Forecast_Amount,
            To_Target.[Budget Owner] = From_Source.Budget_Owner,
            To_Target.[Approval Status] = From_Source.Approval_Status,
            To_Target.[Approved By] = From_Source.Approved_By,
            To_Target.Notes = From_Source.Notes,
            To_Target.[Approval Date] = From_Source.Approval_Date,
            To_Target.[Last Modified Date] = From_Source.Last_Modified_Date,
            To_Target.Variance = From_Source.Variance,
            To_Target.[Variance Pct] = From_Source.Variance_Pct,
            To_Target.[Available Budget] = From_Source.Available_Budget,
            To_Target.[Utilization Pct] = From_Source.Utilization_Pct,
            To_Target.[Forecast Variance] = From_Source.Forecast_Variance

        -- Row does not exist (INSERT)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                Budget_ID,
                Date_ID,
                [Budget Period],
                Account_ID,
                Business_Line_ID,
                Employee_ID,
                [Budget Version],
                [Budget Type],
                [Budget Category],
                [Cost Center],
                [Project Code],
                [Budgeted Amount],
                [Actual Amount],
                [Committed Amount],
                [Forecast Amount],
                [Budget Owner],
                [Approval Status],
                [Approved By],
                Notes,
                [Approval Date],
                [Last Modified Date],
                Variance,
                [Variance Pct],
                [Available Budget],
                [Utilization Pct],
                [Forecast Variance]
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
			'prod_data',
			'pr_prod_fact_budget',
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