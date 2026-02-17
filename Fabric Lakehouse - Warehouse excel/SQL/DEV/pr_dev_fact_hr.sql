
-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dev_data')
BEGIN
    EXEC('CREATE SCHEMA [dev_data]');
END
GO


CREATE OR ALTER PROCEDURE [dev_data].[pr_dev_fact_hr]
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
    -- Check if the table exists in the dev_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Hr' 
            AND s.name = 'dev_data'
    )
    BEGIN
        -- Create Hr fact
        CREATE TABLE [dev_data].[Hr] (
            HR_ID INT NOT NULL,
            Date_ID INT,
            Transaction_Date DATE,
            Employee_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Account_ID VARCHAR(255),
            Transaction_Type VARCHAR(255),
            Payroll_Period VARCHAR(100),
            Payment_Method VARCHAR(100),
            Payment_Status VARCHAR(100),
            Amount DECIMAL(38, 2),
            Hours_Worked DECIMAL(38, 2),
            Overtime_Hours DECIMAL(38, 2),
            Hourly_Rate DECIMAL(38, 2),
            Gross_Amount DECIMAL(38, 2),
            Bonus_Type VARCHAR(100),
            Training_Course VARCHAR(255),
            Travel_Destination VARCHAR(255),
            Cost_Center VARCHAR(255),
            Project_Code VARCHAR(255),
            Approved_By VARCHAR(255),
            Processed_By VARCHAR(255),
            Notes VARCHAR(1999),
            Approval_Date DATE,
            Processing_Date DATE,
            Tax_Amount DECIMAL(38, 2),
            Social_Security DECIMAL(38, 2),
            Health_Insurance DECIMAL(38, 2),
            Net_Amount DECIMAL(38, 2),
            Effective_Hourly_Rate DECIMAL(38, 2)
        );
    END

    -- Count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [Lakehouse_RawData].[dbo].[fact_hr] AS source
    WHERE NOT EXISTS (
        SELECT 1 FROM [dev_data].[Hr] AS target 
        WHERE target.HR_ID = source.HR_ID
    )
        AND source.Processing_Date >= (SELECT ISNULL(MAX(Processing_Date), '1900-01-01') FROM [dev_data].[Hr]);

    -- Count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [dev_data].[Hr] AS target
    INNER JOIN [Lakehouse_RawData].[dbo].[fact_hr] AS source
        ON target.HR_ID = source.HR_ID
    WHERE (
        target.Processing_Date <> source.Processing_Date
        OR target.Net_Amount <> source.Net_Amount
        OR target.Payment_Status <> source.Payment_Status
        OR target.Hours_Worked <> source.Hours_Worked
    )
        AND source.Processing_Date >= (SELECT ISNULL(MAX(Processing_Date), '1900-01-01') FROM [dev_data].[Hr]);

    -- Incremental Load
    MERGE INTO [dev_data].[Hr] AS To_Target 
        USING (
            SELECT
                HR_ID,
                Date_ID,
                Transaction_Date,
                Employee_ID,
                Business_Line_ID,
                Account_ID,
                Transaction_Type,
                Payroll_Period,
                Payment_Method,
                Payment_Status,
                CAST(Amount AS DECIMAL(38, 2)) AS Amount,
                CAST(Hours_Worked AS DECIMAL(38, 2)) AS Hours_Worked,
                CAST(Overtime_Hours AS DECIMAL(38, 2)) AS Overtime_Hours,
                CAST(Hourly_Rate AS DECIMAL(38, 2)) AS Hourly_Rate,
                CAST(Gross_Amount AS DECIMAL(38, 2)) AS Gross_Amount,
                Bonus_Type,
                Training_Course,
                Travel_Destination,
                Cost_Center,
                Project_Code,
                Approved_By,
                Processed_By,
                Notes,
                Approval_Date,
                Processing_Date,
                CAST(Tax_Amount AS DECIMAL(38, 2)) AS Tax_Amount,
                CAST(Social_Security AS DECIMAL(38, 2)) AS Social_Security,
                CAST(Health_Insurance AS DECIMAL(38, 2)) AS Health_Insurance,
                CAST(Net_Amount AS DECIMAL(38, 2)) AS Net_Amount,
                CAST(Effective_Hourly_Rate AS DECIMAL(38, 2)) AS Effective_Hourly_Rate

            FROM [Lakehouse_RawData].[dbo].[fact_hr]
            WHERE Processing_Date >= ( SELECT ISNULL(MAX(Processing_Date), '1900-01-01') FROM [dev_data].[Hr] )
        ) AS From_Source
            ON ( To_Target.HR_ID = From_Source.HR_ID)

        -- Row exists, but data change (UPDATE)
        WHEN MATCHED AND (
            To_Target.Processing_Date <> From_Source.Processing_Date
            OR To_Target.Net_Amount <> From_Source.Net_Amount
            OR To_Target.Payment_Status <> From_Source.Payment_Status
            OR To_Target.Hours_Worked <> From_Source.Hours_Worked
        )
        THEN UPDATE SET
            To_Target.Date_ID = From_Source.Date_ID,
            To_Target.Transaction_Date = From_Source.Transaction_Date,
            To_Target.Account_ID = From_Source.Account_ID,
            To_Target.Business_Line_ID = From_Source.Business_Line_ID,
            To_Target.Employee_ID = From_Source.Employee_ID,
            To_Target.Transaction_Type = From_Source.Transaction_Type,
            To_Target.Payroll_Period = From_Source.Payroll_Period,
            To_Target.Payment_Method = From_Source.Payment_Method,
            To_Target.Payment_Status = From_Source.Payment_Status,
            To_Target.Amount = From_Source.Amount,
            To_Target.Hours_Worked = From_Source.Hours_Worked,
            To_Target.Overtime_Hours = From_Source.Overtime_Hours,
            To_Target.Gross_Amount = From_Source.Gross_Amount,
            To_Target.Bonus_Type = From_Source.Bonus_Type,
            To_Target.Training_Course = From_Source.Training_Course,
            To_Target.Travel_Destination = From_Source.Travel_Destination,
            To_Target.Cost_Center = From_Source.Cost_Center,
            To_Target.Project_Code = From_Source.Project_Code,
            To_Target.Approved_By = From_Source.Approved_By,
            To_Target.Processed_By = From_Source.Processed_By,
            To_Target.Notes = From_Source.Notes,
            To_Target.Approval_Date = From_Source.Approval_Date,
            To_Target.Processing_Date = From_Source.Processing_Date,
            To_Target.Tax_Amount = From_Source.Tax_Amount,
            To_Target.Social_Security = From_Source.Social_Security,
            To_Target.Health_Insurance = From_Source.Health_Insurance,
            To_Target.Net_Amount = From_Source.Net_Amount,
            To_Target.Effective_Hourly_Rate = From_Source.Effective_Hourly_Rate

        -- Row does not exist (INSERT)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT(
                HR_ID,
                Date_ID,
                Transaction_Date,
                Employee_ID,
                Business_Line_ID,
                Account_ID,
                Transaction_Type,
                Payroll_Period,
                Payment_Method,
                Payment_Status,
                Amount,
                Hours_Worked,
                Overtime_Hours,
                Hourly_Rate,
                Gross_Amount,
                Bonus_Type,
                Training_Course,
                Travel_Destination,
                Cost_Center,
                Project_Code,
                Approved_By,
                Processed_By,
                Notes,
                Approval_Date,
                Processing_Date,
                Tax_Amount,
                Social_Security,
                Health_Insurance,
                Net_Amount,
                Effective_Hourly_Rate
            )
            VALUES (
                From_Source.HR_ID,
                From_Source.Date_ID,
                From_Source.Transaction_Date,
                From_Source.Employee_ID,
                From_Source.Business_Line_ID,
                From_Source.Account_ID,
                From_Source.Transaction_Type,
                From_Source.Payroll_Period,
                From_Source.Payment_Method,
                From_Source.Payment_Status,
                From_Source.Amount,
                From_Source.Hours_Worked,
                From_Source.Overtime_Hours,
                From_Source.Hourly_Rate,
                From_Source.Gross_Amount,
                From_Source.Bonus_Type,
                From_Source.Training_Course,
                From_Source.Travel_Destination,
                From_Source.Cost_Center,
                From_Source.Project_Code,
                From_Source.Approved_By,
                From_Source.Processed_By,
                From_Source.Notes,
                From_Source.Approval_Date,
                From_Source.Processing_Date,
                From_Source.Tax_Amount,
                From_Source.Social_Security,
                From_Source.Health_Insurance,
                From_Source.Net_Amount,
                From_Source.Effective_Hourly_Rate
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
			'dev_data',
			'pr_dev_fact_hr',
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


