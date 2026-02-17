-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prod_data')
BEGIN
    EXEC('CREATE SCHEMA [prod_data]');
END
GO


CREATE OR ALTER PROCEDURE [prod_data].[pr_prod_fact_hr]
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
        WHERE t.name = 'Hr' 
            AND s.name = 'prod_data'
    )
    BEGIN
        -- create Hr fact in prod_data
        CREATE TABLE [prod_data].[Hr] (
            HR_ID INT NOT NULL,
            Date_ID INT,
            [Transaction Date] DATE,
            Employee_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Account_ID VARCHAR(255),
            [Transaction Type] VARCHAR(255),
            [Payroll Period] VARCHAR(100),
            [Payment Method] VARCHAR(100),
            [Payment Status] VARCHAR(100),
            Amount DECIMAL(38, 2),
            [Hours Worked] DECIMAL(38, 2),
            [Overtime Hours] DECIMAL(38, 2),
            [Hourly Rate] DECIMAL(38, 2),
            [Gross Amount] DECIMAL(38, 2),
            [Bonus Type] VARCHAR(100),
            [Training Course] VARCHAR(255),
            [Travel Destination] VARCHAR(255),
            [Cost Center] VARCHAR(255),
            [Project Code] VARCHAR(255),
            [Approved By] VARCHAR(255),
            [Processed By] VARCHAR(255),
            Notes VARCHAR(1999),
            [Approval Date] DATE,
            [Processing Date] DATE,
            [Tax Amount] DECIMAL(38, 2),
            [Social Security] DECIMAL(38, 2),
            [Health Insurance] DECIMAL(38, 2),
            [Net Amount] DECIMAL(38, 2),
            [Effective Hourly Rate] DECIMAL(38, 2)
        );
    END

    -- count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [test_data].[Hr] AS From_Source
    WHERE NOT EXISTS (
        SELECT 1 FROM [prod_data].[Hr] AS To_Target 
        WHERE To_Target.HR_ID = From_Source.HR_ID
    )
        AND From_Source.Processing_Date >= (SELECT ISNULL(MAX([Processing Date]), '1900-01-01') FROM [prod_data].[Hr]);

    -- count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [prod_data].[Hr] AS To_Target
    INNER JOIN [test_data].[Hr] AS From_Source
        ON To_Target.HR_ID = From_Source.HR_ID
    WHERE (
        To_Target.[Processing Date] <> From_Source.Processing_Date
        OR To_Target.[Net Amount] <> From_Source.Net_Amount
        OR To_Target.[Payment Status] <> From_Source.Payment_Status
        OR To_Target.[Hours Worked] <> From_Source.Hours_Worked
    )
        AND From_Source.Processing_Date >= (SELECT ISNULL(MAX([Processing Date]), '1900-01-01') FROM [prod_data].[Hr]);

    -- incremental Load from test_data to prod_data
    MERGE INTO [prod_data].[Hr] AS To_Target 
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
                
            FROM [test_data].[Hr]
            WHERE Processing_Date >= (SELECT ISNULL(MAX([Processing Date]), '1900-01-01') FROM [prod_data].[Hr])
        ) AS From_Source
            ON (To_Target.HR_ID = From_Source.HR_ID)

        -- row exists, but data change (UPDATE)
        WHEN MATCHED AND (
            To_Target.[Processing Date] <> From_Source.Processing_Date
            OR To_Target.[Net Amount] <> From_Source.Net_Amount
            OR To_Target.[Payment Status] <> From_Source.Payment_Status
            OR To_Target.[Hours Worked] <> From_Source.Hours_Worked
        )
        THEN UPDATE SET
            To_Target.Date_ID = From_Source.Date_ID,
            To_Target.[Transaction Date] = From_Source.Transaction_Date,
            To_Target.Account_ID = From_Source.Account_ID,
            To_Target.Business_Line_ID = From_Source.Business_Line_ID,
            To_Target.Employee_ID = From_Source.Employee_ID,
            To_Target.[Transaction Type] = From_Source.Transaction_Type,
            To_Target.[Payroll Period] = From_Source.Payroll_Period,
            To_Target.[Payment Method] = From_Source.Payment_Method,
            To_Target.[Payment Status] = From_Source.Payment_Status,
            To_Target.Amount = From_Source.Amount,
            To_Target.[Hours Worked] = From_Source.Hours_Worked,
            To_Target.[Overtime Hours] = From_Source.Overtime_Hours,
            To_Target.[Hourly Rate] = From_Source.Hourly_Rate,
            To_Target.[Gross Amount] = From_Source.Gross_Amount,
            To_Target.[Bonus Type] = From_Source.Bonus_Type,
            To_Target.[Training Course] = From_Source.Training_Course,
            To_Target.[Travel Destination] = From_Source.Travel_Destination,
            To_Target.[Cost Center] = From_Source.Cost_Center,
            To_Target.[Project Code] = From_Source.Project_Code,
            To_Target.[Approved By] = From_Source.Approved_By,
            To_Target.[Processed By] = From_Source.Processed_By,
            To_Target.Notes = From_Source.Notes,
            To_Target.[Approval Date] = From_Source.Approval_Date,
            To_Target.[Processing Date] = From_Source.Processing_Date,
            To_Target.[Tax Amount] = From_Source.Tax_Amount,
            To_Target.[Social Security] = From_Source.Social_Security,
            To_Target.[Health Insurance] = From_Source.Health_Insurance,
            To_Target.[Net Amount] = From_Source.Net_Amount,
            To_Target.[Effective Hourly Rate] = From_Source.Effective_Hourly_Rate

        -- row does not exist (INSERT)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                HR_ID,
                Date_ID,
                [Transaction Date],
                Employee_ID,
                Business_Line_ID,
                Account_ID,
                [Transaction Type],
                [Payroll Period],
                [Payment Method],
                [Payment Status],
                Amount,
                [Hours Worked],
                [Overtime Hours],
                [Hourly Rate],
                [Gross Amount],
                [Bonus Type],
                [Training Course],
                [Travel Destination],
                [Cost Center],
                [Project Code],
                [Approved By],
                [Processed By],
                Notes,
                [Approval Date],
                [Processing Date],
                [Tax Amount],
                [Social Security],
                [Health Insurance],
                [Net Amount],
                [Effective Hourly Rate]
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
			'prod_data',
			'pr_prod_fact_hr',
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