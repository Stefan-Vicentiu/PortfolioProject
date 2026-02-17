-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prod_data')
BEGIN
    EXEC('CREATE SCHEMA [prod_data]');
END
GO

CREATE OR ALTER PROCEDURE [prod_data].[pr_prod_fact_sales]
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

    -- test error
    --DECLARE @TestEroare INT = 1/0;

    -- check if the table exists in the prod_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Sales' 
            AND s.name = 'prod_data'
    )
    BEGIN
        -- create Sales fact in prod_data with Power BI friendly names
        CREATE TABLE [prod_data].[Sales] (
            Sales_ID INT NOT NULL,
            [Sales Line Number] INT,
            Date_ID INT,
            [Transaction DateTime] DATETIME2(6),
            Document_ID VARCHAR(255),
            Product_ID VARCHAR(255),
            Customer_ID VARCHAR(255),
            Employee_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Account_ID VARCHAR(255),
            [Order Number] VARCHAR(255),
            [Invoice Number] VARCHAR(255),
            [Sales Channel] VARCHAR(100),
            [Payment Method] VARCHAR(100),
            [Payment Status] VARCHAR(100),
            [Delivery Method] VARCHAR(100),
            Quantity DECIMAL(38, 2),
            [Unit Price] DECIMAL(38, 2),
            [Unit Cost] DECIMAL(38, 2),
            [Discount Pct] DECIMAL(38, 2),
            [Tax Pct] DECIMAL(38, 2),
            [Promotion Code] VARCHAR(100),
            [Customer Segment Override] VARCHAR(100),
            [Sales Region] VARCHAR(100),
            [Warehouse Location] VARCHAR(255),
            [Created By] VARCHAR(255),
            [Modified By] VARCHAR(255),
            [Is Return] INT,
            [Return Reason] VARCHAR(255),
            Notes VARCHAR(1999),
            [Gross Amount] DECIMAL(38, 2),
            [Discount Amount] DECIMAL(38, 2),
            [Net Amount] DECIMAL(38, 2),
            [Tax Amount] DECIMAL(38, 2),
            [Total Amount] DECIMAL(38, 2),
            [Cost Amount] DECIMAL(38, 2),
            [Profit Amount] DECIMAL(38, 2),
            [Profit Margin Pct] DECIMAL(38, 2),
            [Created Date] DATETIME2(6),
            [Modified Date] DATETIME2(6),
            [Days Outstanding] INT,
            [Delivery Days] INT
        );
    END

    -- get last transaction date for incremental logic
    DECLARE @LastTransactionDate DATETIME2(6) = (SELECT ISNULL(MAX([Transaction DateTime]), '1900-01-01 00:00:00.000000') FROM [prod_data].[Sales]);

    -- Count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [test_data].[Sales] AS From_Source
    WHERE NOT EXISTS (
        SELECT 1 FROM [prod_data].[Sales] AS To_Target 
        WHERE To_Target.Sales_ID = From_Source.Sales_ID
    )
        AND From_Source.Transaction_DateTime >= @LastTransactionDate;

    -- Count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [prod_data].[Sales] AS To_Target
    INNER JOIN [test_data].[Sales] AS From_Source
        ON To_Target.Sales_ID = From_Source.Sales_ID
    WHERE (
        To_Target.[Transaction DateTime] <> From_Source.Transaction_DateTime
        OR To_Target.[Total Amount] <> From_Source.Total_Amount
        OR To_Target.[Payment Status] <> From_Source.Payment_Status
    )
        AND From_Source.Transaction_DateTime >= @LastTransactionDate;

    -- incremental load using MERGE from test_data to prod_data
    MERGE INTO [prod_data].[Sales] AS To_Target 
        USING (
            SELECT
                Sales_ID,
                Sales_Line_Number,
                Date_ID,
                Transaction_DateTime,
                Document_ID,
                Product_ID,
                Customer_ID,
                Employee_ID,
                Business_Line_ID,
                Account_ID,
                Order_Number,
                Invoice_Number,
                Sales_Channel,
                Payment_Method,
                Payment_Status,
                Delivery_Method,
                Quantity,
                Unit_Price,
                Unit_Cost,
                Discount_Pct,
                Tax_Pct,
                Promotion_Code,
                Customer_Segment_Override,
                Sales_Region,
                Warehouse_Location,
                Created_By,
                Modified_By,
                Is_Return,
                Return_Reason,
                Notes,
                Gross_Amount,
                Discount_Amount,
                Net_Amount,
                Tax_Amount,
                Total_Amount,
                Cost_Amount,
                Profit_Amount,
                Profit_Margin_Pct,
                Created_Date,
                Modified_Date,
                Days_Outstanding,
                Delivery_Days
                
            FROM [test_data].[Sales]
            WHERE Transaction_DateTime >= @LastTransactionDate
        ) AS From_Source
            ON (To_Target.Sales_ID = From_Source.Sales_ID)

    WHEN MATCHED AND (
        To_Target.[Transaction DateTime] <> From_Source.Transaction_DateTime
        OR To_Target.[Total Amount] <> From_Source.Total_Amount
        OR To_Target.[Payment Status] <> From_Source.Payment_Status
    )
    THEN UPDATE SET 
        To_Target.[Sales Line Number] = From_Source.Sales_Line_Number,
        To_Target.Date_ID = From_Source.Date_ID,
        To_Target.[Transaction DateTime] = From_Source.Transaction_DateTime,
        To_Target.Document_ID = From_Source.Document_ID,
        To_Target.Product_ID = From_Source.Product_ID,
        To_Target.Customer_ID = From_Source.Customer_ID,
        To_Target.Employee_ID = From_Source.Employee_ID,
        To_Target.Business_Line_ID = From_Source.Business_Line_ID,
        To_Target.Account_ID = From_Source.Account_ID,
        To_Target.[Order Number] = From_Source.Order_Number,
        To_Target.[Invoice Number] = From_Source.Invoice_Number,
        To_Target.[Sales Channel] = From_Source.Sales_Channel,
        To_Target.[Payment Method] = From_Source.Payment_Method,
        To_Target.[Payment Status] = From_Source.Payment_Status,
        To_Target.[Delivery Method] = From_Source.Delivery_Method,
        To_Target.Quantity = From_Source.Quantity,
        To_Target.[Unit Price] = From_Source.Unit_Price,
        To_Target.[Unit Cost] = From_Source.Unit_Cost,
        To_Target.[Discount Pct] = From_Source.Discount_Pct,
        To_Target.[Tax Pct] = From_Source.Tax_Pct,
        To_Target.[Promotion Code] = From_Source.Promotion_Code,
        To_Target.[Customer Segment Override] = From_Source.Customer_Segment_Override,
        To_Target.[Sales Region] = From_Source.Sales_Region,
        To_Target.[Warehouse Location] = From_Source.Warehouse_Location,
        To_Target.[Created By] = From_Source.Created_By,
        To_Target.[Modified By] = From_Source.Modified_By,
        To_Target.[Is Return] = From_Source.Is_Return,
        To_Target.[Return Reason] = From_Source.Return_Reason,
        To_Target.Notes = From_Source.Notes,
        To_Target.[Gross Amount] = From_Source.Gross_Amount,
        To_Target.[Discount Amount] = From_Source.Discount_Amount,
        To_Target.[Net Amount] = From_Source.Net_Amount,
        To_Target.[Tax Amount] = From_Source.Tax_Amount,
        To_Target.[Total Amount] = From_Source.Total_Amount,
        To_Target.[Cost Amount] = From_Source.Cost_Amount,
        To_Target.[Profit Amount] = From_Source.Profit_Amount,
        To_Target.[Profit Margin Pct] = From_Source.Profit_Margin_Pct,
        To_Target.[Created Date] = From_Source.Created_Date,
        To_Target.[Modified Date] = From_Source.Modified_Date,
        To_Target.[Days Outstanding] = From_Source.Days_Outstanding,
        To_Target.[Delivery Days] = From_Source.Delivery_Days

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            Sales_ID, 
            [Sales Line Number], 
            Date_ID, 
            [Transaction DateTime], 
            Document_ID, 
            Product_ID, 
            Customer_ID, 
            Employee_ID, 
            Business_Line_ID, 
            Account_ID, 
            [Order Number], 
            [Invoice Number], 
            [Sales Channel], 
            [Payment Method], 
            [Payment Status], 
            [Delivery Method], 
            Quantity, 
            [Unit Price], 
            [Unit Cost], 
            [Discount Pct], 
            [Tax Pct], 
            [Promotion Code], 
            [Customer Segment Override], 
            [Sales Region], 
            [Warehouse Location], 
            [Created By], 
            [Modified By], 
            [Is Return], 
            [Return Reason], 
            Notes, 
            [Gross Amount], 
            [Discount Amount], 
            [Net Amount], 
            [Tax Amount], 
            [Total Amount], 
            [Cost Amount], 
            [Profit Amount], 
            [Profit Margin Pct], 
            [Created Date], 
            [Modified Date], 
            [Days Outstanding], 
            [Delivery Days]
        )
        VALUES (
            From_Source.Sales_ID, 
            From_Source.Sales_Line_Number, 
            From_Source.Date_ID, 
            From_Source.Transaction_DateTime, 
            From_Source.Document_ID, 
            From_Source.Product_ID, 
            From_Source.Customer_ID, 
            From_Source.Employee_ID, 
            From_Source.Business_Line_ID, 
            From_Source.Account_ID, 
            From_Source.Order_Number, 
            From_Source.Invoice_Number, 
            From_Source.Sales_Channel, 
            From_Source.Payment_Method, 
            From_Source.Payment_Status, 
            From_Source.Delivery_Method, 
            From_Source.Quantity, 
            From_Source.Unit_Price, 
            From_Source.Unit_Cost, 
            From_Source.Discount_Pct, 
            From_Source.Tax_Pct, 
            From_Source.Promotion_Code, 
            From_Source.Customer_Segment_Override, 
            From_Source.Sales_Region, 
            From_Source.Warehouse_Location, 
            From_Source.Created_By, 
            From_Source.Modified_By, 
            From_Source.Is_Return, 
            From_Source.Return_Reason, 
            From_Source.Notes, 
            From_Source.Gross_Amount, 
            From_Source.Discount_Amount, 
            From_Source.Net_Amount, 
            From_Source.Tax_Amount, 
            From_Source.Total_Amount, 
            From_Source.Cost_Amount, 
            From_Source.Profit_Amount, 
            From_Source.Profit_Margin_Pct, 
            From_Source.Created_Date, 
            From_Source.Modified_Date, 
            From_Source.Days_Outstanding, 
            From_Source.Delivery_Days
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
			'pr_prod_fact_sales',
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