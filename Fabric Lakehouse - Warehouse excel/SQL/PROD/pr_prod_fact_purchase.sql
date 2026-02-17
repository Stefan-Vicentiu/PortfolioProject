-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prod_data')
BEGIN
    EXEC('CREATE SCHEMA [prod_data]');
END
GO

CREATE OR ALTER PROCEDURE [prod_data].[pr_prod_fact_purchase]
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
        WHERE t.name = 'Purchases' 
            AND s.name = 'prod_data'
    )
    BEGIN
        -- Create Purchase fact in prod_data with Power BI friendly names
        CREATE TABLE [prod_data].[Purchases] (
            Purchase_ID INT NOT NULL,
            [Purchase Line Number] INT,
            Date_ID INT,
            [Transaction Date] DATETIME2(6),
            Document_ID VARCHAR(255),
            Product_ID VARCHAR(255),
            Supplier_ID VARCHAR(255),
            Employee_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Account_ID VARCHAR(255),
            [PO Number] VARCHAR(255),
            [Supplier Invoice Number] VARCHAR(255),
            [Purchase Type] VARCHAR(100),
            [Payment Terms] VARCHAR(100),
            [Payment Status] VARCHAR(100),
            [Delivery Status] VARCHAR(100),
            [Quantity Ordered] DECIMAL(38, 2),
            [Quantity Received] DECIMAL(38, 2),
            [Unit Cost] DECIMAL(38, 2),
            [Freight Cost] DECIMAL(38, 2),
            [Tax Pct] DECIMAL(38, 2),
            [Warehouse Destination] VARCHAR(255),
            Incoterms VARCHAR(100),
            Currency VARCHAR(10),
            [Exchange Rate] DECIMAL(38, 4),
            [Quality Check Status] VARCHAR(100),
            [Defect Rate Pct] DECIMAL(38, 2),
            [Approved By] VARCHAR(255),
            [Received By] VARCHAR(255),
            Notes VARCHAR(1999),
            [Expected Delivery Date] DATE,
            [Actual Delivery Date] DATE,
            [Net Amount] DECIMAL(38, 2),
            [Tax Amount] DECIMAL(38, 2),
            [Total Amount] DECIMAL(38, 2),
            [Total Amount RON] DECIMAL(38, 2),
            [Quantity Variance] DECIMAL(38, 2),
            [Delivery Delay Days] INT,
            [Approval Date] DATETIME2(6)
        );
    END

    DECLARE @LastTransactionDate DATETIME2(6) = (SELECT ISNULL(MAX([Transaction Date]), '1900-01-01 00:00:00.000000') FROM [prod_data].[Purchases]);

    -- Count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [test_data].[Purchases] AS From_Source
    WHERE NOT EXISTS (
        SELECT 1 FROM [prod_data].[Purchases] AS To_Target 
        WHERE To_Target.Purchase_ID = From_Source.Purchase_ID
    )
        AND From_Source.Transaction_Date >= @LastTransactionDate;

    -- Count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [prod_data].[Purchases] AS To_Target
    INNER JOIN [test_data].[Purchases] AS From_Source
        ON To_Target.Purchase_ID = From_Source.Purchase_ID
    WHERE (
        To_Target.[Transaction Date] <> From_Source.Transaction_Date OR
        To_Target.[Total Amount] <> From_Source.Total_Amount OR
        To_Target.[Delivery Status] <> From_Source.Delivery_Status
    )
        AND From_Source.Transaction_Date >= @LastTransactionDate;

    -- Incremental Load using MERGE from test_data to prod_data
    MERGE INTO [prod_data].[Purchases] AS To_Target 
        USING (
            SELECT
                Purchase_ID,
                Purchase_Line_Number,
                Date_ID,
                Transaction_Date,
                Document_ID,
                Product_ID,
                Supplier_ID,
                Employee_ID,
                Business_Line_ID,
                Account_ID,
                PO_Number,
                Supplier_Invoice_Number,
                Purchase_Type,
                Payment_Terms,
                Payment_Status,
                Delivery_Status,
                Quantity_Ordered,
                Quantity_Received,
                Unit_Cost,
                Freight_Cost,
                Tax_Pct,
                Warehouse_Destination,
                Incoterms,
                Currency,
                Exchange_Rate,
                Quality_Check_Status,
                Defect_Rate_Pct,
                Approved_By,
                Received_By,
                Notes,
                Expected_Delivery_Date,
                Actual_Delivery_Date,
                Net_Amount,
                Tax_Amount,
                Total_Amount,
                Total_Amount_RON,
                Quantity_Variance,
                Delivery_Delay_Days,
                Approval_Date
            FROM [test_data].[Purchases]
            WHERE Transaction_Date >= @LastTransactionDate
        ) AS From_Source
            ON (To_Target.Purchase_ID = From_Source.Purchase_ID)

    WHEN MATCHED AND (
        To_Target.[Transaction Date] <> From_Source.Transaction_Date OR
        To_Target.[Total Amount] <> From_Source.Total_Amount OR
        To_Target.[Delivery Status] <> From_Source.Delivery_Status
    )
    THEN UPDATE SET 
        To_Target.[Purchase Line Number] = From_Source.Purchase_Line_Number,
        To_Target.Date_ID = From_Source.Date_ID,
        To_Target.[Transaction Date] = From_Source.Transaction_Date,
        To_Target.Document_ID = From_Source.Document_ID,
        To_Target.Product_ID = From_Source.Product_ID,
        To_Target.Supplier_ID = From_Source.Supplier_ID,
        To_Target.Employee_ID = From_Source.Employee_ID,
        To_Target.Business_Line_ID = From_Source.Business_Line_ID,
        To_Target.Account_ID = From_Source.Account_ID,
        To_Target.[PO Number] = From_Source.PO_Number,
        To_Target.[Supplier Invoice Number] = From_Source.Supplier_Invoice_Number,
        To_Target.[Purchase Type] = From_Source.Purchase_Type,
        To_Target.[Payment Terms] = From_Source.Payment_Terms,
        To_Target.[Payment Status] = From_Source.Payment_Status,
        To_Target.[Delivery Status] = From_Source.Delivery_Status,
        To_Target.[Quantity Ordered] = From_Source.Quantity_Ordered,
        To_Target.[Quantity Received] = From_Source.Quantity_Received,
        To_Target.[Unit Cost] = From_Source.Unit_Cost,
        To_Target.[Freight Cost] = From_Source.Freight_Cost,
        To_Target.[Tax Pct] = From_Source.Tax_Pct,
        To_Target.[Warehouse Destination] = From_Source.Warehouse_Destination,
        To_Target.Incoterms = From_Source.Incoterms,
        To_Target.Currency = From_Source.Currency,
        To_Target.[Exchange Rate] = From_Source.Exchange_Rate,
        To_Target.[Quality Check Status] = From_Source.Quality_Check_Status,
        To_Target.[Defect Rate Pct] = From_Source.Defect_Rate_Pct,
        To_Target.[Approved By] = From_Source.Approved_By,
        To_Target.[Received By] = From_Source.Received_By,
        To_Target.Notes = From_Source.Notes,
        To_Target.[Expected Delivery Date] = From_Source.Expected_Delivery_Date,
        To_Target.[Actual Delivery Date] = From_Source.Actual_Delivery_Date,
        To_Target.[Net Amount] = From_Source.Net_Amount,
        To_Target.[Tax Amount] = From_Source.Tax_Amount,
        To_Target.[Total Amount] = From_Source.Total_Amount,
        To_Target.[Total Amount RON] = From_Source.Total_Amount_RON,
        To_Target.[Quantity Variance] = From_Source.Quantity_Variance,
        To_Target.[Delivery Delay Days] = From_Source.Delivery_Delay_Days,
        To_Target.[Approval Date] = From_Source.Approval_Date

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            Purchase_ID, 
            [Purchase Line Number], 
            Date_ID, 
            [Transaction Date], 
            Document_ID, 
            Product_ID, 
            Supplier_ID, 
            Employee_ID, 
            Business_Line_ID, 
            Account_ID, 
            [PO Number], 
            [Supplier Invoice Number], 
            [Purchase Type], 
            [Payment Terms], 
            [Payment Status], 
            [Delivery Status], 
            [Quantity Ordered], 
            [Quantity Received], 
            [Unit Cost], 
            [Freight Cost], 
            [Tax Pct], 
            [Warehouse Destination], 
            Incoterms, 
            Currency, 
            [Exchange Rate], 
            [Quality Check Status], 
            [Defect Rate Pct], 
            [Approved By], 
            [Received By], 
            Notes, 
            [Expected Delivery Date], 
            [Actual Delivery Date], 
            [Net Amount], 
            [Tax Amount], 
            [Total Amount], 
            [Total Amount RON], 
            [Quantity Variance], 
            [Delivery Delay Days],
            [Approval Date]
        )
        VALUES (
            From_Source.Purchase_ID, 
            From_Source.Purchase_Line_Number, 
            From_Source.Date_ID, 
            From_Source.Transaction_Date, 
            From_Source.Document_ID, 
            From_Source.Product_ID, 
            From_Source.Supplier_ID, 
            From_Source.Employee_ID, 
            From_Source.Business_Line_ID, 
            From_Source.Account_ID, 
            From_Source.PO_Number, 
            From_Source.Supplier_Invoice_Number, 
            From_Source.Purchase_Type, 
            From_Source.Payment_Terms, 
            From_Source.Payment_Status, 
            From_Source.Delivery_Status, 
            From_Source.Quantity_Ordered, 
            From_Source.Quantity_Received, 
            From_Source.Unit_Cost, 
            From_Source.Freight_Cost, 
            From_Source.Tax_Pct, 
            From_Source.Warehouse_Destination, 
            From_Source.Incoterms, 
            From_Source.Currency, 
            From_Source.Exchange_Rate, 
            From_Source.Quality_Check_Status, 
            From_Source.Defect_Rate_Pct, 
            From_Source.Approved_By, 
            From_Source.Received_By, 
            From_Source.Notes, 
            From_Source.Expected_Delivery_Date, 
            From_Source.Actual_Delivery_Date, 
            From_Source.Net_Amount, 
            From_Source.Tax_Amount, 
            From_Source.Total_Amount, 
            From_Source.Total_Amount_RON, 
            From_Source.Quantity_Variance, 
            From_Source.Delivery_Delay_Days,
            From_Source.Approval_Date
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
			'pr_prod_fact_purchase',
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