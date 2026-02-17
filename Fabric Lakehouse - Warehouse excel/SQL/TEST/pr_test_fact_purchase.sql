-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test_data')
BEGIN
    EXEC('CREATE SCHEMA [test_data]');
END
GO

CREATE OR ALTER PROCEDURE [test_data].[pr_test_fact_purchase]
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
        WHERE t.name = 'Purchases' 
            AND s.name = 'test_data'
    )
    BEGIN
        -- Create Purchase fact
        CREATE TABLE [test_data].[Purchases] (
            Purchase_ID INT NOT NULL,
            Purchase_Line_Number INT,
            Date_ID INT,
            Transaction_Date DATETIME2(6),
            Document_ID VARCHAR(255),
            Product_ID VARCHAR(255),
            Supplier_ID VARCHAR(255),
            Employee_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Account_ID VARCHAR(255),
            PO_Number VARCHAR(255),
            Supplier_Invoice_Number VARCHAR(255),
            Purchase_Type VARCHAR(100),
            Payment_Terms VARCHAR(100),
            Payment_Status VARCHAR(100),
            Delivery_Status VARCHAR(100),
            Quantity_Ordered DECIMAL(38, 2),
            Quantity_Received DECIMAL(38, 2),
            Unit_Cost DECIMAL(38, 2),
            Freight_Cost DECIMAL(38, 2),
            Tax_Pct DECIMAL(38, 2),
            Warehouse_Destination VARCHAR(255),
            Incoterms VARCHAR(100),
            Currency VARCHAR(10),
            Exchange_Rate DECIMAL(38, 4),
            Quality_Check_Status VARCHAR(100),
            Defect_Rate_Pct DECIMAL(38, 2),
            Approved_By VARCHAR(255),
            Received_By VARCHAR(255),
            Notes VARCHAR(1999),
            Expected_Delivery_Date DATE,
            Actual_Delivery_Date DATE,
            Net_Amount DECIMAL(38, 2),
            Tax_Amount DECIMAL(38, 2),
            Total_Amount DECIMAL(38, 2),
            Total_Amount_RON DECIMAL(38, 2),
            Quantity_Variance DECIMAL(38, 2),
            Delivery_Delay_Days INT,
            
            -- new calculated column from DAX logic
            Approval_Date DATETIME2(6)
        );
    END

    DECLARE @LastTransactionDate DATETIME2(6) = (SELECT ISNULL(MAX(Transaction_Date), '1900-01-01 00:00:00.000000') FROM [test_data].[Purchases]);

    -- Count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [Lakehouse_RawData].[dbo].[fact_purchases] AS From_Source
    WHERE 
        NOT EXISTS (
            SELECT 1 FROM [test_data].[Purchases] AS To_Target 
            WHERE To_Target.Purchase_ID = From_Source.Purchase_ID
        )
        AND From_Source.Transaction_Date >= @LastTransactionDate

    -- Count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [test_data].[Purchases] AS To_Target
        INNER JOIN [Lakehouse_RawData].[dbo].[fact_purchases] AS From_Source 
            ON To_Target.Purchase_ID = From_Source.Purchase_ID
    WHERE From_Source.Transaction_Date >= @LastTransactionDate
      AND (To_Target.Total_Amount <> From_Source.Total_Amount 
        OR To_Target.Delivery_Status <> From_Source.Delivery_Status);

    -- Incremental Load
    MERGE INTO [test_data].[Purchases] AS To_Target 
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
                CAST(Quantity_Ordered AS DECIMAL(38, 2)) AS Quantity_Ordered,
                CAST(Quantity_Received AS DECIMAL(38, 2)) AS Quantity_Received,
                CAST(Unit_Cost AS DECIMAL(38, 2)) AS Unit_Cost,
                CAST(Freight_Cost AS DECIMAL(38, 2)) AS Freight_Cost,
                CAST(Tax_Pct AS DECIMAL(38, 2)) AS Tax_Pct,
                Warehouse_Destination,
                Incoterms,
                Currency,
                CAST(Exchange_Rate AS DECIMAL(38, 4)) AS Exchange_Rate,
                Quality_Check_Status,
                CAST(Defect_Rate_Pct AS DECIMAL(38, 2)) AS Defect_Rate_Pct,
                Approved_By,
                Received_By,
                Notes,
                Expected_Delivery_Date,
                Actual_Delivery_Date,
                CAST(Net_Amount AS DECIMAL(38, 2)) AS Net_Amount,
                CAST(Tax_Amount AS DECIMAL(38, 2)) AS Tax_Amount,
                CAST(Total_Amount AS DECIMAL(38, 2)) AS Total_Amount,
                CAST(Total_Amount_RON AS DECIMAL(38, 2)) AS Total_Amount_RON,
                CAST(Quantity_Variance AS DECIMAL(38, 2)) AS Quantity_Variance,
                Delivery_Delay_Days,
                
                -- Calculation: [Transaction Date] + 2
                DATEADD(DAY, 2, Transaction_Date) AS Calc_Approval_Date

            FROM [Lakehouse_RawData].[dbo].[fact_purchases]
            WHERE Transaction_Date >= @LastTransactionDate
        ) AS From_Source
            ON (To_Target.Purchase_ID = From_Source.Purchase_ID)

    WHEN MATCHED AND (
        To_Target.Transaction_Date <> From_Source.Transaction_Date OR
        To_Target.Total_Amount <> From_Source.Total_Amount OR
        To_Target.Delivery_Status <> From_Source.Delivery_Status
    )
    THEN UPDATE SET 
        To_Target.Purchase_ID = From_Source.Purchase_ID,
        To_Target.Purchase_Line_Number = From_Source.Purchase_Line_Number,
        To_Target.Date_ID = From_Source.Date_ID,
        To_Target.Transaction_Date = From_Source.Transaction_Date,
        To_Target.Document_ID = From_Source.Document_ID,
        To_Target.Product_ID = From_Source.Product_ID,
        To_Target.Supplier_ID = From_Source.Supplier_ID,
        To_Target.Employee_ID = From_Source.Employee_ID,
        To_Target.Business_Line_ID = From_Source.Business_Line_ID,
        To_Target.Account_ID = From_Source.Account_ID,
        To_Target.PO_Number = From_Source.PO_Number,
        To_Target.Supplier_Invoice_Number = From_Source.Supplier_Invoice_Number,
        To_Target.Purchase_Type = From_Source.Purchase_Type,
        To_Target.Payment_Terms = From_Source.Payment_Terms,
        To_Target.Payment_Status = From_Source.Payment_Status,
        To_Target.Delivery_Status = From_Source.Delivery_Status,
        To_Target.Quantity_Ordered = From_Source.Quantity_Ordered,
        To_Target.Quantity_Received = From_Source.Quantity_Received,
        To_Target.Unit_Cost = From_Source.Unit_Cost,
        To_Target.Freight_Cost = From_Source.Freight_Cost,
        To_Target.Tax_Pct = From_Source.Tax_Pct,
        To_Target.Warehouse_Destination = From_Source.Warehouse_Destination,
        To_Target.Incoterms = From_Source.Incoterms,
        To_Target.Currency = From_Source.Currency,
        To_Target.Exchange_Rate = From_Source.Exchange_Rate,
        To_Target.Quality_Check_Status = From_Source.Quality_Check_Status,
        To_Target.Defect_Rate_Pct = From_Source.Defect_Rate_Pct,
        To_Target.Approved_By = From_Source.Approved_By,
        To_Target.Received_By = From_Source.Received_By,
        To_Target.Notes = From_Source.Notes,
        To_Target.Expected_Delivery_Date = From_Source.Expected_Delivery_Date,
        To_Target.Actual_Delivery_Date = From_Source.Actual_Delivery_Date,
        To_Target.Net_Amount = From_Source.Net_Amount,
        To_Target.Tax_Amount = From_Source.Tax_Amount,
        To_Target.Total_Amount = From_Source.Total_Amount,
        To_Target.Total_Amount_RON = From_Source.Total_Amount_RON,
        To_Target.Quantity_Variance = From_Source.Quantity_Variance,
        To_Target.Delivery_Delay_Days = From_Source.Delivery_Delay_Days,
        To_Target.Approval_Date = From_Source.Calc_Approval_Date

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
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
            From_Source.Calc_Approval_Date
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
			'pr_test_fact_purchase',
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