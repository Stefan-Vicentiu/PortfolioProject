
-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dev_data')
BEGIN
    EXEC('CREATE SCHEMA [dev_data]');
END
GO


CREATE OR ALTER PROCEDURE [dev_data].[pr_dev_fact_sales]
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
        WHERE t.name = 'Sales' 
            AND s.name = 'dev_data'
    )
    BEGIN
        -- create Sales fact
        CREATE TABLE [dev_data].[Sales] (
            Sales_ID INT NOT NULL,
            Sales_Line_Number INT,
            Date_ID INT,
            Transaction_DateTime DATETIME2(6),
            Document_ID VARCHAR(255),
            Product_ID VARCHAR(255),
            Customer_ID VARCHAR(255),
            Employee_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Account_ID VARCHAR(255),
            Order_Number VARCHAR(255),
            Invoice_Number VARCHAR(255),
            Sales_Channel VARCHAR(100),
            Payment_Method VARCHAR(100),
            Payment_Status VARCHAR(100),
            Delivery_Method VARCHAR(100),
            Quantity DECIMAL(38, 2),
            Unit_Price DECIMAL(38, 2),
            Unit_Cost DECIMAL(38, 2),
            Discount_Pct DECIMAL(38, 2),
            Tax_Pct DECIMAL(38, 2),
            Promotion_Code VARCHAR(100),
            Customer_Segment_Override VARCHAR(100),
            Sales_Region VARCHAR(100),
            Warehouse_Location VARCHAR(255),
            Created_By VARCHAR(255),
            Modified_By VARCHAR(255),
            Is_Return INT,
            Return_Reason VARCHAR(255),
            Notes VARCHAR(1999),
            Gross_Amount DECIMAL(38, 2),
            Discount_Amount DECIMAL(38, 2),
            Net_Amount DECIMAL(38, 2),
            Tax_Amount DECIMAL(38, 2),
            Total_Amount DECIMAL(38, 2),
            Cost_Amount DECIMAL(38, 2),
            Profit_Amount DECIMAL(38, 2),
            Profit_Margin_Pct DECIMAL(38, 2),
            Created_Date DATETIME2(6),
            Modified_Date DATETIME2(6)
        );
    END

    DECLARE @LastTransactionDate DATETIME2(6) = (SELECT ISNULL(MAX(Transaction_DateTime), '1900-01-01 00:00:00.000000') FROM [dev_data].[Sales]);

    -- count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [Lakehouse_RawData].[dbo].[fact_sales] AS From_Source
    WHERE 
        NOT EXISTS (
            SELECT 1 FROM [dev_data].[Sales] AS To_Target 
            WHERE To_Target.Sales_ID = From_Source.Sales_ID
        )
        AND From_Source.Transaction_DateTime >= @LastTransactionDate

    -- count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [dev_data].[Sales] AS To_Target
        INNER JOIN [Lakehouse_RawData].[dbo].[fact_sales] AS From_Source 
            ON To_Target.Sales_ID = From_Source.Sales_ID
    WHERE From_Source.Transaction_DateTime >= @LastTransactionDate
      AND (To_Target.Total_Amount <> From_Source.Total_Amount 
        OR To_Target.Payment_Status <> From_Source.Payment_Status);

    -- incremental Load
    MERGE INTO [dev_data].[Sales] AS To_Target 
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
                CAST(Quantity AS DECIMAL(38, 2)) AS Quantity, 
                CAST(Unit_Price AS DECIMAL(38, 2)) AS Unit_Price, 
                CAST(Unit_Cost AS DECIMAL(38, 2)) AS Unit_Cost, 
                CAST(Discount_Pct AS DECIMAL(38, 2)) AS Discount_Pct, 
                CAST(Tax_Pct AS DECIMAL(38, 2)) AS Tax_Pct, 
                Promotion_Code, 
                Customer_Segment_Override, 
                Sales_Region, 
                Warehouse_Location, 
                Created_By, 
                Modified_By, 
                Is_Return, 
                Return_Reason, 
                Notes, 
                CAST(Gross_Amount AS DECIMAL(38, 2)) AS Gross_Amount, 
                CAST(Discount_Amount AS DECIMAL(38, 2)) AS Discount_Amount, 
                CAST(Net_Amount AS DECIMAL(38, 2)) AS Net_Amount, 
                CAST(Tax_Amount AS DECIMAL(38, 2)) AS Tax_Amount, 
                CAST(Total_Amount AS DECIMAL(38, 2)) AS Total_Amount, 
                CAST(Cost_Amount AS DECIMAL(38, 2)) AS Cost_Amount, 
                CAST(Profit_Amount AS DECIMAL(38, 2)) AS Profit_Amount, 
                CAST(Profit_Margin_Pct AS DECIMAL(38, 2)) AS Profit_Margin_Pct, 
                Created_Date, 
                Modified_Date

            FROM [Lakehouse_RawData].[dbo].[fact_sales]
            WHERE Transaction_DateTime >= @LastTransactionDate
        ) AS From_Source
            ON (To_Target.Sales_ID = From_Source.Sales_ID)

    WHEN MATCHED AND (
        To_Target.Transaction_DateTime <> From_Source.Transaction_DateTime
        OR To_Target.Total_Amount <> From_Source.Total_Amount
        OR To_Target.Payment_Status <> From_Source.Payment_Status
    )
    THEN UPDATE SET 
        To_Target.Sales_ID = From_Source.Sales_ID,
        To_Target.Sales_Line_Number = From_Source.Sales_Line_Number,
        To_Target.Date_ID = From_Source.Date_ID,
        To_Target.Transaction_DateTime = From_Source.Transaction_DateTime,
        To_Target.Document_ID = From_Source.Document_ID,
        To_Target.Product_ID = From_Source.Product_ID,
        To_Target.Customer_ID = From_Source.Customer_ID,
        To_Target.Employee_ID = From_Source.Employee_ID,
        To_Target.Business_Line_ID = From_Source.Business_Line_ID,
        To_Target.Account_ID = From_Source.Account_ID,
        To_Target.Order_Number = From_Source.Order_Number,
        To_Target.Invoice_Number = From_Source.Invoice_Number,
        To_Target.Sales_Channel = From_Source.Sales_Channel,
        To_Target.Payment_Method = From_Source.Payment_Method,
        To_Target.Payment_Status = From_Source.Payment_Status,
        To_Target.Delivery_Method = From_Source.Delivery_Method,
        To_Target.Quantity = From_Source.Quantity,
        To_Target.Unit_Price = From_Source.Unit_Price,
        To_Target.Unit_Cost = From_Source.Unit_Cost,
        To_Target.Discount_Pct = From_Source.Discount_Pct,
        To_Target.Tax_Pct = From_Source.Tax_Pct,
        To_Target.Promotion_Code = From_Source.Promotion_Code,
        To_Target.Customer_Segment_Override = From_Source.Customer_Segment_Override,
        To_Target.Sales_Region = From_Source.Sales_Region,
        To_Target.Warehouse_Location = From_Source.Warehouse_Location,
        To_Target.Created_By = From_Source.Created_By,
        To_Target.Modified_By = From_Source.Modified_By,
        To_Target.Is_Return = From_Source.Is_Return,
        To_Target.Return_Reason = From_Source.Return_Reason,
        To_Target.Notes = From_Source.Notes,
        To_Target.Gross_Amount = From_Source.Gross_Amount,
        To_Target.Discount_Amount = From_Source.Discount_Amount,
        To_Target.Net_Amount = From_Source.Net_Amount,
        To_Target.Tax_Amount = From_Source.Tax_Amount,
        To_Target.Total_Amount = From_Source.Total_Amount,
        To_Target.Cost_Amount = From_Source.Cost_Amount,
        To_Target.Profit_Amount = From_Source.Profit_Amount,
        To_Target.Profit_Margin_Pct = From_Source.Profit_Margin_Pct,
        To_Target.Created_Date = From_Source.Created_Date,
        To_Target.Modified_Date = From_Source.Modified_Date

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
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
            Modified_Date
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
            From_Source.Modified_Date
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
			'pr_dev_fact_sales',
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