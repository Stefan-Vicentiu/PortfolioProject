-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test_data')
BEGIN
    EXEC('CREATE SCHEMA [test_data]');
END
GO


CREATE OR ALTER PROCEDURE [test_data].[pr_test_fact_inventory]
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

    -- Check if the table exists in the test_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Inventory' 
            AND s.name = 'test_data'
    )
    BEGIN
        -- Create Inventory fact
        CREATE TABLE [test_data].[Inventory] (
            Inventory_ID INT NOT NULL,
            Date_ID INT,
            Transaction_DateTime DATETIME2(6),
            Document_ID VARCHAR(255),
            Product_ID VARCHAR(255),
            Supplier_ID VARCHAR(255),
            Customer_ID VARCHAR(255),
            Employee_ID VARCHAR(255),
            Business_Line_ID VARCHAR(255),
            Movement_Type VARCHAR(255),
            Transaction_Number VARCHAR(255),
                        Reference_Document VARCHAR(255),
            Warehouse_From VARCHAR(255),
            Warehouse_To VARCHAR(255),
            Storage_Location VARCHAR(255),
            Quantity DECIMAL(38, 2),
            Unit_Cost DECIMAL(38, 2),
            Quantity_Before DECIMAL(38, 2),
            Batch_Number VARCHAR(255),
            Serial_Number VARCHAR(255),
            Quality_Status VARCHAR(100),
            Reason_Code VARCHAR(100),
            Shipping_Method VARCHAR(100),
            Tracking_Number VARCHAR(255),
            Freight_Cost DECIMAL(38, 2),
            Processed_By VARCHAR(255),
            Approved_By VARCHAR(255),
            Is_Verified INT,
            Notes VARCHAR(1999),
            Expiry_Date DATE,
            Manufacturing_Date DATE,
            Verification_Date DATE,
            Quantity_After DECIMAL(38, 2),
            Total_Value DECIMAL(38, 2),
            Stock_Value_Before DECIMAL(38, 2),
            Stock_Value_After DECIMAL(38, 2),
            Days_To_Expiry INT,
            Product_Age_Days INT
        );
    END

    DECLARE @TransactionDateTime DATETIME2(6) = (SELECT ISNULL(MAX(Transaction_DateTime), '1900-01-01 00:00:00.000000') FROM [test_data].[Inventory]);

    -- Count new rows
    SELECT 
        @RowsInserted = COUNT(*)
    FROM [Lakehouse_RawData].[dbo].[fact_inventory] AS From_Source
    WHERE 
        NOT EXISTS (
            SELECT 1 FROM [test_data].[Inventory] AS To_Target 
            WHERE To_Target.Inventory_ID = From_Source.Inventory_ID
        )
        AND From_Source.Transaction_DateTime >= @TransactionDateTime

    -- Count modified rows
    SELECT 
        @RowsUpdated = COUNT(*)
    FROM [test_data].[Inventory] AS To_Target
    INNER JOIN [Lakehouse_RawData].[dbo].[fact_inventory] AS From_Source
        ON To_Target.Inventory_ID = From_Source.Inventory_ID
    WHERE (
        To_Target.Transaction_DateTime <> From_Source.Transaction_DateTime
        OR To_Target.Quantity <> From_Source.Quantity
        OR To_Target.Total_Value <> From_Source.Total_Value
        OR To_Target.Is_Verified <> From_Source.Is_Verified
    )
        AND From_Source.Transaction_DateTime >= @TransactionDateTime

    -- Incremental Load
    MERGE INTO [test_data].[Inventory] AS To_Target 
        USING (
            SELECT
                Inventory_ID,
                Date_ID,
                Transaction_DateTime,
                Document_ID,
                Product_ID,
                Supplier_ID,
                Customer_ID,
                Employee_ID,
                Business_Line_ID,
                Movement_Type,
                Transaction_Number,
                Reference_Document,
                Warehouse_From,
                Warehouse_To,
                Storage_Location,
                CAST(Quantity AS DECIMAL(38, 2)) AS Quantity,
                CAST(Unit_Cost AS DECIMAL(38, 2)) AS Unit_Cost,
                CAST(Quantity_Before AS DECIMAL(38, 2)) AS Quantity_Before,
                Batch_Number,
                Serial_Number,
                Quality_Status,
                Reason_Code,
                Shipping_Method,
                Tracking_Number,
                CAST(Freight_Cost AS DECIMAL(38, 2)) AS Freight_Cost,
                Processed_By,
                Approved_By,
                Is_Verified,
                Notes,
                Expiry_Date,
                Manufacturing_Date,
                Verification_Date,
                CAST(Quantity_After AS DECIMAL(38, 2)) AS Quantity_After,
                CAST(Total_Value AS DECIMAL(38, 2)) AS Total_Value,
                CAST(Stock_Value_Before AS DECIMAL(38, 2)) AS Stock_Value_Before,
                CAST(Stock_Value_After AS DECIMAL(38, 2)) AS Stock_Value_After,
                Days_To_Expiry,
                Product_Age_Days

            FROM [Lakehouse_RawData].[dbo].[fact_inventory]
            WHERE Transaction_DateTime >= @TransactionDateTime
        ) AS From_Source
            ON ( To_Target.Inventory_ID = From_Source.Inventory_ID)

        -- Row exists, but data change (UPDATE)
        WHEN MATCHED AND (
            To_Target.Transaction_DateTime <> From_Source.Transaction_DateTime
            OR To_Target.Quantity <> From_Source.Quantity
            OR To_Target.Total_Value <> From_Source.Total_Value
            OR To_Target.Is_Verified <> From_Source.Is_Verified
        )
        THEN UPDATE SET             
            To_Target.Inventory_ID = From_Source.Inventory_ID,
            To_Target.Date_ID = From_Source.Date_ID,
            To_Target.Transaction_DateTime = From_Source.Transaction_DateTime,
            To_Target.Document_ID = From_Source.Document_ID,
            To_Target.Product_ID = From_Source.Product_ID,
            To_Target.Supplier_ID = From_Source.Supplier_ID,
            To_Target.Customer_ID = From_Source.Customer_ID,
            To_Target.Employee_ID = From_Source.Employee_ID,
            To_Target.Business_Line_ID = From_Source.Business_Line_ID,
            To_Target.Movement_Type = From_Source.Movement_Type,
            To_Target.Transaction_Number = From_Source.Transaction_Number,
            To_Target.Reference_Document = From_Source.Reference_Document,
            To_Target.Warehouse_From = From_Source.Warehouse_From,
            To_Target.Warehouse_To = From_Source.Warehouse_To,
            To_Target.Storage_Location = From_Source.Storage_Location,
            To_Target.Quantity = From_Source.Quantity,
            To_Target.Unit_Cost = From_Source.Unit_Cost,
            To_Target.Quantity_Before = From_Source.Quantity_Before,
            To_Target.Batch_Number = From_Source.Batch_Number,
            To_Target.Serial_Number = From_Source.Serial_Number,
            To_Target.Quality_Status = From_Source.Quality_Status,
            To_Target.Reason_Code = From_Source.Reason_Code,
            To_Target.Shipping_Method = From_Source.Shipping_Method,
            To_Target.Tracking_Number = From_Source.Tracking_Number,
            To_Target.Freight_Cost = From_Source.Freight_Cost,
            To_Target.Processed_By = From_Source.Processed_By,
            To_Target.Approved_By = From_Source.Approved_By,
            To_Target.Is_Verified = From_Source.Is_Verified,
            To_Target.Notes = From_Source.Notes,
            To_Target.Expiry_Date = From_Source.Expiry_Date,
            To_Target.Manufacturing_Date = From_Source.Manufacturing_Date,
            To_Target.Verification_Date = From_Source.Verification_Date,
            To_Target.Quantity_After = From_Source.Quantity_After,
            To_Target.Total_Value = From_Source.Total_Value,
            To_Target.Stock_Value_Before = From_Source.Stock_Value_Before,
            To_Target.Stock_Value_After = From_Source.Stock_Value_After,
            To_Target.Days_To_Expiry = From_Source.Days_To_Expiry,
            To_Target.Product_Age_Days = From_Source.Product_Age_Days

        -- Row does not exist (INSERT)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT(
                Inventory_ID,
                Date_ID,
                Transaction_DateTime,
                Document_ID,
                Product_ID,
                Supplier_ID,
                Customer_ID,
                Employee_ID,
                Business_Line_ID,
                Movement_Type,
                Transaction_Number,
                Reference_Document,
                Warehouse_From,
                Warehouse_To,
                Storage_Location,
                Quantity,
                Unit_Cost,
                Quantity_Before,
                Batch_Number,
                Serial_Number,
                Quality_Status,
                Reason_Code,
                Shipping_Method,
                Tracking_Number,
                Freight_Cost,
                Processed_By,
                Approved_By,
                Is_Verified,
                Notes,
                Expiry_Date,
                Manufacturing_Date,
                Verification_Date,
                Quantity_After,
                Total_Value,
                Stock_Value_Before,
                Stock_Value_After,
                Days_To_Expiry,
                Product_Age_Days
            )
            VALUES (
                From_Source.Inventory_ID,
                From_Source.Date_ID,
                From_Source.Transaction_DateTime,
                From_Source.Document_ID,
                From_Source.Product_ID,
                From_Source.Supplier_ID,
                From_Source.Customer_ID,
                From_Source.Employee_ID,
                From_Source.Business_Line_ID,
                From_Source.Movement_Type,
                From_Source.Transaction_Number,
                From_Source.Reference_Document,
                From_Source.Warehouse_From,
                From_Source.Warehouse_To,
                From_Source.Storage_Location,
                From_Source.Quantity,
                From_Source.Unit_Cost,
                From_Source.Quantity_Before,
                From_Source.Batch_Number,
                From_Source.Serial_Number,
                From_Source.Quality_Status,
                From_Source.Reason_Code,
                From_Source.Shipping_Method,
                From_Source.Tracking_Number,
                From_Source.Freight_Cost,
                From_Source.Processed_By,
                From_Source.Approved_By,
                From_Source.Is_Verified,
                From_Source.Notes,
                From_Source.Expiry_Date,
                From_Source.Manufacturing_Date,
                From_Source.Verification_Date,
                From_Source.Quantity_After,
                From_Source.Total_Value,
                From_Source.Stock_Value_Before,
                From_Source.Stock_Value_After,
                From_Source.Days_To_Expiry,
                From_Source.Product_Age_Days
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
			'pr_test_fact_inventory',
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