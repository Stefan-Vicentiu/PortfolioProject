
-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test_data')
BEGIN
    EXEC('CREATE SCHEMA [test_data]');
END
GO

CREATE OR ALTER PROCEDURE [test_data].[pr_test_dim_products]
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
    -- check if the table exists in the test_data schema
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Products' 
            AND s.name = 'test_data'
    )
    BEGIN
        -- create Products dimension with Launch_Date
        CREATE TABLE [test_data].[Products] (
            Product_ID VARCHAR(255) NOT NULL,
            Product_Name VARCHAR(255),
            SKU VARCHAR(100),
            Category VARCHAR(100),
            Brand VARCHAR(100),
            Unit_Cost DECIMAL(18,2),
            Unit_Price DECIMAL(18,2),
            Weight_KG DECIMAL(18,2),
            Is_Active BIT,
            Markup_Pct DECIMAL(18,2),
            Launch_Date DATETIME2(6)
        );
    END

    -- >>>>>   Full Refresh Strategy: Truncate and Reload   <<<<<
    TRUNCATE TABLE [test_data].[Products];

    -- Insert from dev_data to test_data with Business Logic (Launch Date)
    INSERT INTO [test_data].[Products] (
        Product_ID,
        Product_Name,
        SKU,
        Category,
        Brand,
        Unit_Cost,
        Unit_Price,
        Weight_KG,
        Is_Active,
        Markup_Pct,
        Launch_Date
    )
    SELECT 
        Products.Product_ID,
        Products.Product_Name,
        Products.SKU,
        Products.Category,
        Products.Brand,
        Products.Unit_Cost,
        Products.Unit_Price,
        Products.Weight_KG,
        Products.Is_Active,
        Products.Markup_Pct,
        Sales.First_Sale_Date
    FROM [dev_data].[Products] AS Products
        LEFT JOIN (
            -- Transposed DAX: CALCULATE(MIN(Sales[Date]), ALLEXCEPT(Products, Product_Code))
            SELECT 
                Product_ID, 
                MIN(Transaction_DateTime) AS First_Sale_Date
            FROM [dev_data].[Sales]
            GROUP BY Product_ID
        ) AS Sales 
            ON Products.Product_ID = Sales.Product_ID;

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
			'test_data',
			'pr_test_dim_products',
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
