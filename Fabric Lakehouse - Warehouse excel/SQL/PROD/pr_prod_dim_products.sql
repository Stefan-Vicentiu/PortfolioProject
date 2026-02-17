-- only create the schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prod_data')
BEGIN
    EXEC('CREATE SCHEMA [prod_data]');
END
GO

CREATE OR ALTER PROCEDURE [prod_data].[pr_prod_dim_products]
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
        WHERE t.name = 'Products' 
            AND s.name = 'prod_data'
    )
    BEGIN
        -- create Products dimension in prod_data
        CREATE TABLE [prod_data].[Products] (
            [Product Code] VARCHAR(255) NOT NULL,
            [Product Name] VARCHAR(255),
            SKU VARCHAR(100),
            Category VARCHAR(100),
            Brand VARCHAR(100),
            [Unit Cost] DECIMAL(18,2),
            [Unit Price] DECIMAL(18,2),
            [Weight KG] DECIMAL(18,2),
            [Is Active] BIT,
            [Markup Pct] DECIMAL(18,2),
            [Launch Date] DATETIME2(6)
        );
    END

    -- >>>>>   Full Refresh Strategy: Truncate and Reload   <<<<<
    TRUNCATE TABLE [prod_data].[Products];

    -- Insert from test_data to prod_data
    INSERT INTO [prod_data].[Products] (
        [Product Code],
        [Product Name],
        SKU,
        Category,
        Brand,
        [Unit Cost],
        [Unit Price],
        [Weight KG],
        [Is Active],
        [Markup Pct],
        [Launch Date]
    )
    SELECT 
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

    FROM [test_data].[Products];

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
			'prod_data',
			'pr_prod_dim_products',
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