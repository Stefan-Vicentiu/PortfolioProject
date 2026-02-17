CREATE OR ALTER PROCEDURE [admin_logging].[RunAllDEV]
AS
BEGIN
    -- deactivate counting for performance
    SET NOCOUNT ON;
    -- 1. Budget
    BEGIN TRY EXEC [dev_data].[pr_dev_fact_budget];       END TRY BEGIN CATCH END CATCH
    -- 2. Hr
    BEGIN TRY EXEC [dev_data].[pr_dev_fact_hr];           END TRY BEGIN CATCH END CATCH
    -- 3. Inventory
    BEGIN TRY EXEC [dev_data].[pr_dev_fact_inventory];    END TRY BEGIN CATCH END CATCH
    -- 4. Purchase
    BEGIN TRY EXEC [dev_data].[pr_dev_fact_purchase];     END TRY BEGIN CATCH END CATCH
    -- 5. Sales
    BEGIN TRY EXEC [dev_data].[pr_dev_fact_sales];        END TRY BEGIN CATCH END CATCH
    -- 6. Accounts
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_accounts];      END TRY BEGIN CATCH END CATCH
    -- 7. Business Lines
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_business_lines]; END TRY BEGIN CATCH END CATCH
    -- 8. Customers
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_customers];     END TRY BEGIN CATCH END CATCH
    -- 9. Products
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_products];      END TRY BEGIN CATCH END CATCH
    -- 10. Employees
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_employees];     END TRY BEGIN CATCH END CATCH
    -- 11. Suppliers
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_suppliers];     END TRY BEGIN CATCH END CATCH
    -- 12. Documents
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_documents];     END TRY BEGIN CATCH END CATCH
    -- 13. Date
    BEGIN TRY EXEC [dev_data].[pr_dev_dim_date];          END TRY BEGIN CATCH END CATCH
END
