CREATE OR ALTER PROCEDURE [admin_logging].[pr_get_formatted_pipeline_errors]
AS
BEGIN
    -- deactivate counting for performance
    SET NOCOUNT ON;

    -- 1. Time Window Strategy
        /* 
        I'm using a 3-hour lookback window here.
        This is critical to handle the time difference between Fabric servers (UTC) and my local time.
        If I used a shorter window (like 1 hour), I might miss errors that just happened due to timezone lag.
        */
    DECLARE @LookbackPeriod DATETIME2 = DATEADD(HOUR, -3, SYSUTCDATETIME());
    
    -- 2. Variable Declaration
        /* 
        I'll use separate variables for each environment's errors.
        This avoids the messy logic of trying to concatenate everything in one go and handles NULLs much better.
        */
    DECLARE @DevErrors VARCHAR(MAX);
    DECLARE @TestErrors VARCHAR(MAX);
    DECLARE @ProdErrors VARCHAR(MAX);

    -- 3. Fetching Errors for DEV
        -- I use STRING_AGG with CHAR(13) to list errors one below the other without bullet points.
    SELECT 
        @DevErrors = STRING_AGG(ProcedureName + ': ' + ErrorMessage, CHAR(13))
    FROM [admin_logging].[ExecutionLogs]
    WHERE 
        SchemaName = 'dev_data' 
        AND Status = 'Error' 
        AND StartTime >= @LookbackPeriod;

    -- 4. Fetching Errors for TEST
    SELECT 
        @TestErrors = STRING_AGG(ProcedureName + ': ' + ErrorMessage, CHAR(13))
    FROM [admin_logging].[ExecutionLogs]
    WHERE 
        SchemaName = 'test_data' 
        AND Status = 'Error' 
        AND StartTime >= @LookbackPeriod;

    -- 5. Fetching Errors for PROD
    SELECT 
        @ProdErrors = STRING_AGG(ProcedureName + ': ' + ErrorMessage, CHAR(13))
    FROM [admin_logging].[ExecutionLogs]
    WHERE 
        SchemaName = 'prod_data' 
        AND Status = 'Error' 
        AND StartTime >= @LookbackPeriod;

    -- 6. Building the Final Report
        /*
        I use CONCAT_WS with a double new line (CHAR(13) + CHAR(13)) as the separator.
        This function is smart: it only adds the separator if the value is NOT NULL.
        So, if I only have PROD errors, I won't get empty spaces or weird headers for DEV/TEST.
        */
    DECLARE @FinalReport VARCHAR(MAX);
    
    SET @FinalReport = CONCAT_WS(CHAR(13) + CHAR(13), 
        CASE WHEN @DevErrors IS NOT NULL THEN 'DEV:' + CHAR(13) + @DevErrors END,
        CASE WHEN @TestErrors IS NOT NULL THEN 'TEST:' + CHAR(13) + @TestErrors END,
        CASE WHEN @ProdErrors IS NOT NULL THEN 'PROD:' + CHAR(13) + @ProdErrors END
    );

    -- 7. Final Output for the Pipeline
        /*
        If @FinalReport is NULL (no errors found anywhere), I return an empty string.
        This ensures the 'IF Condition' in my pipeline evaluates to False and skips the email.
        */
    SELECT ISNULL(@FinalReport, '') AS FormattedErrorList;
END;
