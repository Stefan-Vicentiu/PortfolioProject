DECLARE @Sql VARCHAR(MAX) = ''; 

-- 1. Generăm DROP pentru TABELE
SELECT @Sql = @Sql + 'DROP TABLE [' + s.name + '].[' + t.name + ']; '
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dev_data';

-- 2. Generăm DROP pentru PROCEDURI (Asta lipsea!)
SELECT @Sql = @Sql + 'DROP PROCEDURE [' + s.name + '].[' + p.name + ']; '
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name = 'dev_data';

-- 3. Executăm curățenia obiectelor
IF @Sql <> '' 
BEGIN
    EXEC(@Sql);
END

-- 4. Acum ștergem schema (va merge garantat)
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'dev_data')
BEGIN
    DROP SCHEMA [dev_data];
END
GO
