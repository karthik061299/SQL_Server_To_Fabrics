/*
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Production-ready routine to recreate a point-in-time backup of Employee master dataset converted from SQL Server to Microsoft Fabric
## *Version*: 3 
## *Updated on*: 
_____________________________________________
*/

-- =======================================================================================
--   Script Name :  employee_bkup_refresh_fabric.sql
--   Purpose     :  Production-ready routine to (re)create a point-in-time backup of the
--                  Employee master dataset converted for Microsoft Fabric.
--                  Creates backup structure, populates it from authoritative tables.
--   Target DB   :  Microsoft Fabric
--   Author      :  AAVA
--   Created On  :  Converted from SQL Server
-- =======================================================================================

-- Step 1: Drop existing backup table if it exists using Fabric syntax
DROP TABLE IF EXISTS employee_bkup;

-- Step 2: Create the backup table structure with optimized Fabric data types
CREATE TABLE employee_bkup
(
    EmployeeNo   INT         NOT NULL PRIMARY KEY CLUSTERED,  -- Added CLUSTERED for performance
    FirstName    STRING      NOT NULL,   
    LastName     STRING      NOT NULL,   
    DepartmentNo INT         NULL,       
    NetPay       INT         NULL,
    BackupDate   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Added audit timestamp
)
WITH (
    DISTRIBUTION = HASH(EmployeeNo),  -- Fabric-specific distribution hint
    PARTITION = RANGE(BackupDate)      -- Partitioning for better query performance
);

-- Step 3: Populate the backup table with data from source tables
-- Using Fabric-optimized syntax and error handling
BEGIN
    -- Using Fabric's error handling approach
    DECLARE @error_message STRING = '';
    DECLARE @row_count INT = 0;
    
    -- Insert with enhanced error handling and logging
    BEGIN TRY
        INSERT INTO employee_bkup (EmployeeNo, FirstName, LastName, DepartmentNo, NetPay, BackupDate)
        SELECT  
            e.EmployeeNo,
            COALESCE(TRIM(e.FirstName), '') AS FirstName,  -- Using COALESCE instead of ISNULL
            COALESCE(TRIM(e.LastName), '') AS LastName,    -- Using COALESCE instead of ISNULL
            e.DepartmentNo,
            s.NetPay,
            CURRENT_TIMESTAMP AS BackupDate                 -- Using CURRENT_TIMESTAMP instead of GETDATE()
        FROM Employee AS e
        INNER JOIN Salary AS s
            ON e.EmployeeNo = s.EmployeeNo
        WHERE e.EmployeeNo IS NOT NULL;
        
        -- Get row count using Fabric syntax
        SET @row_count = @@ROWCOUNT;
        
        -- Log the operation success
        INSERT INTO operation_log (operation_name, status, record_count, timestamp)
        VALUES ('employee_bkup refresh', 'SUCCESS', @row_count, CURRENT_TIMESTAMP);
    END TRY
    BEGIN CATCH
        -- Capture error details using Fabric's error functions
        SET @error_message = ERROR_MESSAGE();
        
        -- Log the operation failure
        INSERT INTO operation_log (operation_name, status, error_message, timestamp)
        VALUES ('employee_bkup refresh', 'FAILED', @error_message, CURRENT_TIMESTAMP);
        
        -- Re-throw the error with additional context
        THROW 50000, @error_message, 1;
    END CATCH;
    
    -- Step 4: Conditional logic for empty source tables
    -- Using Fabric's approach to conditional operations
    IF @row_count = 0
    BEGIN
        -- Drop the backup table if no data was inserted
        DROP TABLE IF EXISTS employee_bkup;
        
        -- Log the empty table scenario
        INSERT INTO operation_log (operation_name, status, message, timestamp)
        VALUES ('employee_bkup refresh', 'WARNING', 'Backup table dropped - source table is empty', CURRENT_TIMESTAMP);
    END
    ELSE
    BEGIN
        -- Optimize the table for query performance
        ALTER TABLE employee_bkup REBUILD;
        
        -- Update statistics for better query performance
        UPDATE STATISTICS employee_bkup;
        
        -- Log successful completion with metrics
        INSERT INTO operation_log (operation_name, status, record_count, message, timestamp)
        VALUES ('employee_bkup refresh', 'COMPLETED', @row_count, 'Backup completed with optimization', CURRENT_TIMESTAMP);
    END;
END;

-- Step 5: Generate validation report
-- Using Fabric's advanced analytics capabilities
SELECT 
    'employee_bkup' AS table_name,
    COUNT(*) AS row_count,
    MIN(BackupDate) AS earliest_record,
    MAX(BackupDate) AS latest_record,
    COUNT(DISTINCT DepartmentNo) AS department_count,
    AVG(NetPay) AS average_net_pay,
    CURRENT_TIMESTAMP AS report_generated_at
FROM employee_bkup;

-- Optional: Display sample of backed up data with enhanced formatting
SELECT TOP 10 
    EmployeeNo,
    FirstName,
    LastName,
    DepartmentNo,
    NetPay,
    FORMAT(BackupDate, 'yyyy-MM-dd HH:mm:ss') AS backup_timestamp
FROM employee_bkup
ORDER BY EmployeeNo;

-- =======================================================================================
-- Reference DDL for source tables (Fabric-compatible versions)
-- =======================================================================================

-- Source Employee table structure (Fabric version)
/*
CREATE TABLE Employee (
    EmployeeNo   INT         NOT NULL PRIMARY KEY CLUSTERED,
    FirstName    STRING      NOT NULL,
    LastName     STRING      NOT NULL,
    DepartmentNo INT         NULL,
    HireDate     TIMESTAMP   NULL DEFAULT CURRENT_TIMESTAMP
)
WITH (
    DISTRIBUTION = HASH(EmployeeNo)
);
*/

-- Source Salary table structure (Fabric version)
/*
CREATE TABLE Salary (
    EmployeeNo INT           NOT NULL PRIMARY KEY CLUSTERED,
    NetPay     INT           NULL,
    LastUpdate TIMESTAMP     NULL DEFAULT CURRENT_TIMESTAMP
)
WITH (
    DISTRIBUTION = HASH(EmployeeNo)
);
*/

-- Operation logging table for tracking execution history
/*
CREATE TABLE operation_log (
    log_id          INT         NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    operation_name  STRING      NOT NULL,
    status          STRING      NOT NULL,
    record_count    INT         NULL,
    message         STRING      NULL,
    error_message   STRING      NULL,
    timestamp       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);
*/

-- =======================================================================================
-- CONVERSION NOTES:
-- =======================================================================================
-- 1. Enhanced Fabric-specific optimizations:
--    - Added CLUSTERED keyword to PRIMARY KEY constraints
--    - Implemented DISTRIBUTION hints for better performance
--    - Added table partitioning for improved query performance
--    - Added audit timestamp column for tracking
--
-- 2. Improved data type handling:
--    - Consistent use of STRING for character data
--    - TIMESTAMP for date/time values (replacing DATETIME)
--    - Added FORMAT function for date formatting
--
-- 3. Enhanced error handling:
--    - Implemented structured error logging
--    - Added operation_log table for tracking execution history
--    - Used Fabric's error handling mechanisms
--
-- 4. Function replacements:
--    - GETDATE() → CURRENT_TIMESTAMP
--    - ISNULL() → COALESCE()
--    - Added FORMAT() for better date formatting
--
-- 5. Performance enhancements:
--    - Added UPDATE STATISTICS for query optimization
--    - Implemented table partitioning
--    - Added clustered indexes
--    - Used hash distribution for large tables
--
-- 6. Added validation reporting:
--    - Enhanced metrics for data quality checks
--    - Added timestamp tracking
--    - Improved error logging and monitoring
-- =======================================================================================
