/*
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Production-ready routine to recreate a point-in-time backup of Employee master dataset converted from SQL Server to Microsoft Fabric
## *Version*: 2 
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

-- Fabric SQL does not support procedural elements like TRY-CATCH, so we'll use set-based operations

-- Step 1: Drop existing backup table if it exists
-- Note: Fabric uses DROP TABLE IF EXISTS syntax for table existence checking
DROP TABLE IF EXISTS employee_bkup;

-- Step 2: Create the backup table structure
-- Converted SQL Server data types to Fabric-compatible types
CREATE TABLE employee_bkup
(
    EmployeeNo   INT         NOT NULL PRIMARY KEY,   -- Fabric supports PRIMARY KEY declaration
    FirstName    STRING      NOT NULL,   -- CHAR(30) converted to STRING
    LastName     STRING      NOT NULL,   -- CHAR(30) converted to STRING  
    DepartmentNo INT         NULL,       -- SMALLINT converted to INT for better compatibility
    NetPay       INT         NULL        -- Keeping INT as in the original SQL Server script
);

-- Step 3: Populate the backup table with data from source tables
-- Using Fabric-compatible syntax
INSERT INTO employee_bkup (EmployeeNo, FirstName, LastName, DepartmentNo, NetPay)
SELECT  
    e.EmployeeNo,
    TRIM(e.FirstName) AS FirstName,     -- Added TRIM to handle CHAR padding
    TRIM(e.LastName) AS LastName,       -- Added TRIM to handle CHAR padding
    e.DepartmentNo,
    s.NetPay
FROM Employee AS e
INNER JOIN Salary AS s
    ON e.EmployeeNo = s.EmployeeNo      -- Fixed join condition (was incorrectly joining on NetPay)
WHERE e.EmployeeNo IS NOT NULL;         -- Added explicit null check for data quality

-- Step 4: Conditional logic replacement
-- Since Fabric doesn't support procedural IF-ELSE in the same way,
-- we handle empty source tables through a separate validation query

-- Check if source table is empty and drop backup if needed
-- This mimics the original SQL Server behavior
DECLARE @source_count INT = (SELECT COUNT(*) FROM Employee);
IF @source_count = 0
BEGIN
    DROP TABLE IF EXISTS employee_bkup;
    SELECT 'Backup table dropped - source table is empty' AS status;
END
ELSE
BEGIN
    -- Validation query to check row count (replaces procedural validation)
    SELECT 
        COUNT(*) AS backup_row_count,
        CURRENT_TIMESTAMP AS backup_created_at,
        'Backup completed successfully' AS status
    FROM employee_bkup;
END

-- Optional: Update statistics for better query performance
-- Fabric equivalent of UPDATE STATISTICS
ALTER TABLE employee_bkup REBUILD;

-- Optional: Display sample of backed up data
SELECT TOP 10 *
FROM employee_bkup
ORDER BY EmployeeNo;

-- =======================================================================================
-- Reference DDL for source tables (Fabric-compatible versions)
-- =======================================================================================

-- Source Employee table structure (Fabric version)
/*
CREATE TABLE Employee (
    EmployeeNo   INT         NOT NULL PRIMARY KEY,
    FirstName    STRING      NOT NULL,   -- VARCHAR(50) converted to STRING
    LastName     STRING      NOT NULL,   -- VARCHAR(50) converted to STRING
    DepartmentNo INT         NULL
);
*/

-- Source Salary table structure (Fabric version)
/*
CREATE TABLE Salary (
    EmployeeNo INT           NOT NULL PRIMARY KEY,
    NetPay     INT           NULL        -- Keeping INT as in the original SQL Server script
);
*/

-- =======================================================================================
-- CONVERSION NOTES:
-- =======================================================================================
-- 1. Removed SQL Server-specific elements:
--    - SET NOCOUNT ON (not needed in Fabric)
--    - BEGIN TRY/CATCH blocks (not supported)
--    - OBJECT_ID() function (replaced with DROP TABLE IF EXISTS)
--    - GO statements (not needed)
--    - XACT_STATE() and ROLLBACK (different transaction handling)
--    - THROW statement (different error handling)
--
-- 2. Data type conversions:
--    - CHAR(30) -> STRING (more flexible in Fabric)
--    - SMALLINT -> INT (better compatibility)
--    - VARCHAR -> STRING (Fabric standard)
--
-- 3. Syntax adjustments:
--    - Added TRIM() functions to handle CHAR padding
--    - Simplified table existence checking
--    - Fixed join condition between Employee and Salary tables
--    - Added explicit WHERE conditions for data quality
--    - Implemented conditional logic using Fabric syntax
--
-- 4. Performance optimizations:
--    - Added ALTER TABLE REBUILD (equivalent to UPDATE STATISTICS)
--    - Simplified conditional logic
--    - Added TOP clause for sample data display
--    - Maintained PRIMARY KEY constraints for better query performance
-- =======================================================================================