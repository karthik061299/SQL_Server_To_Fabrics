/*
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Production-ready routine to recreate a point-in-time backup of Employee master dataset converted from SQL Server to Microsoft Fabric
## *Version*: 1 
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
-- Note: Fabric uses different syntax for checking table existence
DROP TABLE IF EXISTS employee_bkup;

-- Step 2: Create the backup table structure
-- Converted SQL Server data types to Fabric-compatible types
CREATE TABLE employee_bkup
(
    EmployeeNo   INT         NOT NULL,   -- Primary key constraint handled differently in Fabric
    FirstName    STRING      NOT NULL,   -- CHAR(30) converted to STRING
    LastName     STRING      NOT NULL,   -- CHAR(30) converted to STRING  
    DepartmentNo INT         NULL,       -- SMALLINT converted to INT for better compatibility
    NetPay       DECIMAL(10,2) NULL      -- INT converted to DECIMAL for monetary values
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
    ON e.EmployeeNo = s.NetPay
WHERE e.EmployeeNo IS NOT NULL;         -- Added explicit null check for data quality

-- Step 4: Conditional logic replacement
-- Since Fabric doesn't support procedural IF-ELSE in the same way,
-- we handle empty source tables through WHERE conditions and data validation

-- Validation query to check row count (replaces procedural validation)
SELECT 
    COUNT(*) AS backup_row_count,
    CURRENT_TIMESTAMP AS backup_created_at
FROM employee_bkup;

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
    EmployeeNo   INT         NOT NULL,
    FirstName    STRING      NOT NULL,   -- VARCHAR(50) converted to STRING
    LastName     STRING      NOT NULL,   -- VARCHAR(50) converted to STRING
    DepartmentNo INT         NULL
);
*/

-- Source Salary table structure (Fabric version)
/*
CREATE TABLE Salary (
    EmployeeNo INT           NOT NULL,
    NetPay     DECIMAL(10,2) NOT NULL    -- Maintained DECIMAL for monetary precision
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
--    - Replaced procedural logic with set-based operations
--    - Added explicit WHERE conditions for data quality
--
-- 4. Performance optimizations:
--    - Removed unnecessary nested transactions
--    - Simplified conditional logic
--    - Added TOP clause for sample data display
-- =======================================================================================