# SQL Server to Fabric Conversion Test Cases

_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Test case documentation for SQL Server to Fabric conversion of uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 2 
## *Updated on*: 
_____________________________________________

## Overview

This document provides a comprehensive list of test cases for validating the SQL Server to Fabric conversion of the `uspSemanticClaimTransactionMeasuresData` stored procedure. The test cases are designed to verify syntax changes, logic preservation, and performance characteristics between the SQL Server and Fabric implementations.

## Test Case Categories

1. **Syntax Conversion Tests** - Validate that SQL Server syntax is correctly converted to Fabric syntax
2. **Logic Preservation Tests** - Ensure business logic is preserved during conversion
3. **Dynamic SQL Generation Tests** - Verify dynamic SQL generation functionality
4. **Specific Business Rules Tests** - Test specific business rules in the stored procedure
5. **Performance Validation Tests** - Compare performance between SQL Server and Fabric
6. **Data Integrity Tests** - Validate data integrity between platforms
7. **Integration Tests** - Test end-to-end workflows and integration scenarios
8. **Edge Case Tests** - Test handling of edge cases and unusual scenarios

## Test Cases

### 1. Syntax Conversion Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| SC-001 | Session ID Conversion | Both SQL Server and Fabric environments available | 1. Execute SQL Server query: `SELECT @@SPID AS session_id`<br>2. Execute Fabric query: `SELECT SESSION_ID() AS session_id` | Both queries return a session ID value | Both queries return a valid integer session ID |
| SC-002 | Temporary Table Syntax | Both environments available | 1. Create a temporary table in SQL Server with session ID<br>2. Create a temporary table in Fabric with session ID<br>3. Insert test data<br>4. Query the tables | Both queries return the same data | Data in both temporary tables matches |
| SC-003 | Date Function Conversion | Both environments available | 1. Execute SQL Server query with `GETDATE()`<br>2. Execute Fabric query with `CURRENT_TIMESTAMP()` | Both queries return current date/time | Both functions return valid datetime values within 1 minute of each other |
| SC-004 | Hash Function Conversion | Both environments available | 1. Execute SQL Server query with `HASHBYTES('SHA2_256', 'test_string')`<br>2. Execute Fabric query with `SHA2('test_string', 256)` | Both queries return the same hash value | Hash values match between platforms |
| SC-005 | String Aggregation Conversion | Both environments available | 1. Create test data with string values<br>2. Use `STRING_AGG` in both platforms to concatenate values | Both queries return the same concatenated string | Aggregated strings match between platforms |

### 2. Logic Preservation Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| LP-001 | Date Conversion Logic | Both environments available | 1. Test the conversion of '01/01/1900' to '01/01/1700' in both platforms | Both platforms convert the date correctly | Both platforms return '01/01/1700' when '01/01/1900' is input |
| LP-002 | Financial Calculations | Both environments available | 1. Create test data with financial values<br>2. Calculate net paid and member responsibility in both platforms | Both platforms calculate the same values | Financial calculations match between platforms |
| LP-003 | Hash Value Change Detection | Both environments available | 1. Create original and modified records<br>2. Calculate hash values in both platforms<br>3. Compare hash values | Hash values differ for modified records and match for identical records | Hash-based change detection works consistently |

### 3. Dynamic SQL Generation Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| DG-001 | Measure SQL Generation | Mock metadata available | 1. Generate measure SQL from metadata in both platforms | Both platforms generate the same SQL | Generated SQL matches between platforms |
| DG-002 | Temp Table Naming with Session ID | Session IDs available | 1. Generate temp table names with session IDs in both platforms | Temp table names follow platform-specific conventions | SQL Server uses `##` prefix, Fabric uses `temp_` prefix |

### 4. Specific Business Rules Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| BR-001 | Retired Indicator Filtering | Test data with retired indicators | 1. Filter records based on retired indicator in both platforms | Only active records (RetiredInd=0) are included | Filtered results match between platforms |
| BR-002 | Row Number Partitioning | Test data with multiple records per partition | 1. Apply ROW_NUMBER() partitioning in both platforms<br>2. Filter to Rownum=1 | Only one record per partition is returned | Partitioning and filtering works consistently |

### 5. Performance Validation Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| PV-001 | Execution Time Comparison | Both environments available | 1. Execute the same query in both platforms<br>2. Measure execution time | Fabric execution time is within acceptable threshold | Fabric is no more than 20% slower than SQL Server |

### 6. Data Integrity Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| DI-001 | Result Count Validation | Both environments available | 1. Execute the same query in both platforms<br>2. Compare result counts | Result counts match between platforms | Row counts are identical |
| DI-002 | Financial Totals Validation | Both environments available | 1. Calculate financial totals in both platforms<br>2. Compare totals | Financial totals match between platforms | Totals are within 0.01 of each other |

### 7. Integration Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| IT-001 | End-to-End Workflow | Test data available in both environments | 1. Execute stored procedure in both platforms<br>2. Compare results | Results match between platforms | Data matches between platforms |
| IT-002 | Cross-Validation Between Platforms | Test data available in both environments | 1. Execute stored procedure in both platforms<br>2. Compare specific financial measures | Financial measures match between platforms | Measures are within 0.01 of each other |
| IT-003 | Dynamic SQL Execution | Both environments available | 1. Generate and execute dynamic SQL in both platforms | Results match between platforms | Generated SQL produces the same results |
| IT-004 | Session ID Handling for Temp Tables | Both environments available | 1. Execute stored procedure with different session IDs | Temp tables are created with correct session IDs | Temp tables have unique names based on session ID |
| IT-005 | Transaction Isolation | Both environments available | 1. Execute stored procedure concurrently with different session IDs | Concurrent executions don't interfere with each other | Each execution uses its own temp tables |

### 8. Edge Case Tests

| Test Case ID | Description | Preconditions | Test Steps | Expected Result | Pass/Fail Criteria |
|-------------|-------------|---------------|------------|-----------------|--------------------|
| EC-001 | Empty Result Set | Both environments available | 1. Execute stored procedure with parameters that return no results | Both platforms handle empty results correctly | No errors occur with empty result sets |
| EC-002 | Date Conversion Edge Case | Both environments available | 1. Execute stored procedure with '1900-01-01' as start date | Date is converted to '1700-01-01' in both platforms | Conversion occurs correctly |
| EC-003 | Large Data Volume | Large test dataset available | 1. Execute stored procedure with large data volume | Both platforms handle large data volume | No errors or timeouts occur |

## Performance Test Matrix

| Test ID | Data Volume | SQL Server Time (s) | Fabric Time (s) | Difference (%) | Pass/Fail |
|---------|-------------|---------------------|-----------------|----------------|------------|
| PT-001 | Small (100 records) | TBD | TBD | TBD | TBD |
| PT-002 | Medium (1,000 records) | TBD | TBD | TBD | TBD |
| PT-003 | Large (10,000 records) | TBD | TBD | TBD | TBD |

## Test Execution Report Template

```
Test Execution Report - [TIMESTAMP]

Total Tests: [COUNT]
Passed: [COUNT]
Failed: [COUNT]
Skipped: [COUNT]

Test Details:
  [TEST_NAME]: [OUTCOME]
  [TEST_NAME]: [OUTCOME]
    Error: [ERROR_MESSAGE]
  ...

Performance Summary:
  SQL Server Average: [TIME] seconds
  Fabric Average: [TIME] seconds
  Difference: [PERCENTAGE]%

Conclusion:
  [PASS/FAIL] - [SUMMARY]
```

## API Cost Calculation

**API Cost for this test suite**: $0.0000

*Note: This test suite uses internal processing capabilities without external API calls.*