_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server stored procedure uspSemanticClaimTransactionMeasuresData conversion to Fabric SQL review with enhanced recommendations
## *Version*: 3 
## *Updated on*: 
_____________________________________________

# SQL Server to Fabric Conversion Review

## 1. Summary

The `uspSemanticClaimTransactionMeasuresData` stored procedure is a complex data processing routine designed to retrieve and process semantic claim transaction measures data for ClaimMeasures population. This procedure involves sophisticated ETL operations including dynamic SQL generation, temporary table management with session-specific naming, hash-based change detection, and complex data transformations.

The conversion to Microsoft Fabric requires a fundamental architectural shift from traditional T-SQL stored procedures to Fabric's Spark SQL-based environment. This review document identifies key conversion challenges and provides specific recommendations for a successful migration while maintaining functionality, performance, and data integrity.

The procedure's primary purpose is to process claim transaction data, apply business rules from the Rules.SemanticLayerMetaData table, and populate the Semantic.ClaimTransactionMeasures table with calculated measures. It handles complex logic for identifying new and changed records using hash values and manages various recovery types and financial calculations.

## 2. Conversion Accuracy

### 2.1 Core Functionality Analysis

| Component | SQL Server Implementation | Fabric Equivalent | Compatibility | Migration Complexity |
|-----------|---------------------------|----------------------|---------------|---------------------|
| Session ID Management | `@@spid` | `SESSION_ID()` or GUID generation | ⚠️ Requires modification | Medium |
| Temporary Tables | Global temp tables (`##CTM` + session ID) | Temporary views or DataFrame caching | ⚠️ Requires restructuring | High |
| Dynamic SQL | Complex string concatenation with `sp_executesql` | Parameterized queries or Python/Scala code | ⚠️ Requires significant refactoring | High |
| Hash Value Generation | `HASHBYTES('SHA2_512', ...)` | `HASH()` or Python hash functions | ⚠️ Requires function replacement | Medium |
| Date Handling | Minimum date: '01/01/1900' | Minimum date: '01/01/1700' | ✅ Simple value replacement | Low |
| Index Management | Dynamic index creation/disabling | Delta Lake optimization techniques | ⚠️ Requires complete redesign | High |
| String Concatenation | `CONCAT_WS('~', ...)` | `CONCAT_WS('~', ...)` or Python string joining | ✅ Compatible with minor adjustments | Low |
| Error Handling | Basic T-SQL error handling | Try/Catch blocks in Python/Scala | ⚠️ Requires redesign | Medium |

### 2.2 SQL Syntax Compatibility

| SQL Feature | Compatibility | Notes | Migration Approach |
|-------------|--------------|-------|--------------------|
| Basic SELECT/INSERT/UPDATE | ⚠️ Medium | Syntax similar but execution context differs | Direct conversion with context adjustments |
| JOIN operations | ✅ High | Syntax compatible but optimization differs | Direct conversion with performance tuning |
| Aggregation functions | ✅ High | Direct equivalents available | Direct conversion |
| Window functions | ✅ High | ROW_NUMBER() and other window functions supported | Direct conversion |
| Common Table Expressions | ✅ High | WITH clause fully supported | Direct conversion |
| Subqueries | ✅ High | Compatible syntax | Direct conversion |
| CASE expressions | ✅ High | Direct conversion | Direct conversion |
| String functions | ⚠️ Medium | Some functions may have different names or parameters | Function mapping required |
| Date functions | ⚠️ Medium | GETDATE() → CURRENT_TIMESTAMP | Function mapping required |
| Variable declarations | ❌ Low | DECLARE not supported in same way | Complete redesign required |
| Procedural logic | ❌ Low | BEGIN/END blocks not supported | Algorithmic redesign required |
| Dynamic SQL execution | ❌ Low | sp_executesql not available | Complete redesign required |

## 3. Discrepancies and Issues

### 3.1 Critical Issues

#### 3.1.1 Stored Procedure Architecture

**Issue**: Fabric doesn't support traditional stored procedures

**SQL Server Implementation:**
```sql
ALTER procedure [Semantic].[uspSemanticClaimTransactionMeasuresData]
(
    @pJobStartDateTime datetime2
  , @pJobEndDateTime datetime2
)
as
begin
    -- Procedure body
end;
```

**Fabric Solution:**
```python
# Fabric Notebook implementation
# Parameters as notebook widgets
pJobStartDateTime = spark.conf.get("pJobStartDateTime")
pJobEndDateTime = spark.conf.get("pJobEndDateTime")

# Main processing function
def process_semantic_claim_transaction_measures(start_date, end_date):
    # Processing logic implemented in Python/Spark
    # Return results as DataFrame
    
# Execute the function
result_df = process_semantic_claim_transaction_measures(pJobStartDateTime, pJobEndDateTime)
```

**Impact:** High - Requires complete architectural redesign

#### 3.1.2 Session ID and Temporary Table Management

**Issue**: Fabric doesn't support global temporary tables with session-specific naming

**SQL Server Implementation:**
```sql
declare @TabName varchar(100);
select @TabName = '##CTM' + cast(@@spid as varchar(10));

set @Select_SQL_Query = N'  DROP TABLE IF EXISTS  ' + @TabName;
execute sp_executesql @Select_SQL_Query;

-- Later used for dynamic table creation
set @Select_SQL_Query = N' \nselect * \ninto ' + @TabName + N' FROM...';
```

**Fabric Solution:**
```python
# Use Spark temporary views instead of temp tables
import uuid

# Generate unique identifier for this session
session_id = str(uuid.uuid4()).replace('-', '')[:10]
temp_view_name = f"claim_transaction_measures_{session_id}"

# Create temporary view
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW {temp_view_name} AS
SELECT * FROM claim_transactions WHERE...
""")

# For larger datasets, cache the DataFrame
claim_df = spark.sql(f"SELECT * FROM {temp_view_name}")
claim_df.cache()

# Clean up when done
spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
claim_df.unpersist()
```

**Impact:** High - Requires fundamental restructuring of temporary data storage approach

#### 3.1.3 Dynamic SQL Generation and Execution

**Issue**: Fabric has limited support for dynamic SQL execution

**SQL Server Implementation:**
```sql
-- Build dynamic SQL from multiple components
set @Select_SQL_Query = N'  DROP TABLE IF EXISTS  ' + @TabName;

-- Dynamically build measure calculations from metadata
select @Measure_SQL_Query
    = (string_agg(convert(nvarchar(max), concat(Logic, ' AS ', Measure_Name)), ',')within group(order by Measure_Name asc))
from Rules.SemanticLayerMetaData
where SourceType = 'Claims';

-- Combine components
set @Full_SQL_Query = N' ' + @Select_SQL_Query + @Measure_SQL_Query + @From_SQL_Query;

-- Execute with parameters
execute sp_executesql @Full_SQL_Query
                    , N' @pJobStartDateTime DATETIME2,  @pJobEndDateTime DATETIME2'
                    , @pJobStartDateTime = @pJobStartDateTime
                    , @pJobEndDateTime = @pJobEndDateTime;
```

**Fabric Solution:**
```python
# Python-based dynamic query construction
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Get measure definitions from metadata
measure_df = spark.sql("""
    SELECT Measure_Name, Logic 
    FROM Rules.SemanticLayerMetaData 
    WHERE SourceType = 'Claims'
    ORDER BY Measure_Name
""")

# Build select clause dynamically
select_clause = "SELECT FactClaimTransactionLineWCKey, RevisionNumber"

# Add measures dynamically
measure_expressions = []
for row in measure_df.collect():
    measure_expressions.append(f"{row.Logic} AS {row.Measure_Name}")

if measure_expressions:
    select_clause += ", " + ", ".join(measure_expressions)

# Build from clause
from_clause = """
FROM FactClaimTransactionLineWC
INNER JOIN ClaimTransactionDescriptors 
    ON FactClaimTransactionLineWC.ClaimTransactionWCKey = ClaimTransactionDescriptors.ClaimTransactionWCKey
-- other joins
"""

# Build where clause
where_clause = f"WHERE LoadUpdateDate >= '{start_date}' AND LoadUpdateDate <= '{end_date}'"

# Execute final query
final_query = f"{select_clause} {from_clause} {where_clause}"
result_df = spark.sql(final_query)
```

**Impact:** High - Complex dynamic SQL generation requires complete redesign

#### 3.1.4 Hash Value Generation for Change Detection

**Issue**: Different hash functions available in Fabric

**SQL Server Implementation:**
```sql
-- Hash generation for change detection
CONVERT(NVARCHAR(512), HASHBYTES('SHA2_512', CONCAT_WS('~',FactClaimTransactionLineWCKey
  ,RevisionNumber,PolicyWCKey,PolicyRiskStateWCKey,ClaimWCKey,ClaimTransactionLineCategoryKey,
  -- many more fields concatenated
  )), 1) AS HashValue
```

**Fabric Solution:**
```python
# Using Spark SQL hash functions
from pyspark.sql.functions import sha2, concat_ws, col

# Define columns to include in hash
hash_columns = [
    "FactClaimTransactionLineWCKey", "RevisionNumber", "PolicyWCKey", 
    "PolicyRiskStateWCKey", "ClaimWCKey", "ClaimTransactionLineCategoryKey",
    # Add all other required columns
]

# Create column references
col_refs = [col(c) for c in hash_columns]

# Generate hash value
df = df.withColumn("HashValue", 
                  sha2(concat_ws("~", *col_refs), 512))
```

**Impact:** Medium - Hash generation requires function replacement but concept remains similar

### 3.2 Medium Priority Issues

#### 3.2.1 Data Type Mapping

**Issue**: SQL Server data types need mapping to Fabric equivalents

**SQL Server Types and Fabric Equivalents:**

| SQL Server Type | Fabric Equivalent | Notes |
|----------------|-------------------|-------|
| `DATETIME2` | `TIMESTAMP` | Compatible with minor syntax changes |
| `VARCHAR(MAX)` | `STRING` | No length limitation in Fabric |
| `DECIMAL(18,2)` | `DECIMAL(38,18)` | Higher precision in Fabric by default |
| `BIT` | `BOOLEAN` | Direct mapping |
| `VARBINARY(MAX)` | `BINARY` | Compatible with minor syntax changes |
| `NVARCHAR` | `STRING` | No Unicode distinction in Fabric |
| `INT` | `INT` or `LONG` | Direct mapping |
| `BIGINT` | `LONG` | Direct mapping |

**Impact:** Medium - Data type conversion required throughout codebase

#### 3.2.2 Date Handling (1900 to 1700 Conversion)

**Issue**: Different minimum date values

**SQL Server Implementation:**
```sql
if @pJobStartDateTime = '01/01/1900'
begin
    set @pJobStartDateTime = '01/01/1700';
end;
```

**Fabric Solution:**
```python
# Python implementation
from datetime import datetime
import pandas as pd

# Convert string to datetime if needed
if isinstance(pJobStartDateTime, str):
    pJobStartDateTime = pd.to_datetime(pJobStartDateTime)

min_date_sql = pd.to_datetime("1900-01-01")
min_date_fabric = pd.to_datetime("1700-01-01")

if pJobStartDateTime == min_date_sql:
    pJobStartDateTime = min_date_fabric
```

**Impact:** Low - Simple value replacement

#### 3.2.3 Index Handling

**Issue**: Fabric uses different optimization techniques than SQL Server indexes

**SQL Server Implementation:**
```sql
if exists
(
    select *
    from sys.indexes
    where object_id = object_id(N'Semantic.ClaimTransactionMeasures')
          and name = N'IXSemanticClaimTransactionMeasuresAgencyKey'
)
begin
    alter index IXSemanticClaimTransactionMeasuresAgencyKey
    on Semantic.ClaimTransactionMeasures
    disable;
end;
```

**Fabric Solution:**
```python
# Delta Lake optimization techniques

# Check if table exists and is a Delta table
table_exists = spark.catalog.tableExists("Semantic.ClaimTransactionMeasures")

if table_exists:
    # Get table format
    table_format = spark.sql("DESCRIBE DETAIL Semantic.ClaimTransactionMeasures") \
                       .select("format").collect()[0][0]
    
    if table_format.lower() == "delta":
        # Optimize the table
        spark.sql("""
        OPTIMIZE Semantic.ClaimTransactionMeasures
        ZORDER BY (AgencyKey, ClaimTransactionWCKey)
        """)
        
        # Configure auto-optimize
        spark.sql("""
        ALTER TABLE Semantic.ClaimTransactionMeasures
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true', 
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
```

**Impact:** Medium - Complete redesign of optimization strategy required

### 3.3 Low Priority Issues

#### 3.3.1 System Table References

**Issue**: Different system tables and views in Fabric

**SQL Server Implementation:**
```sql
select max(t3.[rowcnt]) TableReferenceRowCount
from sys.tables t2
    inner join sys.sysindexes t3
        on t2.object_id = t3.id
where t2.[name] = 'ClaimTransactionMeasures'
      and schema_name(t2.[schema_id]) in ( 'semantic' )
```

**Fabric Solution:**
```python
# Use Spark catalog functions
def get_table_row_count(schema_name, table_name):
    try:
        # Check if table exists
        if spark.catalog.tableExists(f"{schema_name}.{table_name}"):
            # Get table statistics
            table_stats = spark.sql(f"DESCRIBE DETAIL {schema_name}.{table_name}")
            
            # Extract row count
            if "numRows" in table_stats.columns:
                row_count = table_stats.select("numRows").collect()[0][0]
                return row_count if row_count is not None else 0
            else:
                # If stats not available, count rows directly (expensive)
                return spark.table(f"{schema_name}.{table_name}").count()
        else:
            return 0
    except Exception as e:
        print(f"Error getting row count: {str(e)}")
        return 0

# Usage
table_reference_row_count = get_table_row_count("semantic", "ClaimTransactionMeasures")
```

**Impact:** Low - Different metadata access methods required

#### 3.3.2 Error Handling

**Issue**: Different error handling mechanisms

**SQL Server Implementation:**
```sql
-- Limited error handling in original procedure
BEGIN TRY
    -- Processing logic
END TRY
BEGIN CATCH
    -- Error handling
END CATCH
```

**Fabric Solution:**
```python
# Comprehensive error handling and logging
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ClaimProcessing")

try:
    # Start timing
    start_time = datetime.now()
    logger.info(f"Starting claim processing with parameters: {start_date} to {end_date}")
    
    # Processing logic
    result_df = spark.sql(query)
    row_count = result_df.count()
    
    # Log success
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Processing completed successfully. Rows processed: {row_count}, Duration: {duration} seconds")
    
except Exception as e:
    # Log error details
    logger.error(f"Processing failed: {str(e)}")
    
    # Write to error table
    error_df = spark.createDataFrame([(datetime.now(), "uspSemanticClaimTransactionMeasuresData", str(e))], 
                                   ["ErrorTime", "ProcedureName", "ErrorMessage"])
    error_df.write.format("delta").mode("append").saveAsTable("logs.ErrorLog")
    
    # Re-raise for notebook failure handling
    raise
```

**Impact:** Medium - Complete redesign of error handling approach

## 4. Optimization Suggestions

### 4.1 Architectural Redesign

**Current Approach:**
Monolithic stored procedure with complex dynamic SQL generation.

**Recommended Optimization:**

```
Modular Notebook Architecture:

1. Main Orchestration Notebook
   ├── Parameter validation and initialization
   ├── Execution logging
   └── Orchestration of child notebooks

2. Data Extraction Notebook
   ├── Source data retrieval
   ├── Initial filtering
   └── Temporary view creation

3. Data Transformation Notebook
   ├── Join operations
   ├── Measure calculations
   └── Hash value generation

4. Data Loading Notebook
   ├── Change detection
   ├── Delta table updates
   └── Result reporting

5. Utility Notebooks
   ├── Logging functions
   ├── Error handling
   └── Performance monitoring
```

**Implementation Example:**
```python
# 1. Main Orchestration Notebook

# Parameter widgets
dbutils.widgets.text("pJobStartDateTime", "")
dbutils.widgets.text("pJobEndDateTime", "")

# Get parameters
start_date = dbutils.widgets.get("pJobStartDateTime")
end_date = dbutils.widgets.get("pJobEndDateTime")

# Parameter validation
if not start_date or not end_date:
    raise ValueError("Start date and end date are required")

# Special date handling
if start_date == "01/01/1900":
    start_date = "01/01/1700"

# Start logging
log_execution_start("uspSemanticClaimTransactionMeasuresData", {"start_date": start_date, "end_date": end_date})

try:
    # Run extraction notebook
    dbutils.notebook.run("./1_Data_Extraction", timeout_seconds=3600, arguments={"start_date": start_date, "end_date": end_date})
    
    # Run transformation notebook
    dbutils.notebook.run("./2_Data_Transformation", timeout_seconds=3600)
    
    # Run loading notebook
    result = dbutils.notebook.run("./3_Data_Loading", timeout_seconds=3600)
    
    # Log success
    log_execution_success("uspSemanticClaimTransactionMeasuresData", result)
    
except Exception as e:
    # Log failure
    log_execution_failure("uspSemanticClaimTransactionMeasuresData", str(e))
    raise
```

### 4.2 Temporary Data Storage Strategy

**Current Approach:**
Dynamically named global temporary tables (`##CTM` + session ID).

**Recommended Optimization:**

```python
# Comprehensive temporary data management strategy

# 1. For small datasets: Spark temporary views
def create_temp_view(name, query):
    """Create a temporary view with the given name and query"""
    spark.sql(f"DROP VIEW IF EXISTS {name}")
    spark.sql(f"CREATE TEMPORARY VIEW {name} AS {query}")
    return name

# 2. For medium datasets: Cached DataFrames
def create_cached_df(query):
    """Create and cache a DataFrame from the given query"""
    df = spark.sql(query)
    df.cache()
    return df

# 3. For large datasets: Temporary Delta tables
def create_temp_delta_table(name, query, partition_by=None):
    """Create a temporary Delta table with optional partitioning"""
    # Generate unique name with session ID
    import uuid
    session_id = str(uuid.uuid4()).replace('-', '')[:8]
    full_name = f"temp.{name}_{session_id}"
    
    # Drop if exists
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    
    # Create table
    if partition_by:
        spark.sql(f"""
        CREATE TABLE {full_name}
        USING DELTA
        PARTITIONED BY ({partition_by})
        AS {query}
        """)
    else:
        spark.sql(f"""
        CREATE TABLE {full_name}
        USING DELTA
        AS {query}
        """)
    
    return full_name
```

### 4.3 Hash-Based Change Detection

**Current Approach:**
Complex hash generation and comparison for change detection.

**Recommended Optimization:**

```python
# Efficient hash-based change detection
from pyspark.sql.functions import sha2, concat_ws, col, when, lit

def detect_changes(new_data_df, existing_table, key_columns, hash_columns):
    """Detect new and changed records using hash-based comparison"""
    # Generate hash value for new data
    col_refs = [col(c) for c in hash_columns]
    new_data_with_hash = new_data_df.withColumn("HashValue", 
                                             sha2(concat_ws("~", *col_refs), 512))
    
    # Read existing data with key columns and hash
    if spark.catalog.tableExists(existing_table):
        existing_data = spark.table(existing_table).select(
            *key_columns, "HashValue"
        )
        
        # Join with new data to identify changes
        join_condition = " AND ".join([f"new.{k} = existing.{k}" for k in key_columns])
        
        # Mark records as new, changed, or unchanged
        result = new_data_with_hash.alias("new").join(
            existing_data.alias("existing"),
            expr(join_condition),
            "left_outer"
        ).select(
            "new.*",
            when(col("existing.HashValue").isNull(), lit("INSERT"))
            .when(col("new.HashValue") != col("existing.HashValue"), lit("UPDATE"))
            .otherwise(lit("UNCHANGED")).alias("ChangeType")
        )
        
        # Filter to only changed records if needed
        changed_records = result.filter(col("ChangeType").isin("INSERT", "UPDATE"))
        return changed_records
    else:
        # All records are new if table doesn't exist
        return new_data_with_hash.withColumn("ChangeType", lit("INSERT"))
```