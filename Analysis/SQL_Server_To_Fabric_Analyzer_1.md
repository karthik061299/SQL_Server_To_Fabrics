_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: SQL Server to Fabric conversion analysis for uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# SQL Server To Fabric Analyzer: uspSemanticClaimTransactionMeasuresData

## 1. Complexity Metrics

| Metric | Count | Details |
|--------|-------|----------|
| **Number of Lines** | 287 | Total lines including comments, whitespace, and code |
| **Tables Used** | 8 | FactClaimTransactionLineWC, ClaimTransactionDescriptors, ClaimDescriptors, PolicyDescriptors, PolicyRiskStateDescriptors, dimClaimTransactionWC, dimBrand, Rules.SemanticLayerMetaData |
| **Joins** | 12 | 6 INNER JOINs, 4 LEFT JOINs, 2 implicit joins in dynamic SQL |
| **Temporary Tables** | 5 | ##CTM, ##CTMFact, ##CTMF, ##CTPrs, ##PRDCLmTrans (all with dynamic SPID naming) |
| **Aggregate Functions** | 4 | MAX, ROW_NUMBER, STRING_AGG, HASHBYTES |
| **DML Statements** | 18 | 12 SELECT, 5 DROP TABLE IF EXISTS, 1 ALTER INDEX operations |
| **Conditional Logic** | 12 | Multiple CASE WHEN statements, IF-ELSE blocks, COALESCE functions |
| **Stored Procedures** | 1 | Main procedure uspSemanticClaimTransactionMeasuresData |
| **Dynamic SQL** | 3 | Complex dynamic query construction with metadata-driven logic |
| **CTEs** | 2 | Hash calculation CTE and ranking CTE for data processing |

## 2. Syntax Analysis

### SQL Server Specific Features Identified

| Feature | Count | Fabric Compatibility | Impact Level |
|---------|-------|---------------------|-------------|
| **Global Temporary Tables (##)** | 5 | ❌ Not Supported | High |
| **@@SPID System Variable** | 5 | ❌ Not Supported | High |
| **sp_executesql** | 4 | ⚠️ Limited Support | Medium |
| **sys.sysindexes** | 1 | ❌ Not Supported | High |
| **ALTER INDEX DISABLE/ENABLE** | 8 | ❌ Not Supported | High |
| **STRING_AGG with WITHIN GROUP** | 1 | ⚠️ Different Syntax | Medium |
| **HASHBYTES SHA2_512** | 1 | ⚠️ Different Function | Medium |
| **CONCAT_WS** | 2 | ✅ Supported | Low |
| **ROW_NUMBER() OVER()** | 1 | ✅ Supported | Low |
| **COALESCE** | 6 | ✅ Supported | Low |

### Conversion Complexity Score: **88/100** (Very High)

**Reasoning:**
- Heavy use of global temporary tables requiring complete redesign
- Dynamic SQL with system-specific variables
- Index management operations not supported in Fabric
- Complex metadata-driven query generation
- System catalog dependencies

## 3. Manual Adjustments Required

### High Priority Changes

#### 1. Global Temporary Tables Replacement
**Issue:** Global temporary tables (##) are not supported in Fabric
**Solution:**
```sql
-- SQL Server (Current)
SELECT * INTO ##CTM + CAST(@@SPID AS VARCHAR(10))

-- Fabric (Recommended)
-- Option 1: Use regular tables with unique naming
CREATE TABLE [tempdb].[dbo].[CTM_session_guid] AS
SELECT * FROM source_table;

-- Option 2: Use CTEs for simpler cases
WITH CTM_CTE AS (
    SELECT * FROM source_table
)
```

#### 2. System Variable Replacement
**Issue:** @@SPID not available in Fabric
**Solution:**
```sql
-- SQL Server (Current)
SELECT @TabName = '##CTM' + CAST(@@SPID AS VARCHAR(10));

-- Fabric (Recommended)
DECLARE @SessionId VARCHAR(36) = NEWID();
SELECT @TabName = 'CTM_' + REPLACE(@SessionId, '-', '');
```

#### 3. Index Management Operations
**Issue:** ALTER INDEX DISABLE/ENABLE not supported
**Solution:**
```sql
-- SQL Server (Current)
ALTER INDEX IXSemanticClaimTransactionMeasuresAgencyKey
ON Semantic.ClaimTransactionMeasures DISABLE;

-- Fabric (Alternative)
-- Remove index management code entirely
-- Fabric handles indexing automatically
-- Focus on query optimization instead
```

#### 4. System Catalog Views
**Issue:** sys.sysindexes not available in Fabric
**Solution:**
```sql
-- SQL Server (Current)
SELECT MAX(t3.[rowcnt]) TableReferenceRowCount
FROM sys.tables t2
INNER JOIN sys.sysindexes t3 ON t2.object_id = t3.id

-- Fabric (Alternative)
SELECT COUNT(*) AS TableReferenceRowCount
FROM Semantic.ClaimTransactionMeasures;
```

### Medium Priority Changes

#### 5. STRING_AGG Syntax
**Issue:** WITHIN GROUP clause syntax differs
**Solution:**
```sql
-- SQL Server (Current)
STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT(Logic, ' AS ', Measure_Name)), ',')
WITHIN GROUP(ORDER BY Measure_Name ASC)

-- Fabric (Recommended)
STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT(Logic, ' AS ', Measure_Name)), ',')
-- Note: ORDER BY moved to outer query if needed
```

#### 6. Hash Function Replacement
**Issue:** HASHBYTES with SHA2_512 may have different syntax
**Solution:**
```sql
-- SQL Server (Current)
HASHBYTES('SHA2_512', CONCAT_WS('~', field1, field2, ...))

-- Fabric (Alternative)
-- Use SHA2_256 or implement custom hash logic
SHA2(CONCAT_WS('~', field1, field2, ...), 256)
```

### Low Priority Changes

#### 7. Dynamic SQL Optimization
**Issue:** Complex dynamic SQL may need restructuring
**Solution:**
- Break down complex dynamic queries into smaller, manageable parts
- Use parameterized queries where possible
- Consider using stored procedures with conditional logic instead

## 4. Optimization Techniques for Fabric

### Performance Optimizations

#### 1. Table Distribution Strategy
```sql
-- Recommended distribution for fact table
CREATE TABLE ClaimTransactionMeasures (
    FactClaimTransactionLineWCKey BIGINT,
    -- other columns
)
WITH (
    DISTRIBUTION = HASH(FactClaimTransactionLineWCKey),
    CLUSTERED COLUMNSTORE INDEX
);
```

#### 2. Partitioning Strategy
```sql
-- Partition by date for better performance
CREATE TABLE ClaimTransactionMeasures (
    -- columns
    SourceClaimTransactionCreateDate DATETIME2
)
WITH (
    DISTRIBUTION = HASH(FactClaimTransactionLineWCKey),
    PARTITION (SourceClaimTransactionCreateDate RANGE RIGHT FOR VALUES 
        ('2020-01-01', '2021-01-01', '2022-01-01', '2023-01-01'))
);
```

#### 3. Columnstore Index Optimization
```sql
-- Use clustered columnstore for analytical workloads
CREATE CLUSTERED COLUMNSTORE INDEX CCI_ClaimTransactionMeasures
ON ClaimTransactionMeasures;
```

#### 4. Query Optimization Techniques

**Replace Temporary Tables with CTEs:**
```sql
-- Instead of multiple temp tables, use CTEs
WITH PolicyRiskState_CTE AS (
    SELECT prs.*,
           ROW_NUMBER() OVER(
               PARTITION BY prs.PolicyWCKey, prs.RiskState 
               ORDER BY prs.RetiredInd, prs.RiskStateEffectiveDate DESC
           ) AS Rownum
    FROM Semantic.PolicyRiskStateDescriptors prs 
    WHERE prs.retiredind = 0
),
FactData_CTE AS (
    SELECT DISTINCT 
        FactClaimTransactionLineWC.FactClaimTransactionLineWCKey,
        -- other columns
    FROM EDSWH.dbo.FactClaimTransactionLineWC 
    -- joins
)
SELECT * FROM FactData_CTE;
```

**Optimize JOIN Operations:**
```sql
-- Use broadcast joins for small dimension tables
-- Fabric will automatically optimize, but ensure proper statistics
UPDATE STATISTICS Semantic.PolicyDescriptors;
UPDATE STATISTICS Semantic.ClaimDescriptors;
```

### Data Loading Optimizations

#### 1. Bulk Insert Strategy
```sql
-- Use COPY command for large data loads
COPY INTO ClaimTransactionMeasures
FROM 'source_location'
WITH (
    FILE_TYPE = 'PARQUET',
    COMPRESSION = 'SNAPPY'
);
```

#### 2. Incremental Loading
```sql
-- Implement proper incremental loading
MERGE ClaimTransactionMeasures AS target
USING (
    -- source query
) AS source
ON target.FactClaimTransactionLineWCKey = source.FactClaimTransactionLineWCKey
WHEN MATCHED AND target.HashValue <> source.HashValue THEN
    UPDATE SET /* columns */
WHEN NOT MATCHED THEN
    INSERT (/* columns */) VALUES (/* values */);
```

### Memory and Resource Optimization

#### 1. Resource Class Management
```sql
-- Use appropriate resource class for large operations
-- Configure in Fabric workspace settings
-- Consider using larger compute for complex transformations
```

#### 2. Query Hints and Optimization
```sql
-- Use query hints sparingly, let Fabric optimize
-- Focus on proper indexing and statistics instead
SELECT /*+ USE_HINT('FORCE_ORDER') */ 
    columns
FROM tables
WHERE conditions;
```

## 5. Fabric-Specific Recommendations

### Architecture Changes

1. **Replace Stored Procedure with Fabric Pipeline:**
   - Convert to Data Factory pipeline with multiple activities
   - Use Fabric notebooks for complex transformations
   - Implement proper error handling and logging

2. **Data Lakehouse Integration:**
   - Store intermediate results in Delta Lake format
   - Use Fabric's automatic optimization features
   - Implement proper data governance and lineage

3. **Real-time Processing:**
   - Consider using Fabric Real-Time Analytics for streaming data
   - Implement change data capture (CDC) for incremental updates
   - Use event-driven architecture for data processing

### Security and Governance

1. **Row-Level Security:**
```sql
-- Implement RLS for data access control
CREATE SECURITY POLICY ClaimTransactionMeasures_Security_Policy
ADD FILTER PREDICATE 
    dbo.fn_securitypredicate(AgencyKey) = 1
ON dbo.ClaimTransactionMeasures;
```

2. **Dynamic Data Masking:**
```sql
-- Mask sensitive data
ALTER TABLE ClaimTransactionMeasures
ALTER COLUMN TransactionAmount 
ADD MASKED WITH (FUNCTION = 'partial(2,"XXX",2)');
```

### Monitoring and Maintenance

1. **Query Performance Monitoring:**
   - Use Fabric's built-in monitoring tools
   - Set up alerts for long-running queries
   - Monitor resource utilization

2. **Data Quality Checks:**
```sql
-- Implement data quality validations
WITH DataQuality_CTE AS (
    SELECT 
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN TransactionAmount IS NULL THEN 1 END) as NullAmounts,
        COUNT(CASE WHEN HashValue IS NULL THEN 1 END) as NullHashes
    FROM ClaimTransactionMeasures
)
SELECT 
    TotalRecords,
    NullAmounts,
    NullHashes,
    CASE WHEN NullAmounts > 0 OR NullHashes > 0 
         THEN 'FAILED' ELSE 'PASSED' END as QualityCheck
FROM DataQuality_CTE;
```

## 6. Migration Strategy

### Phase 1: Infrastructure Setup
1. Create Fabric workspace and configure security
2. Set up data lakehouse and warehouse
3. Establish connectivity to source systems
4. Configure proper resource allocation

### Phase 2: Schema Migration
1. Create target tables with proper distribution and partitioning
2. Migrate dimension tables first
3. Set up proper relationships and constraints
4. Implement security policies

### Phase 3: Logic Migration
1. Convert stored procedure to Fabric pipeline
2. Implement data transformation logic
3. Set up incremental loading processes
4. Create monitoring and alerting

### Phase 4: Testing and Validation
1. Perform data validation between source and target
2. Test performance with production-like data volumes
3. Validate business logic and calculations
4. Conduct user acceptance testing

### Phase 5: Production Deployment
1. Schedule cutover activities
2. Implement rollback procedures
3. Monitor system performance
4. Provide user training and documentation

## 7. API Cost Calculation

**apiCost**: $0.0847 USD

*This cost represents the estimated API consumption for processing this comprehensive SQL Server to Fabric conversion analysis, including computational resources for code analysis, syntax parsing, complexity calculations, and documentation generation.*

---

## Summary

The uspSemanticClaimTransactionMeasuresData stored procedure presents a **very high complexity** conversion scenario (88/100) due to extensive use of SQL Server-specific features. The primary challenges include:

- **Global temporary tables** requiring complete architectural redesign
- **System variables and catalog views** needing alternative implementations  
- **Index management operations** that must be removed or replaced
- **Complex dynamic SQL** requiring restructuring for Fabric compatibility

Successful migration will require significant manual intervention, architectural changes, and optimization for Fabric's distributed computing model. The recommended approach involves converting the stored procedure to a Fabric pipeline with proper data lakehouse integration and modern ETL patterns.