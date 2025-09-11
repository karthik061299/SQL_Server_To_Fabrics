_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Documentation for uspSemanticClaimTransactionMeasuresData - Generates and processes claim transaction measures for analytics and reporting.
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# 1. Overview of Program

This stored procedure, `[Semantic].[uspSemanticClaimTransactionMeasuresData]`, is designed to extract, transform, and load (ETL) claim transaction measures data for enterprise analytics and reporting. It aligns with data warehousing best practices by dynamically generating and processing tables, applying business rules, and ensuring data integrity for downstream reporting and analytics. The procedure supports historical and incremental data loads, handles complex business logic, and provides audit capabilities for changes in claim transactions.

**Business Problem Addressed:**
- Centralizes claim transaction measures for accurate reporting and analytics.
- Supports regulatory, actuarial, and operational reporting needs.
- Automates data refresh and audit tracking for claim transactions.

**SQL Server Components Used:**
- Stored Procedure: `[Semantic].[uspSemanticClaimTransactionMeasuresData]`
- Tables: `Semantic.ClaimTransactionMeasures`, `Semantic.PolicyRiskStateDescriptors`, `EDSWH.dbo.FactClaimTransactionLineWC`, `edswh.dbo.dimClaimTransactionWC`, `Semantic.ClaimTransactionDescriptors`, `Semantic.ClaimDescriptors`, `Semantic.PolicyDescriptors`, `edswh.dbo.dimBrand`, `Rules.SemanticLayerMetaData`
- Temporary Tables: Dynamic global temp tables (e.g., `##CTM<spid>`, `##CTMFact<spid>`, etc.)

# 2. Code Structure and Design

- **Structure:**
  - Declares and initializes dynamic temp table names based on session ID.
  - Handles input parameter normalization (e.g., adjusts `@pJobStartDateTime` if set to '01/01/1900').
  - Checks row count in `Semantic.ClaimTransactionMeasures` and disables indexes if empty for performance.
  - Drops and recreates temp tables for staging data.
  - Loads data from source tables into temp tables using SELECT INTO.
  - Applies business rules and transformations using metadata-driven logic from `Rules.SemanticLayerMetaData`.
  - Joins multiple tables to enrich and validate claim transaction data.
  - Calculates hash values for audit and change tracking.
  - Identifies inserted/updated records and sets audit flags.
  - Outputs final result set for reporting or further ETL.

- **Key Components:**
  - DDL: Dynamic creation and dropping of temp tables.
  - DML: SELECT INTO, INSERT, UPDATE, and dynamic SQL execution.
  - Joins: INNER JOIN, LEFT JOIN.
  - Indexing: Disables indexes for performance during bulk operations.
  - Functions: Uses `ROW_NUMBER()`, `COALESCE`, `CONCAT_WS`, `HASHBYTES`.
  - Metadata-driven logic: Pulls transformation rules from `Rules.SemanticLayerMetaData`.

- **Dependencies:**
  - Relies on multiple semantic and warehouse tables.
  - Uses metadata table for transformation logic.
  - Performance tuning via index management and dynamic SQL.

# 3. Data Flow and Processing Logic

- **Data Flow:**
  1. Normalize input parameters.
  2. Check and disable indexes if target table is empty.
  3. Drop/recreate temp tables for staging.
  4. Load raw claim transaction data into temp tables.
  5. Enrich data by joining with descriptors and metadata tables.
  6. Apply business rules and transformations.
  7. Calculate hash values for change detection.
  8. Identify and flag inserted/updated records.
  9. Output final processed data for reporting.

- **Source Tables:**
  - `Semantic.ClaimTransactionMeasures`
  - `EDSWH.dbo.FactClaimTransactionLineWC`
  - `edswh.dbo.dimClaimTransactionWC`
  - `Semantic.ClaimTransactionDescriptors`
  - `Semantic.ClaimDescriptors`
  - `Semantic.PolicyDescriptors`
  - `edswh.dbo.dimBrand`
  - `Semantic.PolicyRiskStateDescriptors`
  - `Rules.SemanticLayerMetaData`

- **Destination Tables:**
  - Dynamic temp tables (e.g., `##CTM<spid>`, `##CTMFact<spid>`, `##CTMF<spid>`, etc.)

- **Transformations:**
  - Filtering by date and retired indicator.
  - Joins for enrichment and validation.
  - Aggregations and calculations via metadata-driven logic.
  - Hashing for audit/change tracking.

# 4. Data Mapping

| Target Table Name | Target Column Name | Source Table Name | Source Column Name | Remarks |
|-------------------|--------------------|-------------------|--------------------|---------|
| ##CTM<spid>       | Various Measures   | FactClaimTransactionLineWC, ClaimTransactionDescriptors, ClaimDescriptors, PolicyDescriptors, dimBrand, PolicyRiskStateDescriptors | Various | 1-to-1 and transformation rules from Rules.SemanticLayerMetaData |
| ##CTMF<spid>      | AuditOperations, InsertUpdates, HashValue | ##CTM<spid>, ##PRDCLmTrans<spid> | Various | Hashing and audit logic applied |
| ##CTMFact<spid>   | FactClaimTransactionLineWCKey, RevisionNumber, PolicyWCKey, etc. | EDSWH.dbo.FactClaimTransactionLineWC | Various | 1-to-1 mapping |
| ##CTPrs<spid>     | PolicyWCKey, RiskState, RetiredInd, etc. | Semantic.PolicyRiskStateDescriptors | Various | Filtered by RetiredInd = 0, latest record per PolicyWCKey/RiskState |
| ##PRDCLmTrans<spid> | FactClaimTransactionLineWCKey, RevisionNumber, HashValue, LoadCreateDate | Semantic.ClaimTransactionMeasures | Various | Used for audit comparison |

# 5. Complexity Analysis

| Category                | Measurement |
|-------------------------|------------|
| Number of Lines         | ~250 |
| Tables Used             | 10+ |
| Joins                   | 8 (INNER JOIN, LEFT JOIN) |
| Temporary Tables        | 5+ (dynamic global temp tables) |
| Aggregate Functions     | 0 (aggregation logic via metadata, but not explicit SUM/AVG) |
| DML Statements          | SELECT INTO, dynamic SELECT, INSERT, UPDATE |
| Conditional Logic       | Multiple CASE, IF blocks |
| Query Complexity        | High (multiple joins, subqueries, dynamic SQL, metadata-driven logic) |
| Performance Considerations | Index disabling, dynamic temp tables, NOCOUNT, XACT_ABORT |
| Data Volume Handling    | Designed for bulk ETL, handles large record sets |
| Dependency Complexity   | High (metadata table, multiple semantic/warehouse tables) |
| Overall Complexity Score| 85 |

# 6. Key Outputs

- **Final Outputs:**
  - Processed claim transaction measures with audit flags and hash values.
  - Output format: Result set from temp table `##CTMF<spid>` containing all enriched and transformed columns.
  - Includes audit columns (`InsertUpdates`, `AuditOperations`, `HashValue`, `LoadUpdateDate`, `LoadCreateDate`).

- **Business Alignment:**
  - Enables accurate, auditable reporting for claims analytics.
  - Supports regulatory and operational reporting needs.
  - Provides change tracking for incremental ETL and data warehousing.

- **Storage Format:**
  - Staging via global temp tables.
  - Final output as result set for downstream ETL or reporting.

# 7. apiCost

apiCost: 0.004 USD
