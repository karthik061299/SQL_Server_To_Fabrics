_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server stored procedure for processing claim transaction measures data
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# SQL Server Documentation: uspSemanticClaimTransactionMeasuresData

## 1. Overview of Program

### Purpose of the SQL Server Code
The stored procedure `uspSemanticClaimTransactionMeasuresData` is designed to process and transform claim transaction data for the semantic layer in a data warehouse environment. It extracts, transforms, and loads claim transaction measures data, calculating various financial metrics related to insurance claims such as paid amounts, incurred amounts, reserves, and recoveries across different categories (indemnity, medical, expense, etc.).

### Alignment with Enterprise Data Warehousing and Analytics
This implementation aligns with enterprise data warehousing principles by:
- Centralizing complex transformation logic in a single, maintainable stored procedure
- Supporting the semantic layer which provides business-friendly data access
- Implementing incremental processing based on date ranges
- Maintaining data lineage and audit information through hash values and revision tracking
- Supporting historical data analysis through effective dating

### Business Problem and Benefits
The procedure addresses the business need to transform raw insurance claim transaction data into standardized financial measures that can be used for:
- Financial analysis and reporting of insurance claims
- Performance monitoring of claims processing
- Risk assessment and management
- Regulatory compliance and reporting

Benefits include:
- Standardized calculation of claim financial metrics
- Improved data quality and consistency
- Efficient incremental processing of claim data
- Enhanced reporting capabilities for business users
- Support for historical analysis through versioning

### High-Level Summary of SQL Server Components
- **Stored Procedures**: The main component is the `uspSemanticClaimTransactionMeasuresData` stored procedure
- **Tables**: References multiple source tables including fact tables (FactClaimTransactionLineWC) and dimension tables
- **Temporary Tables**: Creates several temporary tables for intermediate processing
- **Dynamic SQL**: Uses dynamic SQL generation for measure calculations
- **Indexes**: Manages indexes for performance optimization

## 2. Code Structure and Design

### Structure of the SQL Server Code
The stored procedure follows a structured approach:
1. Parameter declarations (`@pJobStartDateTime`, `@pJobEndDateTime`)
2. Variable declarations for temporary table names and processing
3. Index management for performance optimization
4. Dynamic SQL generation for measure calculations
5. Multi-step data processing using temporary tables
6. Hash value generation for change detection
7. Final result set generation with audit information

### Key Components
- **DDL**: Creates temporary tables for intermediate processing
- **DML**: Uses SELECT, INSERT operations for data transformation
- **Joins**: Multiple INNER and LEFT joins between fact and dimension tables
- **Indexing**: Disables and rebuilds indexes for performance optimization
- **Dynamic SQL**: Generates SQL statements dynamically for measure calculations
- **Hash Functions**: Uses SHA2_512 for change detection

### Primary SQL Server Components
- **Tables**: 
  - Source: EDSWH.dbo.FactClaimTransactionLineWC, Semantic.ClaimTransactionDescriptors, Semantic.ClaimDescriptors, Semantic.PolicyDescriptors, Semantic.PolicyRiskStateDescriptors
  - Target: Semantic.ClaimTransactionMeasures
- **Temporary Tables**: ##CTM, ##CTMFact, ##CTMF, ##CTPrs, ##PRDCLmTrans
- **Dynamic SQL**: Used for generating measure calculations
- **CTEs**: Used for change detection and filtering

### Dependencies and Performance Tuning
- Dependencies on dimension and fact tables in the EDSMart database
- Index management for performance optimization
- Use of temporary tables for intermediate results
- Hash-based change detection for efficient updates
- Row number functions for filtering latest records

## 3. Data Flow and Processing Logic

### Data Flow
1. Source data is extracted from FactClaimTransactionLineWC and related dimension tables
2. Data is filtered based on job date parameters
3. Policy risk state information is retrieved and filtered to get the latest records
4. Claim transaction data is joined with descriptors and policy information
5. Measures are calculated using dynamically generated SQL
6. Hash values are generated for change detection
7. Results are compared with existing data to identify inserts and updates
8. Final result set is generated with audit information

### Source and Destination Tables

**Source Tables and Fields:**
- EDSWH.dbo.FactClaimTransactionLineWC (FactClaimTransactionLineWCKey, RevisionNumber, PolicyWCKey, ClaimWCKey, etc.)
- Semantic.ClaimTransactionDescriptors (ClaimTransactionLineCategoryKey, ClaimTransactionWCKey, etc.)
- Semantic.ClaimDescriptors (ClaimWCKey, EmploymentLocationState, JurisdictionState, etc.)
- Semantic.PolicyDescriptors (PolicyWCKey, BrandKey, etc.)
- Semantic.PolicyRiskStateDescriptors (PolicyRiskStateWCKey, PolicyWCKey, RiskState, etc.)

**Destination Table and Fields:**
- Semantic.ClaimTransactionMeasures (FactClaimTransactionLineWCKey, RevisionNumber, PolicyWCKey, PolicyRiskStateWCKey, ClaimWCKey, etc.)

### Transformations
- **Filtering**: Excludes retired records and filters based on date parameters
- **Joins**: Connects claim transactions to descriptors, policies, and risk states
- **Aggregations**: Calculates various financial measures
- **Field Calculations**: Derives financial metrics like NetPaidIndemnity, GrossIncurredLoss, etc.
- **Hash Value Generation**: Creates hash values for change detection
- **Audit Information**: Adds audit fields like InsertUpdates, AuditOperations, LoadUpdateDate, LoadCreateDate

## 4. Data Mapping

| Target Table Name | Target Column Name | Source Table Name | Source Column Name | Remarks |
|-------------------|-------------------|-------------------|-------------------|----------|
| ClaimTransactionMeasures | FactClaimTransactionLineWCKey | FactClaimTransactionLineWC | FactClaimTransactionLineWCKey | 1:1 mapping |
| ClaimTransactionMeasures | RevisionNumber | FactClaimTransactionLineWC | RevisionNumber | 1:1 mapping with COALESCE(RevisionNumber, 0) |
| ClaimTransactionMeasures | PolicyWCKey | FactClaimTransactionLineWC | PolicyWCKey | 1:1 mapping |
| ClaimTransactionMeasures | PolicyRiskStateWCKey | PolicyRiskStateDescriptors | PolicyRiskStateWCKey | Joined using PolicyWCKey and RiskState, COALESCE(PolicyRiskStateWCKey, -1) |
| ClaimTransactionMeasures | ClaimWCKey | FactClaimTransactionLineWC | ClaimWCKey | 1:1 mapping |
| ClaimTransactionMeasures | ClaimTransactionLineCategoryKey | FactClaimTransactionLineWC | ClaimTransactionLineCategoryKey | 1:1 mapping |
| ClaimTransactionMeasures | ClaimTransactionWCKey | FactClaimTransactionLineWC | ClaimTransactionWCKey | 1:1 mapping |
| ClaimTransactionMeasures | ClaimCheckKey | FactClaimTransactionLineWC | ClaimCheckKey | 1:1 mapping |
| ClaimTransactionMeasures | AgencyKey | PolicyDescriptors | AgencyKey | Joined using PolicyWCKey, COALESCE(AgencyKey, -1) |
| ClaimTransactionMeasures | SourceClaimTransactionCreateDate | FactClaimTransactionLineWC | SourceTransactionLineItemCreateDate | 1:1 mapping |
| ClaimTransactionMeasures | SourceClaimTransactionCreateDateKey | FactClaimTransactionLineWC | SourceTransactionLineItemCreateDateKey | 1:1 mapping |
| ClaimTransactionMeasures | TransactionCreateDate | ClaimTransactionDescriptors | SourceTransactionCreateDate | Joined using multiple keys |
| ClaimTransactionMeasures | TransactionSubmitDate | ClaimTransactionDescriptors | TransactionSubmitDate | Joined using multiple keys |
| ClaimTransactionMeasures | SourceSystem | FactClaimTransactionLineWC | SourceSystem | 1:1 mapping |
| ClaimTransactionMeasures | RecordEffectiveDate | FactClaimTransactionLineWC | RecordEffectiveDate | 1:1 mapping |
| ClaimTransactionMeasures | SourceSystemIdentifier | FactClaimTransactionLineWC | FactClaimTransactionLineWCKey, RevisionNumber | Derived using CONCAT_WS('~', FactClaimTransactionLineWCKey, RevisionNumber) |
| ClaimTransactionMeasures | TransactionAmount | FactClaimTransactionLineWC | TransactionAmount | 1:1 mapping |
| ClaimTransactionMeasures | RetiredInd | FactClaimTransactionLineWC | RetiredInd | 1:1 mapping |
| ClaimTransactionMeasures | Various financial measures | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | HashValue | N/A | N/A | Generated using SHA2_512 hash of concatenated values |
| ClaimTransactionMeasures | InsertUpdates | N/A | N/A | Derived based on existence and hash comparison |
| ClaimTransactionMeasures | AuditOperations | N/A | N/A | Derived based on InsertUpdates value |
| ClaimTransactionMeasures | LoadUpdateDate | N/A | N/A | Set to GETDATE() |
| ClaimTransactionMeasures | LoadCreateDate | ClaimTransactionMeasures | LoadCreateDate | COALESCE(existing LoadCreateDate, GETDATE()) |

## 5. Complexity Analysis

| Category | Measurement |
|----------|-------------|
| Number of Lines | Approximately 350 lines |
| Tables Used | 5-6 tables (FactClaimTransactionLineWC, ClaimTransactionDescriptors, ClaimDescriptors, PolicyDescriptors, PolicyRiskStateDescriptors, SemanticLayerMetaData) |
| Joins | 5-6 joins (INNER JOIN, LEFT JOIN) |
| Temporary Tables | 5 temporary tables (##CTM, ##CTMFact, ##CTMF, ##CTPrs, ##PRDCLmTrans) |
| Aggregate Functions | Multiple, dynamically generated from Rules.SemanticLayerMetaData |
| DML Statements | Multiple SELECT statements, no explicit INSERT/UPDATE/DELETE |
| Conditional Logic | Multiple CASE statements, IF-ELSE blocks |
| Query Complexity | High - Dynamic SQL, multiple joins, CTEs, and subqueries |
| Performance Considerations | Index management, temporary tables, hash-based change detection |
| Data Volume Handling | Designed for processing large volumes of claim transaction data |
| Dependency Complexity | High - Depends on multiple source tables and Rules.SemanticLayerMetaData |
| Overall Complexity Score | 80/100 (High complexity due to dynamic SQL, multiple transformations, and performance optimizations) |

## 6. Key Outputs

### Final Outputs
- **Table**: Populated Semantic.ClaimTransactionMeasures with transformed and enriched claim transaction measures
- **Result Set**: Returns the final result set with all calculated measures and audit information

### Business Goal Alignment
- Supports financial reporting requirements for insurance claims
- Enables trend analysis of claims processing and financial impact
- Provides standardized metrics for risk assessment and management
- Supports compliance reporting requirements
- Enables data-driven decision making for claims management

### Storage Format
- **Production Table**: Permanent table in the semantic layer (Semantic.ClaimTransactionMeasures) for long-term storage
- **Dimensional Model**: Follows a star schema design with measures and dimension keys
- **Versioning**: Supports versioning through RevisionNumber
- **Audit Information**: Includes audit fields for tracking changes

## API Cost
API cost for this documentation: $0.00
