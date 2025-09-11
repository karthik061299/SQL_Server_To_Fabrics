_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server stored procedure for processing claim transaction measures data
## *Version*: 2 
## *Updated on*: 
_____________________________________________

# SQL Server Documentation: uspSemanticClaimTransactionMeasuresData

## 1. Overview of Program

### Purpose of the SQL Server Code
The stored procedure `uspSemanticClaimTransactionMeasuresData` is designed to process and transform insurance claim transaction data for the semantic layer in a data warehouse environment. It extracts raw claim transaction data from source tables, applies complex business rules to calculate various financial metrics, and loads the results into the semantic layer for business reporting and analysis. The procedure handles multiple financial categories related to insurance claims including paid amounts, incurred amounts, reserves, and recoveries across different claim types (indemnity, medical, expense, employer liability, and legal).

### Alignment with Enterprise Data Warehousing and Analytics
This implementation aligns with enterprise data warehousing principles by:
- Centralizing complex transformation logic in a single, maintainable stored procedure
- Supporting the semantic layer which provides business-friendly data access and standardized metrics
- Implementing incremental processing based on date ranges to optimize performance
- Maintaining data lineage and audit information through hash values and revision tracking
- Supporting historical data analysis through effective dating and versioning
- Enabling consistent financial reporting across the organization

### Business Problem and Benefits
The procedure addresses several critical business needs in insurance claims processing:

**Business Problems:**
- Need for standardized financial metrics across different claim types and categories
- Requirement to track claim financial changes over time
- Need to support complex financial analysis and reporting
- Ensuring data consistency for regulatory compliance reporting
- Managing large volumes of transaction data efficiently

**Benefits:**
- Standardized calculation of claim financial metrics using consistent business rules
- Improved data quality and consistency through centralized transformation logic
- Efficient incremental processing of claim data to minimize system load
- Enhanced reporting capabilities with pre-calculated financial measures
- Support for historical analysis through versioning and effective dating
- Reduced development time for downstream reporting applications
- Better decision-making through reliable financial metrics

### High-Level Summary of SQL Server Components
- **Stored Procedures**: The main component is the `uspSemanticClaimTransactionMeasuresData` stored procedure
- **Tables**: 
  - Source tables: EDSWH.dbo.FactClaimTransactionLineWC, EDSWH.dbo.dimClaimTransactionWC
  - Dimension tables: Semantic.ClaimTransactionDescriptors, Semantic.ClaimDescriptors, Semantic.PolicyDescriptors, Semantic.PolicyRiskStateDescriptors
  - Target table: Semantic.ClaimTransactionMeasures
- **Temporary Tables**: Creates several temporary tables (##CTM, ##CTMFact, ##CTMF, ##CTPrs, ##PRDCLmTrans) for intermediate processing
- **Dynamic SQL**: Uses dynamic SQL generation for flexible measure calculations
- **Indexes**: Manages indexes for performance optimization during bulk operations
- **Hash Functions**: Uses SHA2_512 for change detection and data integrity

## 2. Code Structure and Design

### Structure of the SQL Server Code
The stored procedure follows a well-structured approach with distinct processing phases:

1. **Initialization Phase**:
   - Parameter declarations (`@pJobStartDateTime`, `@pJobEndDateTime`)
   - Variable declarations for temporary table names and processing
   - Special handling for default date parameters

2. **Performance Optimization Phase**:
   - Index management (disabling indexes for bulk operations)
   - Creation of temporary tables for intermediate processing

3. **Data Extraction Phase**:
   - Retrieval of policy risk state information with row number filtering
   - Extraction of claim transaction data with date-based filtering

4. **Transformation Phase**:
   - Dynamic SQL generation for measure calculations based on rules
   - Joining of fact and dimension tables
   - Application of business rules for financial calculations

5. **Change Detection Phase**:
   - Hash value generation for change detection
   - Comparison with existing data to identify inserts and updates

6. **Result Generation Phase**:
   - Final result set generation with audit information
   - Output of transformed data

### Key Components
- **DDL Operations**: 
  - Creates multiple temporary tables for intermediate processing
  - Manages index states for performance optimization

- **DML Operations**: 
  - Uses SELECT INTO for temporary table population
  - Complex SELECT statements with multiple joins
  - Uses Common Table Expressions (CTEs) for intermediate results

- **Joins**: 
  - Multiple INNER JOINs between fact and dimension tables
  - LEFT JOINs for optional relationships
  - Hash-based joins for performance

- **Indexing**: 
  - Disables and rebuilds indexes for performance optimization during bulk operations
  - Uses multiple indexes on key fields for query performance

- **Dynamic SQL**: 
  - Generates SQL statements dynamically for measure calculations
  - Uses string aggregation for building complex SQL statements

### Primary SQL Server Components
- **Tables**: 
  - Source: EDSWH.dbo.FactClaimTransactionLineWC, EDSWH.dbo.dimClaimTransactionWC
  - Dimension: Semantic.ClaimTransactionDescriptors, Semantic.ClaimDescriptors, Semantic.PolicyDescriptors, Semantic.PolicyRiskStateDescriptors, EDSWH.dbo.dimBrand
  - Target: Semantic.ClaimTransactionMeasures
  - Rules: Rules.SemanticLayerMetaData (for dynamic measure generation)

- **Temporary Tables**: 
  - ##CTM - Main temporary table for transformed data
  - ##CTMFact - Temporary table for fact data
  - ##CTMF - Final temporary table with change detection
  - ##CTPrs - Temporary table for policy risk state data
  - ##PRDCLmTrans - Temporary table for existing production data

- **Dynamic SQL**: 
  - Used for generating measure calculations based on rules
  - Builds complex SQL statements dynamically

- **CTEs**: 
  - Used for change detection and filtering
  - Implements row numbering for latest record selection

### Dependencies and Performance Tuning
- **Dependencies**:
  - Relies on Rules.SemanticLayerMetaData for dynamic measure generation
  - Depends on dimension and fact tables in the EDSMart database
  - Requires specific table structure and relationships

- **Performance Tuning**:
  - Index management for bulk operations
  - Use of temporary tables for intermediate results
  - Hash-based change detection for efficient updates
  - Row number functions for filtering latest records
  - Dynamic SQL for optimized measure calculations
  - Incremental processing based on date parameters

## 3. Data Flow and Processing Logic

### Data Flow
1. **Source Data Extraction**:
   - Claim transaction data is extracted from FactClaimTransactionLineWC and related dimension tables
   - Data is filtered based on job date parameters for incremental processing
   - Policy risk state information is retrieved and filtered to get the latest records

2. **Data Transformation**:
   - Claim transaction data is joined with descriptors and policy information
   - Financial measures are calculated using dynamically generated SQL based on rules
   - Additional fields are derived including source system identifiers

3. **Change Detection**:
   - Hash values are generated for each record using SHA2_512
   - Results are compared with existing data to identify inserts and updates
   - Audit information is added including operation type and timestamps

4. **Result Generation**:
   - Final result set is generated with all calculated measures and audit information
   - Only new or changed records are included in the output

### Source and Destination Tables

**Source Tables and Fields:**
- **EDSWH.dbo.FactClaimTransactionLineWC**:
  - FactClaimTransactionLineWCKey (PK)
  - RevisionNumber
  - PolicyWCKey (FK)
  - ClaimWCKey (FK)
  - ClaimTransactionLineCategoryKey (FK)
  - ClaimTransactionWCKey (FK)
  - ClaimCheckKey (FK)
  - SourceTransactionLineItemCreateDate
  - SourceTransactionLineItemCreateDateKey
  - SourceSystem
  - RecordEffectiveDate
  - TransactionAmount
  - RetiredInd

- **EDSWH.dbo.dimClaimTransactionWC**:
  - ClaimTransactionWCKey (PK)
  - LoadUpdateDate

- **Semantic.ClaimTransactionDescriptors**:
  - ClaimTransactionLineCategoryKey (PK)
  - ClaimTransactionWCKey (PK)
  - ClaimWCKey (PK)
  - SourceTransactionCreateDate
  - TransactionSubmitDate

- **Semantic.ClaimDescriptors**:
  - ClaimWCKey (PK)
  - EmploymentLocationState
  - JurisdictionState

- **Semantic.PolicyDescriptors**:
  - PolicyWCKey (PK)
  - AgencyKey (FK)
  - BrandKey (FK)

- **Semantic.PolicyRiskStateDescriptors**:
  - PolicyRiskStateWCKey (PK)
  - PolicyWCKey (FK)
  - RiskState
  - RiskStateEffectiveDate
  - RecordEffectiveDate
  - RetiredInd

- **Rules.SemanticLayerMetaData**:
  - SourceType
  - Measure_Name
  - Logic

**Destination Table and Fields:**
- **Semantic.ClaimTransactionMeasures**:
  - FactClaimTransactionLineWCKey (PK)
  - RevisionNumber (PK)
  - PolicyWCKey
  - PolicyRiskStateWCKey
  - ClaimWCKey
  - ClaimTransactionLineCategoryKey
  - ClaimTransactionWCKey
  - ClaimCheckKey
  - AgencyKey
  - SourceClaimTransactionCreateDate
  - SourceClaimTransactionCreateDateKey
  - TransactionCreateDate
  - TransactionSubmitDate
  - SourceSystem
  - RecordEffectiveDate
  - SourceSystemIdentifier
  - Multiple financial measures (NetPaidIndemnity, GrossIncurredLoss, etc.)
  - TransactionAmount
  - RetiredInd
  - HashValue
  - InsertUpdates
  - AuditOperations
  - LoadUpdateDate
  - LoadCreateDate

### Transformations
- **Filtering**:
  - Excludes retired records (RetiredInd = 1) from policy risk state data
  - Filters based on job date parameters for incremental processing
  - Uses row numbering to select only the latest policy risk state records

- **Joins**:
  - Connects claim transactions to descriptors, policies, and risk states
  - Uses appropriate join types (INNER, LEFT) based on relationship requirements

- **Field Calculations**:
  - Derives SourceSystemIdentifier using CONCAT_WS function
  - Applies COALESCE for handling NULL values
  - Dynamically generates financial measure calculations based on rules

- **Hash Value Generation**:
  - Creates SHA2_512 hash values for change detection
  - Concatenates all relevant fields for comprehensive change detection

- **Audit Information**:
  - Adds InsertUpdates flag (1 for insert, 0 for update, 3 for unchanged)
  - Adds AuditOperations text ('Inserted', 'Updated', NULL)
  - Sets LoadUpdateDate to current timestamp
  - Preserves or sets LoadCreateDate appropriately

## 4. Data Mapping

| Target Table Name | Target Column Name | Source Table Name | Source Column Name | Remarks |
|-------------------|-------------------|-------------------|-------------------|----------|
| ClaimTransactionMeasures | FactClaimTransactionLineWCKey | FactClaimTransactionLineWC | FactClaimTransactionLineWCKey | 1:1 mapping, Primary Key |
| ClaimTransactionMeasures | RevisionNumber | FactClaimTransactionLineWC | RevisionNumber | COALESCE(RevisionNumber, 0), Part of composite key |
| ClaimTransactionMeasures | PolicyWCKey | FactClaimTransactionLineWC | PolicyWCKey | 1:1 mapping |
| ClaimTransactionMeasures | PolicyRiskStateWCKey | PolicyRiskStateDescriptors | PolicyRiskStateWCKey | Joined using PolicyWCKey and RiskState, COALESCE(PolicyRiskStateWCKey, -1) |
| ClaimTransactionMeasures | ClaimWCKey | FactClaimTransactionLineWC | ClaimWCKey | 1:1 mapping |
| ClaimTransactionMeasures | ClaimTransactionLineCategoryKey | FactClaimTransactionLineWC | ClaimTransactionLineCategoryKey | 1:1 mapping |
| ClaimTransactionMeasures | ClaimTransactionWCKey | FactClaimTransactionLineWC | ClaimTransactionWCKey | 1:1 mapping |
| ClaimTransactionMeasures | ClaimCheckKey | FactClaimTransactionLineWC | ClaimCheckKey | 1:1 mapping |
| ClaimTransactionMeasures | AgencyKey | PolicyDescriptors | AgencyKey | Joined using PolicyWCKey, COALESCE(AgencyKey, -1) |
| ClaimTransactionMeasures | SourceClaimTransactionCreateDate | FactClaimTransactionLineWC | SourceTransactionLineItemCreateDate | 1:1 mapping with renamed column |
| ClaimTransactionMeasures | SourceClaimTransactionCreateDateKey | FactClaimTransactionLineWC | SourceTransactionLineItemCreateDateKey | 1:1 mapping with renamed column |
| ClaimTransactionMeasures | TransactionCreateDate | ClaimTransactionDescriptors | SourceTransactionCreateDate | Joined using multiple keys |
| ClaimTransactionMeasures | TransactionSubmitDate | ClaimTransactionDescriptors | TransactionSubmitDate | Joined using multiple keys |
| ClaimTransactionMeasures | SourceSystem | FactClaimTransactionLineWC | SourceSystem | 1:1 mapping |
| ClaimTransactionMeasures | RecordEffectiveDate | FactClaimTransactionLineWC | RecordEffectiveDate | 1:1 mapping |
| ClaimTransactionMeasures | SourceSystemIdentifier | FactClaimTransactionLineWC | FactClaimTransactionLineWCKey, RevisionNumber | Derived using CONCAT_WS('~', FactClaimTransactionLineWCKey, RevisionNumber) |
| ClaimTransactionMeasures | TransactionAmount | FactClaimTransactionLineWC | TransactionAmount | 1:1 mapping |
| ClaimTransactionMeasures | RetiredInd | FactClaimTransactionLineWC | RetiredInd | 1:1 mapping |
| ClaimTransactionMeasures | NetPaidIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetPaidMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetPaidExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetPaidEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetPaidLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetPaidLoss | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetPaidLossAndExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetIncurredIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetIncurredMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetIncurredExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetIncurredEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetIncurredLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetIncurredLoss | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | NetIncurredLossAndExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | ReservesIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | ReservesMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | ReservesExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | ReservesEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | ReservesLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | ReservesLoss | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | ReservesLossAndExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossPaidIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossPaidMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossPaidExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossPaidEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossPaidLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossPaidLoss | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossPaidLossAndExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossIncurredIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossIncurredMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossIncurredExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossIncurredEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossIncurredLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossIncurredLoss | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | GrossIncurredLossAndExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryDeductible | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryOverpayment | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoverySubrogation | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryApportionmentContribution | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoverySecondInjuryFund | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryLoss | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryLossAndExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules |
| ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.5 |
| ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.5 |
| ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.5 |
| ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.5 |
| ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.5 |
| ClaimTransactionMeasures | RecoveryDeductibleEmployerLiability | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.7 |
| ClaimTransactionMeasures | RecoveryDeductibleExpense | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.7 |
| ClaimTransactionMeasures | RecoveryDeductibleIndemnity | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.7 |
| ClaimTransactionMeasures | RecoveryDeductibleMedical | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.7 |
| ClaimTransactionMeasures | RecoveryDeductibleLegal | Rules.SemanticLayerMetaData | Logic | Dynamically generated based on rules, added in v0.7 |
| ClaimTransactionMeasures | HashValue | N/A | N/A | Generated using SHA2_512 hash of concatenated values for change detection |
| ClaimTransactionMeasures | InsertUpdates | N/A | N/A | Derived flag: 1=insert, 0=update, 3=unchanged |
| ClaimTransactionMeasures | AuditOperations | N/A | N/A | Derived text: 'Inserted', 'Updated', or NULL |
| ClaimTransactionMeasures | LoadUpdateDate | N/A | N/A | Set to GETDATE() for current processing timestamp |
| ClaimTransactionMeasures | LoadCreateDate | ClaimTransactionMeasures | LoadCreateDate | COALESCE(existing LoadCreateDate, GETDATE()) to preserve original creation date |

## 5. Complexity Analysis

| Category | Measurement |
|----------|-------------|
| Number of Lines | 350+ lines of SQL code |
| Tables Used | 7 tables (FactClaimTransactionLineWC, dimClaimTransactionWC, ClaimTransactionDescriptors, ClaimDescriptors, PolicyDescriptors, PolicyRiskStateDescriptors, dimBrand) |
| Joins | 6+ joins (4 INNER JOINs, 3 LEFT JOINs) |
| Temporary Tables | 5 temporary tables (##CTM, ##CTMFact, ##CTMF, ##CTPrs, ##PRDCLmTrans) |
| Aggregate Functions | Multiple, dynamically generated from Rules.SemanticLayerMetaData |
| DML Statements | Multiple SELECT statements with complex joins, 1 dynamic INSERT via SELECT INTO |
| Conditional Logic | Multiple CASE statements for InsertUpdates and AuditOperations, IF-ELSE blocks for parameter handling and index management |
| Query Complexity | Very High - Dynamic SQL generation, multiple joins, CTEs, subqueries, and complex transformations |
| Performance Considerations | Index management for bulk operations, temporary tables for intermediate results, hash-based change detection, row numbering for filtering |
| Data Volume Handling | Designed for processing large volumes of claim transaction data with incremental processing based on date parameters |
| Dependency Complexity | High - Depends on Rules.SemanticLayerMetaData for dynamic measure generation, multiple source tables with specific relationships |
| Overall Complexity Score | 85/100 (Very High complexity due to dynamic SQL generation, complex transformations, performance optimizations, and extensive error handling) |

## 6. Key Outputs

### Final Outputs
- **Table**: Populated Semantic.ClaimTransactionMeasures with transformed and enriched claim transaction measures
- **Result Set**: Returns the final result set with all calculated measures and audit information
- **Audit Trail**: Provides detailed audit information including operation type and timestamps

### Business Goal Alignment
The outputs of this stored procedure directly support several critical business needs:

1. **Financial Reporting**:
   - Provides standardized financial metrics for insurance claims
   - Enables consistent reporting across different business units
   - Supports regulatory compliance reporting requirements

2. **Performance Analysis**:
   - Allows tracking of claim costs over time
   - Enables analysis of claim settlement efficiency
   - Supports identification of cost trends and patterns

3. **Risk Management**:
   - Provides data for risk assessment and pricing models
   - Enables analysis of claim severity and frequency
   - Supports reserve adequacy analysis

4. **Operational Efficiency**:
   - Centralizes complex calculations to reduce redundant processing
   - Provides pre-calculated metrics for downstream applications
   - Improves data consistency across reporting systems

### Storage Format
- **Production Table**: Permanent table in the semantic layer (Semantic.ClaimTransactionMeasures) for long-term storage
- **Dimensional Model**: Follows a star schema design with measures and dimension keys
- **Versioning**: Supports versioning through RevisionNumber and effective dating
- **Audit Information**: Includes comprehensive audit fields for tracking changes
- **Change Detection**: Uses hash values for efficient change detection

## API Cost
API cost for this documentation: $0.00