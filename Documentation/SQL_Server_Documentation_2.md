_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server stored procedure for claim transaction measures data processing and analysis
## *Version*: 2 
## *Updated on*: 
_____________________________________________

# Documentation for Stored Procedure: uspSemanticClaimTransactionMeasuresData

## 1. Overview of Program

### Purpose
The stored procedure `uspSemanticClaimTransactionMeasuresData` is designed to extract, transform, and load data for claim transaction measures population in the Semantic layer. It processes claim transaction data from source systems, applies business rules and transformations, and prepares the data for analytical consumption. The procedure specifically focuses on workers' compensation (WC) claims data, as indicated by the table names and field references throughout the code.

### Enterprise Data Warehousing Alignment
This implementation aligns with enterprise data warehousing principles by:
- Implementing a semantic layer that translates raw data into business-meaningful metrics
- Supporting data integration from multiple source systems
- Providing consistent calculation of claim transaction measures
- Enabling efficient data access for reporting and analytics
- Implementing change data capture techniques for efficient processing
- Supporting incremental data loads based on date parameters

### Business Problem Addressed
The procedure addresses the business need for standardized claim transaction metrics that can be used for:
- Financial reporting and analysis of workers' compensation claims data
- Tracking of paid and incurred amounts across different claim categories
- Monitoring recovery amounts by various recovery types (subrogation, deductible, etc.)
- Supporting claim analytics and business intelligence
- Providing consistent metrics for risk assessment and underwriting

### Benefits
- Consistent calculation of claim metrics across the organization
- Improved data quality through standardized transformations
- Enhanced performance for claim transaction reporting
- Support for historical analysis through effective dating
- Efficient processing through change detection and incremental loads
- Comprehensive tracking of various financial aspects of claims

### SQL Server Components Summary
- **Stored Procedures**: Main procedure with dynamic SQL generation
- **Temporary Tables**: Multiple temp tables used for staging and processing
- **Indexes**: Management of multiple indexes for performance optimization
- **Dynamic SQL**: Extensive use of dynamic SQL for flexible query construction
- **Hash Functions**: SHA2_512 hashing for change detection
- **Common Table Expressions**: Used for data transformation and change detection

## 2. Code Structure and Design

### Structure
The stored procedure follows a multi-step process:
1. Parameter validation and initialization
2. Index management based on data volume
3. Temporary table creation and population
4. Dynamic SQL construction for measure calculations
5. Data extraction from source tables
6. Application of business rules and transformations
7. Change detection using hash values
8. Final result set generation

### Key Components

#### DDL Operations
- Creation of multiple temporary tables (`##CTM`, `##CTMFact`, `##CTMF`, `##CTPrs`, `##PRDCLmTrans`)
- Index management (disabling/enabling indexes for bulk operations)

#### DML Operations
- SELECT operations to extract data from source tables
- INSERT operations to populate temporary tables
- JOIN operations to combine data from multiple sources
- Dynamic SQL generation for measure calculations

#### Joins
- INNER JOINs for required relationships (claim transaction to descriptors)
- LEFT JOINs for optional relationships (policy risk state data, brand data)

#### Indexing
- Management of multiple indexes on the target table
- Conditional disabling/enabling of indexes based on data volume
- Strategic index management for performance optimization

#### Functions
- Aggregate functions for measure calculations
- Hash functions (SHA2_512) for change detection
- String functions (CONCAT_WS) for key generation
- Window functions (ROW_NUMBER) for data deduplication

### Primary SQL Server Components

#### Tables
- Source tables: 
  - `EDSWH.dbo.FactClaimTransactionLineWC`: Main fact table for claim transaction lines
  - `EDSWH.dbo.dimClaimTransactionWC`: Dimension table for claim transactions
  - `EDSWH.dbo.dimBrand`: Dimension table for brand information
- Semantic layer tables: 
  - `Semantic.ClaimTransactionDescriptors`: Descriptive data for claim transactions
  - `Semantic.ClaimDescriptors`: Descriptive data for claims
  - `Semantic.PolicyDescriptors`: Descriptive data for policies
  - `Semantic.PolicyRiskStateDescriptors`: Risk state data for policies
- Target table: 
  - `Semantic.ClaimTransactionMeasures`: Final output table for claim measures

#### Temporary Tables
- `##CTM` + spid: Main temporary table for transformed data
- `##CTMFact` + spid: Temporary table for fact data
- `##CTMF` + spid: Final temporary table with change detection
- `##CTPrs` + spid: Temporary table for policy risk state data
- `##PRDCLmTrans` + spid: Temporary table for existing production data

#### Common Table Expressions (CTEs)
- `C1`: CTE for hash value calculation and change detection

#### Dynamic SQL
- Dynamic construction of measure calculations from `Rules.SemanticLayerMetaData`
- Dynamic SQL for temporary table management

### Dependencies
- `Rules.SemanticLayerMetaData`: Contains measure definitions and calculation logic
- `Semantic.ClaimTransactionMeasures`: Target table for the processed data
- `Semantic.ClaimTransactionDescriptors`, `Semantic.ClaimDescriptors`, `Semantic.PolicyDescriptors`: Descriptor tables for dimension data
- System catalog views (`sys.tables`, `sys.sysindexes`, `sys.indexes`): Used for metadata operations

## 3. Data Flow and Processing Logic

### Data Flow
1. **Source Data Extraction**:
   - Extract claim transaction data from `EDSWH.dbo.FactClaimTransactionLineWC` and `EDSWH.dbo.dimClaimTransactionWC`
   - Extract policy risk state data from `Semantic.PolicyRiskStateDescriptors`
   - Extract existing production data from `Semantic.ClaimTransactionMeasures`

2. **Data Transformation**:
   - Join transaction data with descriptor tables
   - Apply business rules from `Rules.SemanticLayerMetaData`
   - Calculate measures for various claim transaction categories
   - Apply deduplication logic for policy risk state data

3. **Change Detection**:
   - Generate hash values for change detection using SHA2_512
   - Compare with existing production data
   - Mark records for insert or update
   - Track audit operations

4. **Data Loading**:
   - Return final result set for loading into `Semantic.ClaimTransactionMeasures`
   - Include audit fields for tracking changes

### Source and Destination Tables

#### Source Tables
- `EDSWH.dbo.FactClaimTransactionLineWC`: Contains claim transaction line items with financial amounts
- `EDSWH.dbo.dimClaimTransactionWC`: Contains claim transaction dimension data
- `Semantic.ClaimTransactionDescriptors`: Contains descriptive data for claim transactions
- `Semantic.ClaimDescriptors`: Contains descriptive data for claims
- `Semantic.PolicyDescriptors`: Contains descriptive data for policies
- `Semantic.PolicyRiskStateDescriptors`: Contains risk state data for policies
- `EDSWH.dbo.dimBrand`: Contains brand dimension data

#### Destination Table
- `Semantic.ClaimTransactionMeasures`: Target table for the processed claim transaction measures

### Transformations

1. **Filtering**:
   - Filter by load update date (`LoadUpdateDate >= @pJobStartDateTime`)
   - Filter policy risk state data to exclude retired records (`RetiredInd = 0`)
   - Filter for changed records based on hash value comparison

2. **Joins**:
   - Join transaction data with descriptor tables
   - Join policy data with risk state data based on policy key and risk state
   - Join policy data with brand information

3. **Aggregations**:
   - Apply measure calculations as defined in `Rules.SemanticLayerMetaData`
   - Calculate various financial metrics (paid, incurred, reserves, recoveries)

4. **Field Calculations**:
   - Calculate hash values for change detection
   - Determine insert/update operations
   - Set audit fields (LoadUpdateDate, LoadCreateDate)
   - Generate source system identifier

## 4. Data Mapping

| Target Table Name | Target Column Name | Source Table Name | Source Column Name | Remarks |
|-------------------|-------------------|-------------------|-------------------|----------|
| Semantic.ClaimTransactionMeasures | FactClaimTransactionLineWCKey | EDSWH.dbo.FactClaimTransactionLineWC | FactClaimTransactionLineWCKey | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | RevisionNumber | EDSWH.dbo.FactClaimTransactionLineWC | RevisionNumber | COALESCE(RevisionNumber, 0) |
| Semantic.ClaimTransactionMeasures | PolicyWCKey | EDSWH.dbo.FactClaimTransactionLineWC | PolicyWCKey | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | PolicyRiskStateWCKey | Semantic.PolicyRiskStateDescriptors | PolicyRiskStateWCKey | COALESCE(PolicyRiskStateWCKey, -1) |
| Semantic.ClaimTransactionMeasures | ClaimWCKey | EDSWH.dbo.FactClaimTransactionLineWC | ClaimWCKey | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | ClaimTransactionLineCategoryKey | EDSWH.dbo.FactClaimTransactionLineWC | ClaimTransactionLineCategoryKey | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | ClaimTransactionWCKey | EDSWH.dbo.FactClaimTransactionLineWC | ClaimTransactionWCKey | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | ClaimCheckKey | EDSWH.dbo.FactClaimTransactionLineWC | ClaimCheckKey | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | AgencyKey | Semantic.PolicyDescriptors | AgencyKey | COALESCE(AgencyKey, -1) |
| Semantic.ClaimTransactionMeasures | SourceClaimTransactionCreateDate | EDSWH.dbo.FactClaimTransactionLineWC | SourceTransactionLineItemCreateDate | 1:1 mapping with column rename |
| Semantic.ClaimTransactionMeasures | SourceClaimTransactionCreateDateKey | EDSWH.dbo.FactClaimTransactionLineWC | SourceTransactionLineItemCreateDateKey | 1:1 mapping with column rename |
| Semantic.ClaimTransactionMeasures | TransactionCreateDate | Semantic.ClaimTransactionDescriptors | SourceTransactionCreateDate | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | TransactionSubmitDate | Semantic.ClaimTransactionDescriptors | TransactionSubmitDate | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | SourceSystem | EDSWH.dbo.FactClaimTransactionLineWC | SourceSystem | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | RecordEffectiveDate | EDSWH.dbo.FactClaimTransactionLineWC | RecordEffectiveDate | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | SourceSystemIdentifier | EDSWH.dbo.FactClaimTransactionLineWC | FactClaimTransactionLineWCKey, RevisionNumber | CONCAT_WS('~', FactClaimTransactionLineWCKey, RevisionNumber) |
| Semantic.ClaimTransactionMeasures | TransactionAmount | EDSWH.dbo.FactClaimTransactionLineWC | TransactionAmount | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | RetiredInd | EDSWH.dbo.FactClaimTransactionLineWC | RetiredInd | 1:1 mapping |
| Semantic.ClaimTransactionMeasures | HashValue | Calculated | Multiple columns | SHA2_512 hash of all relevant columns |
| Semantic.ClaimTransactionMeasures | LoadUpdateDate | System | GETDATE() | Current timestamp |
| Semantic.ClaimTransactionMeasures | LoadCreateDate | Existing or System | COALESCE(existing.LoadCreateDate, GETDATE()) | Preserved or new timestamp |
| Semantic.ClaimTransactionMeasures | NetPaidIndemnity | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetPaidMedical | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetPaidExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetPaidEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetPaidLegal | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetPaidLoss | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetPaidLossAndExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetIncurredIndemnity | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetIncurredMedical | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetIncurredExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetIncurredEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetIncurredLegal | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetIncurredLoss | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | NetIncurredLossAndExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | ReservesIndemnity | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | ReservesMedical | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | ReservesExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | ReservesEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | ReservesLegal | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | ReservesLoss | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | ReservesLossAndExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossPaidIndemnity | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossPaidMedical | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossPaidExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossPaidEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossPaidLegal | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossPaidLoss | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossPaidLossAndExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossIncurredIndemnity | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossIncurredMedical | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossIncurredExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossIncurredEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossIncurredLegal | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossIncurredLoss | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | GrossIncurredLossAndExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryIndemnity | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryMedical | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryLegal | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryDeductible | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryOverpayment | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoverySubrogation | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryApportionmentContribution | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoverySecondInjuryFund | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryLoss | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryLossAndExpense | Rules.SemanticLayerMetaData | Calculated | Dynamic calculation based on rules |
| Semantic.ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Added in v0.5 (DAA-9476) |
| Semantic.ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundExpense | Rules.SemanticLayerMetaData | Calculated | Added in v0.5 (DAA-9476) |
| Semantic.ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundIndemnity | Rules.SemanticLayerMetaData | Calculated | Added in v0.5 (DAA-9476) |
| Semantic.ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundLegal | Rules.SemanticLayerMetaData | Calculated | Added in v0.5 (DAA-9476) |
| Semantic.ClaimTransactionMeasures | RecoveryNonSubroNonSecondInjuryFundMedical | Rules.SemanticLayerMetaData | Calculated | Added in v0.5 (DAA-9476) |
| Semantic.ClaimTransactionMeasures | RecoveryDeductibleEmployerLiability | Rules.SemanticLayerMetaData | Calculated | Added in v0.6 (DAA-13691) |
| Semantic.ClaimTransactionMeasures | RecoveryDeductibleExpense | Rules.SemanticLayerMetaData | Calculated | Added in v0.6 (DAA-13691) |
| Semantic.ClaimTransactionMeasures | RecoveryDeductibleIndemnity | Rules.SemanticLayerMetaData | Calculated | Added in v0.6 (DAA-13691) |
| Semantic.ClaimTransactionMeasures | RecoveryDeductibleMedical | Rules.SemanticLayerMetaData | Calculated | Added in v0.6 (DAA-13691) |
| Semantic.ClaimTransactionMeasures | RecoveryDeductibleLegal | Rules.SemanticLayerMetaData | Calculated | Added in v0.6 (DAA-13691) |

## 5. Complexity Analysis

| Category | Metric | Value |
|----------|--------|-------|
| Number of Lines | Count of lines in the SQL script | 450+ |
| Tables Used | Number of tables referenced | 9+ |
| Joins | Number of joins and types used | 5+ (2 INNER JOIN, 3 LEFT JOIN) |
| Temporary Tables | Number of Temp Tables or Table Variables | 5 |
| Aggregate Functions | Number of Aggregate Functions | 10+ (dynamically generated) |
| DML Statements | Number of DML statements by type | SELECT: 10+, INSERT: 5+, DROP: 5+ |
| Conditional Logic | Number of CASE WHEN statements, IF-ELSE blocks | 5+ (2 CASE statements, 9+ IF-ELSE blocks) |
| Query Complexity | Number of joins, subqueries, and CTEs | High (10+ joins, 1+ CTEs, multiple subqueries) |
| Performance Considerations | Query execution time and memory consumption | High (index management, temp tables, hash calculations) |
| Data Volume Handling | Number of records processed | High (enterprise-scale claim data) |
| Dependency Complexity | External dependencies | High (multiple descriptor tables, rules metadata) |
| Overall Complexity Score | Score from 0 to 100 | 85 |

## 6. Key Outputs

### Final Outputs

The primary output is a result set containing claim transaction measures data, which includes:

1. **Claim Transaction Identifiers**:
   - FactClaimTransactionLineWCKey
   - RevisionNumber
   - PolicyWCKey
   - ClaimWCKey
   - ClaimTransactionWCKey
   - ClaimCheckKey

2. **Financial Measures**:
   - Net Paid amounts (Indemnity, Medical, Expense, Employer Liability, Legal)
   - Net Incurred amounts (Indemnity, Medical, Expense, Employer Liability, Legal)
   - Gross Paid amounts (Indemnity, Medical, Expense, Employer Liability, Legal)
   - Gross Incurred amounts (Indemnity, Medical, Expense, Employer Liability, Legal)
   - Reserves (Indemnity, Medical, Expense, Employer Liability, Legal)
   - Recovery amounts by type:
     - Deductible (with breakouts by category added in v0.6)
     - Subrogation
     - Overpayment
     - Apportionment Contribution
     - Second Injury Fund
     - Non-Subrogation Non-Second Injury Fund (added in v0.5)

3. **Metadata**:
   - HashValue for change detection
   - InsertUpdates flag (1 for insert, 0 for update, 3 for no change)
   - AuditOperations description ('Inserted', 'Updated', or NULL)
   - LoadUpdateDate and LoadCreateDate for auditing

### Business Alignment

The outputs align with business goals by providing:

1. **Financial Reporting**:
   - Standardized claim transaction measures for financial reporting
   - Consistent calculation of paid, incurred, and recovery amounts
   - Detailed breakdowns by claim category (indemnity, medical, expense, etc.)

2. **Claims Analysis**:
   - Detailed breakdown of claim costs by category
   - Support for trend analysis and forecasting
   - Tracking of recovery efforts and their financial impact

3. **Operational Efficiency**:
   - Optimized data structure for reporting performance
   - Change detection to minimize data processing
   - Incremental processing based on date parameters

4. **Risk Management**:
   - Comprehensive view of claim costs for risk assessment
   - Tracking of reserves for financial planning
   - Analysis of recovery effectiveness

### Storage Format

The data is stored in the production table `Semantic.ClaimTransactionMeasures`, which serves as a semantic layer for business intelligence and reporting tools. The procedure uses temporary tables for intermediate processing before returning the final result set for loading into the production table. The semantic layer design enables:

1. **Consistent Reporting**: Standard measures across all reporting tools
2. **Performance**: Optimized structure for analytical queries
3. **Maintainability**: Centralized business logic in the semantic layer
4. **Flexibility**: Support for various reporting needs without duplicating logic

## 7. API Cost

API cost for this documentation generation: $0.0000