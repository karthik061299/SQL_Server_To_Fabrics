_____________________________________________
-- *Author*: AAVA
-- *Created on*: 
-- *Description*: Conversion of uspSemanticClaimTransactionMeasuresData stored procedure from SQL Server to Microsoft Fabric SQL format
-- *Version*: 1 
-- *Updated on*: 
_____________________________________________

--------------------------------------------------------------------------------------------------------------------------------
-- Procedure Name: uspSemanticClaimTransactionMeasuresData_Fabric
-- Purpose: Get Data for ClaimMeasures population - Converted to Fabric SQL
-- Original Database: EDSMart
-- Original Schema: Semantic
-- Conversion Notes: 
--   - Replaced SQL Server specific functions with Fabric equivalents
--   - Converted dynamic SQL to static queries where possible
--   - Replaced temporary tables with CTEs and table variables
--   - Updated data types and syntax for Fabric compatibility
--------------------------------------------------------------------------------------------------------------------------------

-- Fabric SQL Conversion of uspSemanticClaimTransactionMeasuresData
-- Note: This conversion transforms the stored procedure into a Fabric-compatible script

-- Parameters (converted to variables for Fabric)
DECLARE @pJobStartDateTime DATETIME2 = '2019-07-01';
DECLARE @pJobEndDateTime DATETIME2 = '2019-07-02';

-- Variable declarations (Fabric compatible)
DECLARE @CATCount BIGINT = 0;

-- Handle date parameter conversion
IF @pJobStartDateTime = '1900-01-01'
BEGIN
    SET @pJobStartDateTime = '1700-01-01';
END;

-- Get table row count (Fabric equivalent)
SET @CATCount = (
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'ClaimTransactionMeasures' 
    AND TABLE_SCHEMA = 'Semantic'
);

-- Main data processing query (converted from dynamic SQL)
WITH PolicyRiskStateData AS (
    SELECT 
        prs.*,
        ROW_NUMBER() OVER(
            PARTITION BY prs.PolicyWCKey, prs.RiskState 
            ORDER BY prs.RetiredInd, prs.RiskStateEffectiveDate DESC, 
                     prs.RecordEffectiveDate DESC, prs.LoadUpdateDate DESC, 
                     prs.PolicyRiskStateWCKey DESC
        ) AS Rownum
    FROM Semantic.PolicyRiskStateDescriptors prs 
    WHERE prs.RetiredInd = 0
),
FilteredPolicyRiskState AS (
    SELECT * 
    FROM PolicyRiskStateData 
    WHERE Rownum = 1
),
FactClaimData AS (
    SELECT DISTINCT 
        FactClaimTransactionLineWC.FactClaimTransactionLineWCKey,
        FactClaimTransactionLineWC.RevisionNumber,
        FactClaimTransactionLineWC.PolicyWCKey,
        FactClaimTransactionLineWC.ClaimWCKey,
        FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey,
        FactClaimTransactionLineWC.ClaimTransactionWCKey,
        FactClaimTransactionLineWC.ClaimCheckKey,
        FactClaimTransactionLineWC.SourceTransactionLineItemCreateDate,
        FactClaimTransactionLineWC.SourceTransactionLineItemCreateDateKey,
        FactClaimTransactionLineWC.SourceSystem,
        FactClaimTransactionLineWC.RecordEffectiveDate,
        CONCAT(CAST(FactClaimTransactionLineWC.FactClaimTransactionLineWCKey AS STRING), '~', 
               CAST(FactClaimTransactionLineWC.RevisionNumber AS STRING)) AS SourceSystemIdentifier,
        FactClaimTransactionLineWC.TransactionAmount,
        FactClaimTransactionLineWC.LoadUpdateDate,
        FactClaimTransactionLineWC.RetiredInd
    FROM FactClaimTransactionLineWC 
    INNER JOIN DimClaimTransactionWC t
        ON FactClaimTransactionLineWC.ClaimTransactionWCKey = t.ClaimTransactionWCKey
    WHERE FactClaimTransactionLineWC.LoadUpdateDate >= @pJobStartDateTime 
       OR t.LoadUpdateDate >= @pJobStartDateTime
),
MainClaimData AS (
    SELECT DISTINCT
        FactClaimTransactionLineWC.FactClaimTransactionLineWCKey,
        COALESCE(FactClaimTransactionLineWC.RevisionNumber, 0) AS RevisionNumber,
        FactClaimTransactionLineWC.PolicyWCKey,
        COALESCE(rskState.PolicyRiskStateWCKey, -1) AS PolicyRiskStateWCKey,
        FactClaimTransactionLineWC.ClaimWCKey,
        FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey,
        FactClaimTransactionLineWC.ClaimTransactionWCKey,
        FactClaimTransactionLineWC.ClaimCheckKey,
        COALESCE(polAgcy.AgencyKey, -1) AS AgencyKey,
        FactClaimTransactionLineWC.SourceTransactionLineItemCreateDate AS SourceClaimTransactionCreateDate,
        FactClaimTransactionLineWC.SourceTransactionLineItemCreateDateKey AS SourceClaimTransactionCreateDateKey,
        ClaimTransactionDescriptors.SourceTransactionCreateDate AS TransactionCreateDate,
        ClaimTransactionDescriptors.TransactionSubmitDate,
        FactClaimTransactionLineWC.SourceSystem,
        FactClaimTransactionLineWC.RecordEffectiveDate,
        FactClaimTransactionLineWC.SourceSystemIdentifier,
        FactClaimTransactionLineWC.TransactionAmount,
        FactClaimTransactionLineWC.RetiredInd,
        
        -- Measure calculations (Fabric compatible)
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
             AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetPaidIndemnity,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
             AND ClaimTransactionDescriptors.CoverageType = 'Medical'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetPaidMedical,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
             AND ClaimTransactionDescriptors.CoverageType = 'Expense'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetPaidExpense,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
             AND ClaimTransactionDescriptors.CoverageType = 'EmployerLiability'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetPaidEmployerLiability,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
             AND ClaimTransactionDescriptors.CoverageType = 'Legal'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetPaidLegal,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Paid'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetPaidLoss,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Incurred' 
             AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetIncurredIndemnity,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Incurred' 
             AND ClaimTransactionDescriptors.CoverageType = 'Medical'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS NetIncurredMedical,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
             AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS ReservesIndemnity,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
             AND ClaimTransactionDescriptors.CoverageType = 'Medical'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS ReservesMedical,
             
        CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery'
             THEN FactClaimTransactionLineWC.TransactionAmount 
             ELSE 0 END AS RecoveryAmount
             
    FROM FactClaimData AS FactClaimTransactionLineWC
    INNER JOIN Semantic.ClaimTransactionDescriptors AS ClaimTransactionDescriptors
        ON FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey = ClaimTransactionDescriptors.ClaimTransactionLineCategoryKey
        AND FactClaimTransactionLineWC.ClaimTransactionWCKey = ClaimTransactionDescriptors.ClaimTransactionWCKey
        AND FactClaimTransactionLineWC.ClaimWCKey = ClaimTransactionDescriptors.ClaimWCKey
    INNER JOIN Semantic.ClaimDescriptors AS ClaimDescriptors
        ON FactClaimTransactionLineWC.ClaimWCKey = ClaimDescriptors.ClaimWCKey
    LEFT JOIN Semantic.PolicyDescriptors AS polAgcy 
        ON FactClaimTransactionLineWC.PolicyWCKey = polAgcy.PolicyWCKey
    LEFT JOIN DimBrand BK 
        ON polAgcy.BrandKey = BK.BrandKey
    LEFT JOIN FilteredPolicyRiskState AS rskState 
        ON FactClaimTransactionLineWC.PolicyWCKey = rskState.PolicyWCKey
        AND COALESCE(ClaimDescriptors.EmploymentLocationState, ClaimDescriptors.JurisdictionState) = rskState.RiskState
),
HashedData AS (
    SELECT 
        CC.*,
        -- Fabric compatible hash function
        SHA2(CONCAT(
            CAST(FactClaimTransactionLineWCKey AS STRING), '~',
            CAST(RevisionNumber AS STRING), '~',
            CAST(PolicyWCKey AS STRING), '~',
            CAST(PolicyRiskStateWCKey AS STRING), '~',
            CAST(ClaimWCKey AS STRING), '~',
            CAST(ClaimTransactionLineCategoryKey AS STRING), '~',
            CAST(ClaimTransactionWCKey AS STRING), '~',
            CAST(ClaimCheckKey AS STRING), '~',
            CAST(AgencyKey AS STRING), '~',
            CAST(SourceClaimTransactionCreateDate AS STRING), '~',
            CAST(TransactionAmount AS STRING), '~',
            CAST(RetiredInd AS STRING)
        ), 512) AS HashValue
    FROM MainClaimData AS CC
),
ExistingData AS (
    SELECT 
        FactClaimTransactionLineWCKey,
        RevisionNumber,
        HashValue,
        LoadCreateDate
    FROM Semantic.ClaimTransactionMeasures
),
FinalResults AS (
    SELECT DISTINCT
        c.*,
        CASE
            WHEN cl.FactClaimTransactionLineWCKey IS NULL THEN 1
            WHEN cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue <> cl.HashValue THEN 0
            ELSE 3
        END AS InsertUpdates,
        CASE
            WHEN cl.FactClaimTransactionLineWCKey IS NULL THEN 'Inserted'
            WHEN cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue <> cl.HashValue THEN 'Updated'
            ELSE NULL
        END AS AuditOperations,
        CURRENT_TIMESTAMP AS LoadUpdateDate,
        COALESCE(cl.LoadCreateDate, CURRENT_TIMESTAMP) AS LoadCreateDate
    FROM HashedData AS c
    LEFT JOIN ExistingData AS cl 
        ON c.FactClaimTransactionLineWCKey = cl.FactClaimTransactionLineWCKey
        AND c.RevisionNumber = cl.RevisionNumber
    WHERE cl.FactClaimTransactionLineWCKey IS NULL
       OR (cl.FactClaimTransactionLineWCKey IS NOT NULL 
           AND c.HashValue <> cl.HashValue)
)
-- Final SELECT statement for output
SELECT 
    FactClaimTransactionLineWCKey,
    RevisionNumber,
    PolicyWCKey,
    PolicyRiskStateWCKey,
    ClaimWCKey,
    ClaimTransactionLineCategoryKey,
    ClaimTransactionWCKey,
    ClaimCheckKey,
    AgencyKey,
    SourceClaimTransactionCreateDate,
    SourceClaimTransactionCreateDateKey,
    TransactionCreateDate,
    TransactionSubmitDate,
    SourceSystem,
    RecordEffectiveDate,
    SourceSystemIdentifier,
    NetPaidIndemnity,
    NetPaidMedical,
    NetPaidExpense,
    NetPaidEmployerLiability,
    NetPaidLegal,
    NetPaidLoss,
    NetIncurredIndemnity,
    NetIncurredMedical,
    ReservesIndemnity,
    ReservesMedical,
    RecoveryAmount,
    TransactionAmount,
    HashValue,
    RetiredInd,
    InsertUpdates,
    AuditOperations,
    LoadUpdateDate,
    LoadCreateDate
FROM FinalResults
ORDER BY FactClaimTransactionLineWCKey;

-- Conversion Notes:
-- 1. Replaced GETDATE() with CURRENT_TIMESTAMP
-- 2. Converted dynamic SQL to static CTEs for better performance
-- 3. Replaced CONCAT_WS with CONCAT for Fabric compatibility
-- 4. Updated HASHBYTES to SHA2 function
-- 5. Removed SQL Server specific system tables and functions
-- 6. Converted temporary tables to CTEs
-- 7. Updated string concatenation to use CONCAT function
-- 8. Replaced ISNULL with COALESCE
-- 9. Simplified schema references for Fabric
-- 10. Removed procedural elements and converted to set-based operations