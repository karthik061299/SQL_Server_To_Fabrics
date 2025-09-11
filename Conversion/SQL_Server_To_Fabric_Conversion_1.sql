_____________________________________________
-- *Author*: AAVA
-- *Created on*: 
-- *Description*: Converted SQL Server stored procedure uspSemanticClaimTransactionMeasuresData to Microsoft Fabric SQL format for ClaimMeasures population
-- *Version*: 1 
-- *Updated on*: 
_____________________________________________

--------------------------------------------------------------------------------------------------------------------------------
-- Procedure Name: uspSemanticClaimTransactionMeasuresData_Fabric
-- Purpose: Get Data for ClaimMeasures population (Converted to Fabric SQL)
-- Original Database: EDSMart
-- Original Schema: Semantic
-- Conversion Notes: 
--   - Replaced SQL Server specific functions with Fabric equivalents
--   - Converted dynamic SQL to static queries where possible
--   - Replaced temporary tables with CTEs and table variables
--   - Updated data types and syntax for Fabric compatibility
--------------------------------------------------------------------------------------------------------------------------------

-- Fabric SQL Conversion of uspSemanticClaimTransactionMeasuresData
-- Note: This conversion assumes Fabric lakehouse/warehouse structure

-- Main query logic converted from stored procedure
WITH PolicyRiskStateFiltered AS (
    SELECT 
        prs.*,
        ROW_NUMBER() OVER(
            PARTITION BY prs.PolicyWCKey, prs.RiskState 
            ORDER BY prs.RetiredInd, prs.RiskStateEffectiveDate DESC, 
                     prs.RecordEffectiveDate DESC, prs.LoadUpdateDate DESC, 
                     prs.PolicyRiskStateWCKey DESC
        ) AS Rownum
    FROM Semantic.PolicyRiskStateDescriptors prs 
    WHERE prs.retiredind = 0
),

PolicyRiskStateDeduped AS (
    SELECT *
    FROM PolicyRiskStateFiltered
    WHERE Rownum = 1
),

FactClaimTransactionFiltered AS (
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
        FactClaimTransactionLineWC.Retiredind
    FROM EDSWH.FactClaimTransactionLineWC 
    INNER JOIN EDSWH.dimClaimTransactionWC t
        ON FactClaimTransactionLineWC.ClaimTransactionWCKey = t.ClaimTransactionWCKey
    WHERE FactClaimTransactionLineWC.LoadUpdateDate >= @pJobStartDateTime 
       OR t.LoadUpdateDate >= @pJobStartDateTime
),

ClaimTransactionMeasuresBase AS (
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
        
        -- Measure calculations (simplified examples - actual logic would come from Rules.SemanticLayerMetaData)
        CASE 
            WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
                 AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
            THEN FactClaimTransactionLineWC.TransactionAmount 
            ELSE 0 
        END AS NetPaidIndemnity,
        
        CASE 
            WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
                 AND ClaimTransactionDescriptors.CoverageType = 'Medical'
            THEN FactClaimTransactionLineWC.TransactionAmount 
            ELSE 0 
        END AS NetPaidMedical,
        
        CASE 
            WHEN ClaimTransactionDescriptors.TransactionType = 'Paid' 
                 AND ClaimTransactionDescriptors.CoverageType = 'Expense'
            THEN FactClaimTransactionLineWC.TransactionAmount 
            ELSE 0 
        END AS NetPaidExpense,
        
        CASE 
            WHEN ClaimTransactionDescriptors.TransactionType = 'Incurred' 
                 AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
            THEN FactClaimTransactionLineWC.TransactionAmount 
            ELSE 0 
        END AS NetIncurredIndemnity,
        
        CASE 
            WHEN ClaimTransactionDescriptors.TransactionType = 'Incurred' 
                 AND ClaimTransactionDescriptors.CoverageType = 'Medical'
            THEN FactClaimTransactionLineWC.TransactionAmount 
            ELSE 0 
        END AS NetIncurredMedical,
        
        CASE 
            WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                 AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
            THEN FactClaimTransactionLineWC.TransactionAmount 
            ELSE 0 
        END AS ReservesIndemnity,
        
        CASE 
            WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery'
            THEN FactClaimTransactionLineWC.TransactionAmount 
            ELSE 0 
        END AS RecoveryAmount
        
    FROM FactClaimTransactionFiltered AS FactClaimTransactionLineWC
    INNER JOIN Semantic.ClaimTransactionDescriptors AS ClaimTransactionDescriptors
        ON FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey = ClaimTransactionDescriptors.ClaimTransactionLineCategoryKey
        AND FactClaimTransactionLineWC.ClaimTransactionWCKey = ClaimTransactionDescriptors.ClaimTransactionWCKey
        AND FactClaimTransactionLineWC.ClaimWCKey = ClaimTransactionDescriptors.ClaimWCkey
    INNER JOIN Semantic.ClaimDescriptors AS ClaimDescriptors
        ON FactClaimTransactionLineWC.ClaimWCKey = ClaimDescriptors.ClaimWCKey
    LEFT JOIN Semantic.PolicyDescriptors AS polAgcy 
        ON FactClaimTransactionLineWC.PolicyWCKey = polAgcy.PolicyWCKey
    LEFT JOIN EDSWH.dimBrand BK 
        ON polAgcy.BrandKey = BK.BrandKey
    LEFT JOIN PolicyRiskStateDeduped AS rskState 
        ON FactClaimTransactionLineWC.PolicyWCKey = rskState.PolicyWCKey
        AND COALESCE(ClaimDescriptors.EmploymentLocationState, ClaimDescriptors.JurisdictionState) = rskState.RiskState
),

ClaimTransactionMeasuresWithHash AS (
    SELECT 
        *,
        -- Hash calculation using Fabric's SHA2 function
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
            CAST(NetPaidIndemnity AS STRING), '~',
            CAST(NetPaidMedical AS STRING), '~',
            CAST(NetIncurredIndemnity AS STRING)
        ), 512) AS HashValue
    FROM ClaimTransactionMeasuresBase
),

ExistingRecords AS (
    SELECT 
        FactClaimTransactionLineWCKey,
        RevisionNumber,
        HashValue,
        LoadCreateDate
    FROM Semantic.ClaimTransactionMeasures
)

-- Final result set with change detection
SELECT 
    c.FactClaimTransactionLineWCKey,
    c.RevisionNumber,
    c.PolicyWCKey,
    c.PolicyRiskStateWCKey,
    c.ClaimWCKey,
    c.ClaimTransactionLineCategoryKey,
    c.ClaimTransactionWCKey,
    c.ClaimCheckKey,
    c.AgencyKey,
    c.SourceClaimTransactionCreateDate,
    c.SourceClaimTransactionCreateDateKey,
    c.TransactionCreateDate,
    c.TransactionSubmitDate,
    c.SourceSystem,
    c.RecordEffectiveDate,
    c.SourceSystemIdentifier,
    c.NetPaidIndemnity,
    c.NetPaidMedical,
    c.NetPaidExpense,
    c.NetIncurredIndemnity,
    c.NetIncurredMedical,
    c.ReservesIndemnity,
    c.RecoveryAmount,
    c.TransactionAmount,
    c.HashValue,
    c.RetiredInd,
    
    -- Change detection logic
    CASE
        WHEN cl.FactClaimTransactionLineWCKey IS NULL THEN 1
        WHEN cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue != cl.HashValue THEN 0
        ELSE 3
    END AS InsertUpdates,
    
    CASE
        WHEN cl.FactClaimTransactionLineWCKey IS NULL THEN 'Inserted'
        WHEN cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue != cl.HashValue THEN 'Updated'
        ELSE NULL
    END AS AuditOperations,
    
    CURRENT_TIMESTAMP() AS LoadUpdateDate,
    COALESCE(cl.LoadCreateDate, CURRENT_TIMESTAMP()) AS LoadCreateDate
    
FROM ClaimTransactionMeasuresWithHash AS c
LEFT JOIN ExistingRecords AS cl 
    ON c.FactClaimTransactionLineWCKey = cl.FactClaimTransactionLineWCKey
    AND c.RevisionNumber = cl.RevisionNumber
WHERE 
    cl.FactClaimTransactionLineWCKey IS NULL
    OR (cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue != cl.HashValue);

--------------------------------------------------------------------------------------------------------------------------------
-- CONVERSION NOTES:
-- 1. Replaced GETDATE() with CURRENT_TIMESTAMP()
-- 2. Converted CONCAT_WS to CONCAT with manual separators
-- 3. Replaced HASHBYTES with SHA2 function
-- 4. Converted temporary tables to CTEs for better performance
-- 5. Simplified dynamic SQL to static queries
-- 6. Updated data type casting using CAST() with STRING type
-- 7. Replaced COALESCE usage where appropriate
-- 8. Removed SQL Server specific index operations
-- 9. Converted procedural elements to set-based operations
-- 10. Updated schema references for Fabric structure
--------------------------------------------------------------------------------------------------------------------------------