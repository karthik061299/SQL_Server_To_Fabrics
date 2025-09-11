_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Conversion of SQL Server stored procedure uspSemanticClaimTransactionMeasuresData to Fabric SQL
## *Version*: 1 
## *Updated on*: 
_____________________________________________

-- Fabric SQL conversion of [Semantic].[uspSemanticClaimTransactionMeasuresData]
-- Original stored procedure gets data for ClaimMeasures population

-- Note: This is a Fabric SQL script converted from SQL Server stored procedure
-- Some procedural elements have been adapted to Fabric's SQL dialect

--------------------------------------------------------------------------------------------------------------------------------
-- Original SP Name: uspSemanticClaimTransactionMeasuresData
-- Purpose: Get Data for ClaimMeasures population.
-- Database: EDSMart
-- Schema: Claim
-- Create On: 04-NOV-2019
-- Execution (Example): EXEC Policy.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime = '07/01/2019', @pJobEndDateTime ='07/02/2019'
--------------------------------------------------------------------------------------------------------------------------------

-- Parameters declaration
DECLARE @pJobStartDateTime DATETIME2;
DECLARE @pJobEndDateTime DATETIME2;

-- Set parameter values (these would typically be passed in when calling the script)
SET @pJobStartDateTime = '${JobStartDateTime}';
SET @pJobEndDateTime = '${JobEndDateTime}';

-- Handle special case for start date
IF @pJobStartDateTime = '01/01/1900'
BEGIN
    SET @pJobStartDateTime = '01/01/1700';
END;

-- Create temporary tables using Fabric's temporary table approach
-- Note: In Fabric, we use CREATE OR REPLACE TABLE instead of ##temp tables

-- Create temporary tables with unique names
CREATE OR REPLACE TABLE CTM AS
SELECT 1 AS dummy WHERE 1=0;

CREATE OR REPLACE TABLE CTMFact AS
SELECT 1 AS dummy WHERE 1=0;

CREATE OR REPLACE TABLE CTMF AS
SELECT 1 AS dummy WHERE 1=0;

CREATE OR REPLACE TABLE CTPrs AS
SELECT 1 AS dummy WHERE 1=0;

CREATE OR REPLACE TABLE PRDCLmTrans AS
SELECT 1 AS dummy WHERE 1=0;

-- Note: Index operations are not directly applicable in Fabric SQL
-- The following section would need to be handled differently in Fabric
-- Original code checked for indexes and disabled them if they existed

-- Populate PRDCLmTrans table
TRUNCATE TABLE PRDCLmTrans;
INSERT INTO PRDCLmTrans
SELECT FactClaimTransactionLineWCKey, RevisionNumber, HashValue, LoadCreateDate
FROM ClaimTransactionMeasures;

-- Populate CTPrs table with policy risk state data
TRUNCATE TABLE CTPrs;
INSERT INTO CTPrs
SELECT * FROM
(
    SELECT prs.*,
        ROW_NUMBER() OVER(PARTITION BY prs.PolicyWCKey, prs.RiskState 
                          ORDER BY prs.RetiredInd, prs.RiskStateEffectiveDate DESC, 
                                   prs.RecordEffectiveDate DESC, prs.LoadUpdateDate DESC, 
                                   prs.PolicyRiskStateWCKey DESC) AS Rownum
    FROM PolicyRiskStateDescriptors prs 
    WHERE prs.RetiredInd = 0
) s 
WHERE Rownum = 1
ORDER BY PolicyWCKey;

-- Populate CTMFact table
TRUNCATE TABLE CTMFact;
INSERT INTO CTMFact
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
    CONCAT(FactClaimTransactionLineWC.FactClaimTransactionLineWCKey, '~', FactClaimTransactionLineWC.RevisionNumber) AS SourceSystemIdentifier,
    FactClaimTransactionLineWC.TransactionAmount,
    FactClaimTransactionLineWC.LoadUpdateDate,
    FactClaimTransactionLineWC.RetiredInd
FROM FactClaimTransactionLineWC
INNER JOIN DimClaimTransactionWC t
    ON FactClaimTransactionLineWC.ClaimTransactionWCKey = t.ClaimTransactionWCKey
WHERE FactClaimTransactionLineWC.LoadUpdateDate >= @pJobStartDateTime
    OR t.LoadUpdateDate >= @pJobStartDateTime;

-- Populate main CTM table
TRUNCATE TABLE CTM;
INSERT INTO CTM
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
    -- Here would be all the measures from Rules.SemanticLayerMetaData
    -- In Fabric, we would need to explicitly list all these measures
    -- For demonstration purposes, we're showing a few example measures
    NetPaidIndemnity,
    NetPaidMedical,
    NetPaidExpense,
    NetPaidEmployerLiability,
    NetPaidLegal,
    NetPaidLoss,
    NetPaidLossAndExpense,
    NetIncurredIndemnity,
    NetIncurredMedical,
    NetIncurredExpense,
    NetIncurredEmployerLiability,
    NetIncurredLegal,
    NetIncurredLoss,
    NetIncurredLossAndExpense,
    ReservesIndemnity,
    ReservesMedical,
    ReservesExpense,
    ReservesEmployerLiability,
    ReservesLegal,
    ReservesLoss,
    ReservesLossAndExpense,
    GrossPaidIndemnity,
    GrossPaidMedical,
    GrossPaidExpense,
    GrossPaidEmployerLiability,
    GrossPaidLegal,
    GrossPaidLoss,
    GrossPaidLossAndExpense,
    GrossIncurredIndemnity,
    GrossIncurredMedical,
    GrossIncurredExpense,
    GrossIncurredEmployerLiability,
    GrossIncurredLegal,
    GrossIncurredLoss,
    GrossIncurredLossAndExpense,
    RecoveryIndemnity,
    RecoveryMedical,
    RecoveryExpense,
    RecoveryEmployerLiability,
    RecoveryLegal,
    RecoveryDeductible,
    RecoveryOverpayment,
    RecoverySubrogation,
    RecoveryApportionmentContribution,
    RecoverySecondInjuryFund,
    RecoveryLoss,
    RecoveryLossAndExpense,
    RecoveryNonSubroNonSecondInjuryFundEmployerLiability,
    RecoveryNonSubroNonSecondInjuryFundExpense,
    RecoveryNonSubroNonSecondInjuryFundIndemnity,
    RecoveryNonSubroNonSecondInjuryFundLegal,
    RecoveryNonSubroNonSecondInjuryFundMedical,
    RecoveryDeductibleEmployerLiability,
    RecoveryDeductibleExpense,
    RecoveryDeductibleIndemnity,
    RecoveryDeductibleMedical,
    RecoveryDeductibleLegal
FROM CTMFact AS FactClaimTransactionLineWC
INNER JOIN ClaimTransactionDescriptors
    ON FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey = ClaimTransactionDescriptors.ClaimTransactionLineCategoryKey
    AND FactClaimTransactionLineWC.ClaimTransactionWCKey = ClaimTransactionDescriptors.ClaimTransactionWCKey
    AND FactClaimTransactionLineWC.ClaimWCKey = ClaimTransactionDescriptors.ClaimWCKey
INNER JOIN ClaimDescriptors
    ON FactClaimTransactionLineWC.ClaimWCKey = ClaimDescriptors.ClaimWCKey
LEFT JOIN PolicyDescriptors AS polAgcy
    ON FactClaimTransactionLineWC.PolicyWCKey = polAgcy.PolicyWCKey
LEFT JOIN DimBrand BK 
    ON PolAgcy.BrandKey = BK.BrandKey
LEFT JOIN CTPrs AS rskState
    ON FactClaimTransactionLineWC.PolicyWCKey = rskState.PolicyWCKey
    AND COALESCE(ClaimDescriptors.EmploymentLocationState, ClaimDescriptors.JurisdictionState) = rskState.RiskState;

-- Create final result with hash values for change detection
TRUNCATE TABLE CTMF;
INSERT INTO CTMF
WITH C1 AS (
    SELECT 
        CC.*,
        -- In Fabric, we use SHA2_512 function with different syntax
        SHA2_512(CONCAT(
            CC.FactClaimTransactionLineWCKey, '~',
            CC.RevisionNumber, '~',
            CC.PolicyWCKey, '~',
            CC.PolicyRiskStateWCKey, '~',
            CC.ClaimWCKey, '~',
            CC.ClaimTransactionLineCategoryKey, '~',
            CC.ClaimTransactionWCKey, '~',
            CC.ClaimCheckKey, '~',
            CC.AgencyKey, '~',
            CC.SourceClaimTransactionCreateDate, '~',
            CC.SourceClaimTransactionCreateDateKey, '~',
            CC.TransactionCreateDate, '~',
            CC.TransactionSubmitDate, '~',
            CC.NetPaidIndemnity, '~',
            CC.NetPaidMedical, '~',
            CC.NetPaidExpense, '~',
            CC.NetPaidEmployerLiability, '~',
            CC.NetPaidLegal, '~',
            CC.NetPaidLoss, '~',
            CC.NetPaidLossAndExpense, '~',
            CC.NetIncurredIndemnity, '~',
            CC.NetIncurredMedical, '~',
            CC.NetIncurredExpense, '~',
            CC.NetIncurredEmployerLiability, '~',
            CC.NetIncurredLegal, '~',
            CC.NetIncurredLoss, '~',
            CC.NetIncurredLossAndExpense, '~',
            CC.ReservesIndemnity, '~',
            CC.ReservesMedical, '~',
            CC.ReservesExpense, '~',
            CC.ReservesEmployerLiability, '~',
            CC.ReservesLegal, '~',
            CC.ReservesLoss, '~',
            CC.ReservesLossAndExpense, '~',
            CC.GrossPaidIndemnity, '~',
            CC.GrossPaidMedical, '~',
            CC.GrossPaidExpense, '~',
            CC.GrossPaidEmployerLiability, '~',
            CC.GrossPaidLegal, '~',
            CC.GrossPaidLoss, '~',
            CC.GrossPaidLossAndExpense, '~',
            CC.GrossIncurredIndemnity, '~',
            CC.GrossIncurredMedical, '~',
            CC.GrossIncurredExpense, '~',
            CC.GrossIncurredEmployerLiability, '~',
            CC.GrossIncurredLegal, '~',
            CC.GrossIncurredLoss, '~',
            CC.GrossIncurredLossAndExpense, '~',
            CC.RecoveryIndemnity, '~',
            CC.RecoveryMedical, '~',
            CC.RecoveryExpense, '~',
            CC.RecoveryEmployerLiability, '~',
            CC.RecoveryLegal, '~',
            CC.RecoveryDeductible, '~',
            CC.RecoveryOverpayment, '~',
            CC.RecoverySubrogation, '~',
            CC.RecoveryApportionmentContribution, '~',
            CC.RecoverySecondInjuryFund, '~',
            CC.RecoveryLoss, '~',
            CC.RecoveryLossAndExpense, '~',
            CC.RecoveryNonSubroNonSecondInjuryFundEmployerLiability, '~',
            CC.RecoveryNonSubroNonSecondInjuryFundExpense, '~',
            CC.RecoveryNonSubroNonSecondInjuryFundIndemnity, '~',
            CC.RecoveryNonSubroNonSecondInjuryFundLegal, '~',
            CC.RecoveryNonSubroNonSecondInjuryFundMedical, '~',
            CC.SourceSystem, '~',
            CC.SourceSystemIdentifier, '~',
            CC.TransactionAmount, '~',
            CC.RecoveryDeductibleEmployerLiability, '~',
            CC.RecoveryDeductibleExpense, '~',
            CC.RecoveryDeductibleIndemnity, '~',
            CC.RecoveryDeductibleMedical, '~',
            CC.RecoveryDeductibleLegal, '~',
            CC.RetiredInd
        )) AS HashValue
    FROM CTM AS CC
)
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
FROM C1 AS c
LEFT JOIN PRDCLmTrans AS cl
    ON c.FactClaimTransactionLineWCKey = cl.FactClaimTransactionLineWCKey
    AND c.RevisionNumber = cl.RevisionNumber
WHERE cl.FactClaimTransactionLineWCKey IS NULL
    OR (cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue <> cl.HashValue);

-- Return final result
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
    GrossIncurredEmployerLiability,
    GrossIncurredExpense,
    GrossIncurredIndemnity,
    GrossIncurredLegal,
    GrossIncurredLoss,
    GrossIncurredLossAndExpense,
    GrossIncurredMedical,
    GrossPaidEmployerLiability,
    GrossPaidExpense,
    GrossPaidIndemnity,
    GrossPaidLegal,
    GrossPaidLoss,
    GrossPaidLossAndExpense,
    GrossPaidMedical,
    NetIncurredEmployerLiability,
    NetIncurredExpense,
    NetIncurredIndemnity,
    NetIncurredLegal,
    NetIncurredLoss,
    NetIncurredLossAndExpense,
    NetIncurredMedical,
    NetPaidEmployerLiability,
    NetPaidExpense,
    NetPaidIndemnity,
    NetPaidLegal,
    NetPaidLoss,
    NetPaidLossAndExpense,
    NetPaidMedical,
    RecoveryApportionmentContribution,
    RecoveryDeductible,
    RecoveryEmployerLiability,
    RecoveryExpense,
    RecoveryIndemnity,
    RecoveryLegal,
    RecoveryLoss,
    RecoveryLossAndExpense,
    RecoveryMedical,
    RecoveryOverpayment,
    RecoverySecondInjuryFund,
    RecoverySubrogation,
    ReservesEmployerLiability,
    ReservesExpense,
    ReservesIndemnity,
    ReservesLegal,
    ReservesLoss,
    ReservesLossAndExpense,
    ReservesMedical,
    RecoveryNonSubroNonSecondInjuryFundEmployerLiability,
    RecoveryNonSubroNonSecondInjuryFundExpense,
    RecoveryNonSubroNonSecondInjuryFundIndemnity,
    RecoveryNonSubroNonSecondInjuryFundLegal,
    RecoveryNonSubroNonSecondInjuryFundMedical,
    TransactionAmount,
    RecoveryDeductibleEmployerLiability,
    RecoveryDeductibleExpense,
    RecoveryDeductibleIndemnity,
    RecoveryDeductibleMedical,
    RecoveryDeductibleLegal,
    HashValue,
    RetiredInd,
    InsertUpdates,
    AuditOperations,
    LoadUpdateDate,
    LoadCreateDate
FROM CTMF;

-- Clean up temporary tables
DROP TABLE CTM;
DROP TABLE CTMFact;
DROP TABLE CTMF;
DROP TABLE CTPrs;
DROP TABLE PRDCLmTrans;
