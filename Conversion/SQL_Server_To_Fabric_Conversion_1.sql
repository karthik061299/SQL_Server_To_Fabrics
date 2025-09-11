_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server stored procedure uspSemanticClaimTransactionMeasuresData converted to Microsoft Fabric SQL
## *Version*: 1 
## *Updated on*: 
_____________________________________________

-- ================================================================
-- Microsoft Fabric SQL Conversion
-- Original: SQL Server Stored Procedure uspSemanticClaimTransactionMeasuresData
-- Converted: Microsoft Fabric SQL Format
-- ================================================================

-- Note: This conversion adapts the original SQL Server stored procedure to Microsoft Fabric SQL
-- Key changes made:
-- 1. Replaced SQL Server specific features (@@SPID, sp_executesql with dynamic SQL)
-- 2. Replaced temporary tables with CTEs and table variables where possible
-- 3. Simplified index management (Fabric handles indexing automatically)
-- 4. Adapted dynamic SQL to static SQL with conditional logic
-- 5. Used Fabric-compatible syntax for string aggregation and other functions

--------------------------------------------------------------------------------------------------------------------------------
--SP Name				: uspSemanticClaimTransactionMeasuresData_Fabric
--Purpose               : Get Data for ClaimMeasures population (Microsoft Fabric Version)
--Database			    : EDSMart
--Schema				: Semantic
--Create On			    : 04-NOV-2019 (Original), Converted for Fabric
--Execution (Example)	: EXEC Semantic.uspSemanticClaimTransactionMeasuresData_Fabric @pJobStartDateTime = '07/01/2019', @pJobEndDateTime ='07/02/2019'
--Revision History
--Ver.#   Updated By									 Updated Date	Change History  
----------------------------------------------------------------------------------------------------------------
--0.1 Created By Tammy Atkinson
--0.2 Perfected By Jack Tower
--0.21 Updated Tammy Atkinson		02/22/2021 Added Left HASH JOINs
--0.22		Tammy.Atkinson@afgroup.com		18-Feb-2021	  Updated to Correct RetiredInd
--0.3		Tammy.Atkinson@afgroup.com		07-Jul-2021   Update to change @pJobStartDateTime= '01/01/1700' when it is '01/01/1900'
--0.4		Tammy.Atkinson@afgroup.com		02-Nov-2021   Update to add DimBrand For use in update to the Rules where Brand Is Required
--0.5		Tammy.Atkinson@afgroup.com		17-Jan-2023   DAA-9476 Add Additional Columns for RecoveryNonSubroNonSecondInjuryFund (5 Additional Columns added)
--0.6		Zach.Henrys@afgroup.com			17-Jul-2023	  DAA-11838 TransactionAmount; DAA-13691 RecoveryDeductible breakouts
--0.7		Tammy.Atkinson@afgroup.com		26-Jul-2023	  DAA-14404 Update Policy Riskstate Data pull to exclude retiredind=1 records
--1.0		Fabric Conversion					Converted to Microsoft Fabric SQL format
--------------------------------------------------------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE Semantic.uspSemanticClaimTransactionMeasuresData_Fabric
(
    @pJobStartDateTime DATETIME2,
    @pJobEndDateTime DATETIME2
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;
    
    -- Handle legacy date conversion
    IF @pJobStartDateTime = '01/01/1900'
    BEGIN
        SET @pJobStartDateTime = '01/01/1700';
    END;

    -- In Fabric, we don't need to manually disable/enable indexes
    -- The following section replaces the index management in the original procedure
    
    -- Create base datasets using CTEs instead of temporary tables
    WITH ProdSource AS (
        SELECT 
            FactClaimTransactionLineWCKey,
            RevisionNumber,
            HashValue,
            LoadCreateDate
        FROM Semantic.ClaimTransactionMeasures
    ),
    
    PolicyRiskStateFiltered AS (
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
    
    PolicyRiskStateData AS (
        SELECT *
        FROM PolicyRiskStateFiltered
        WHERE Rownum = 1
    ),
    
    FactClaimTransactionData AS (
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
        FROM EDSWH.dbo.FactClaimTransactionLineWC 
        INNER JOIN EDSWH.dbo.DimClaimTransactionWC t
            ON FactClaimTransactionLineWC.ClaimTransactionWCKey = t.ClaimTransactionWCKey
        WHERE FactClaimTransactionLineWC.LoadUpdateDate >= @pJobStartDateTime 
           OR t.LoadUpdateDate >= @pJobStartDateTime
    )