_____________________________________________
-- *Author*: AAVA
-- *Created on*: 
-- *Description*: Converts SQL Server stored procedure uspSemanticClaimTransactionMeasuresData to Microsoft Fabric SQL format
-- *Version*: 1 
-- *Updated on*: 
_____________________________________________

/*
================================================================================================================================
-- Procedure Name: uspSemanticClaimTransactionMeasuresData_Fabric
-- Purpose: Get Data for ClaimMeasures population - Converted to Microsoft Fabric SQL
-- Database: EDSMart (Fabric Lakehouse)
-- Schema: Semantic
-- Original Create On: 04-NOV-2019
-- Fabric Conversion: Converted from SQL Server stored procedure
-- Execution Example: Execute as Fabric SQL script with parameters
-- Revision History:
-- Ver.# Updated By                        Updated Date    Change History  
----------------------------------------------------------------------------------------------------------------
-- 0.1 Created By Tammy Atkinson
-- 0.2 Perfected By Jack Tower
-- 0.21 Updated Tammy Atkinson             02/22/2021 Added Left HASH JOINs
-- 0.22 Tammy.Atkinson@afgroup.com         18-Feb-2021   Updated to Correct RetiredInd
-- 0.3  Tammy.Atkinson@afgroup.com         07-Jul-2021   Update to change @pJobStartDateTime= '01/01/1700' when it is '01/01/1900'
-- 0.4  Tammy.Atkinson@afgroup.com         02-Nov-2021   Update to add DimBrand For use in update to the Rules where Brand Is Required
-- 0.5  Tammy.Atkinson@afgroup.com         17-Jan-2023   DAA-9476 Add Additional Columns for RecoveryNonSubroNonSecondInjuryFund (5 Additional Columns added)
-- 0.6  Zach.Henrys@afgroup.com            17-Jul-2023   DAA-11838 TransactionAmount; DAA-13691 RecoveryDeductible breakouts
-- 0.7  Tammy.Atkinson@afgroup.com         26-Jul-2023   DAA-14404 Update Policy Riskstate Data pull to exclude retiredind=1 records
-- 1.0  AAVA Fabric Conversion             Fabric Migration - Converted to Microsoft Fabric SQL format
================================================================================================================================
*/

-- Microsoft Fabric SQL Conversion of uspSemanticClaimTransactionMeasuresData
-- Note: This conversion transforms the SQL Server stored procedure into Fabric-compatible SQL statements

-- Parameter Declaration (Fabric SQL uses DECLARE for variables)
DECLARE @pJobStartDateTime DATETIME2 = '2019-07-01';
DECLARE @pJobEndDateTime DATETIME2 = '2019-07-02';

-- Variable Declarations (Fabric compatible - using session-based naming)
DECLARE @TabName STRING = CONCAT('CTM_'