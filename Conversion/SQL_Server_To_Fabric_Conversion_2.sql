_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server stored procedure uspSemanticClaimTransactionMeasuresData converted to Fabric SQL with improved structure preservation
## *Version*: 2 
## *Updated on*: 
_____________________________________________

-- Microsoft Fabric SQL version of uspSemanticClaimTransactionMeasuresData
-- This conversion maintains the original structure while adapting to Fabric SQL requirements

CREATE OR ALTER PROCEDURE [Semantic].[uspSemanticClaimTransactionMeasuresData]
(
    @pJobStartDateTime datetime2
  , @pJobEndDateTime datetime2
)
AS
BEGIN
    -- Temporary table naming - using Fabric-compatible temp tables with session-specific naming
    DECLARE @TabName     varchar(100)
          , @TabFinal    varchar(100)
          , @TabNameFact varchar(100)
          , @TabNamePrs  varchar(100)
          , @ProdSource  varchar(100);
    
    -- In Fabric SQL, we use session-specific temporary tables instead of global temp tables
    -- Original: ##CTM + cast(@@spid as varchar(10))
    -- Fabric approach: Use regular tables with session-specific naming
    SELECT @TabName = 'CTM_' + REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '');
    SELECT @TabNameFact = 'CTMFact_' + REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '');
    SELECT @TabFinal = 'CTMF_' + REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '');
    SELECT @TabNamePrs = 'CTPrs_' + REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '');
    SELECT @ProdSource = 'PRDCLmTrans_' + REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '');
    
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;
    
    -- Handle special date case
    IF @pJobStartDateTime = '01/01/1900'
    BEGIN
        SET @pJobStartDateTime = '01/01/1700';
    END;
    
    -- Error handling variables
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    
    -- Check if ClaimTransactionMeasures table has data
    DECLARE @CATCount bigint = 0;
    
    -- Fabric-compatible way to get row count
    SELECT @CATCount = COUNT(*) FROM Semantic.ClaimTransactionMeasures;

    BEGIN TRY
        -- Drop temporary tables if they exist
        EXECUTE('DROP TABLE IF EXISTS ' + @TabName);
        EXECUTE('DROP TABLE IF EXISTS ' + @ProdSource);
        EXECUTE('DROP TABLE IF EXISTS ' + @TabFinal);
        EXECUTE('DROP TABLE IF EXISTS ' + @TabNameFact);
        EXECUTE('DROP TABLE IF EXISTS ' + @TabNamePrs);
        
        -- Create temporary tables and populate data
        -- Using dynamic SQL to create tables with dynamic names
        
        DECLARE @Select_SQL_Query nvarchar(max);
        
        -- Create temporary table for ClaimTransactionMeasures
        SET @Select_SQL_Query = '
        CREATE TABLE ' + @ProdSource + ' AS
        SELECT FactClaimTransactionLineWCKey, RevisionNumber, HashValue, LoadCreateDate
        FROM Semantic.ClaimTransactionMeasures;
        ';
        EXECUTE(@Select_SQL_Query);
        
        -- Create temporary table for PolicyRiskStateDescriptors
        SET @Select_SQL_Query = '
        CREATE TABLE ' + @TabNamePrs + ' AS
        SELECT * FROM
        (
            SELECT prs.*
                , ROW_NUMBER() OVER(PARTITION BY prs.PolicyWCKey, prs.RiskState 
                    ORDER BY prs.RetiredInd, prs.RiskStateEffectiveDate DESC, 
                    prs.RecordEffectiveDate DESC, prs.LoadUpdateDate DESC, 
                    prs.PolicyRiskStateWCKey DESC) AS Rownum
            FROM Semantic.PolicyRiskStateDescriptors prs 
            WHERE prs.retiredind = 0
        ) s 
        WHERE Rownum = 1
        ORDER BY PolicyWCKey;
        ';
        EXECUTE(@Select_SQL_Query);
        
        -- Create temporary table for FactClaimTransactionLineWC
        -- Using HASH JOIN hint for better performance in Fabric
        SET @Select_SQL_Query = '
        CREATE TABLE ' + @TabNameFact + ' AS
        SELECT DISTINCT 
            FactClaimTransactionLineWC.FactClaimTransactionLineWCKey
            ,FactClaimTransactionLineWC.RevisionNumber
            ,FactClaimTransactionLineWC.PolicyWCKey
            ,FactClaimTransactionLineWC.ClaimWCKey
            ,FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey
            ,FactClaimTransactionLineWC.ClaimTransactionWCKey
            ,FactClaimTransactionLineWC.ClaimCheckKey
            ,FactClaimTransactionLineWC.SourceTransactionLineItemCreateDate
            ,FactClaimTransactionLineWC.SourceTransactionLineItemCreateDateKey
            ,FactClaimTransactionLineWC.SourceSystem
            ,FactClaimTransactionLineWC.RecordEffectiveDate
            ,CONCAT_WS(''~'',FactClaimTransactionLineWC.FactClaimTransactionLineWCKey,FactClaimTransactionLineWC.RevisionNumber) SourceSystemIdentifier
            ,FactClaimTransactionLineWC.TransactionAmount
            ,FactClaimTransactionLineWC.LoadUpdateDate
            ,FactClaimTransactionLineWC.Retiredind
        FROM EDSWH.dbo.FactClaimTransactionLineWC 
        INNER HASH JOIN EDSWH.dbo.dimClaimTransactionWC t
            ON FactClaimTransactionLineWC.ClaimTransactionWCKey = t.ClaimTransactionWCKey
        WHERE 
            FactClaimTransactionLineWC.LoadUpdateDate >= ''' + CONVERT(VARCHAR, @pJobStartDateTime, 121) + ''' 
            OR t.LoadUpdateDate >= ''' + CONVERT(VARCHAR, @pJobStartDateTime, 121) + ''';
        ';
        EXECUTE(@Select_SQL_Query);
        
        -- Get the measure SQL from Rules.SemanticLayerMetaData
        -- Using STRING_AGG for better performance in Fabric
        DECLARE @Measure_SQL_Query varchar(max);
        
        SELECT @Measure_SQL_Query = STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT(Logic, ' AS ', Measure_Name)), ',') 
        WITHIN GROUP (ORDER BY Measure_Name ASC)
        FROM Rules.SemanticLayerMetaData
        WHERE SourceType = 'Claims';
        
        -- Create main temporary table with all data
        SET @Select_SQL_Query = '
        CREATE TABLE ' + @TabName + ' AS
        SELECT DISTINCT
            FactClaimTransactionLineWC.FactClaimTransactionLineWCKey
            ,COALESCE(FactClaimTransactionLineWC.RevisionNumber,0) AS RevisionNumber
            ,FactClaimTransactionLineWC.PolicyWCKey
            ,COALESCE(rskState.PolicyRiskStateWCKey,-1) AS PolicyRiskStateWCKey
            ,FactClaimTransactionLineWC.ClaimWCKey
            ,FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey
            ,FactClaimTransactionLineWC.ClaimTransactionWCKey
            ,FactClaimTransactionLineWC.ClaimCheckKey
            ,COALESCE(polAgcy.AgencyKey,-1) AS AgencyKey
            ,FactClaimTransactionLineWC.SourceTransactionLineItemCreateDate AS SourceClaimTransactionCreateDate
            ,FactClaimTransactionLineWC.SourceTransactionLineItemCreateDateKey AS SourceClaimTransactionCreateDateKey
            ,ClaimTransactionDescriptors.SourceTransactionCreateDate AS TransactionCreateDate
            ,ClaimTransactionDescriptors.TransactionSubmitDate
            ,FactClaimTransactionLineWC.SourceSystem
            ,FactClaimTransactionLineWC.RecordEffectiveDate
            ,FactClaimTransactionLineWC.SourceSystemIdentifier
            ,FactClaimTransactionLineWC.TransactionAmount
            ,FactClaimTransactionLineWC.RetiredInd
            ,' + @Measure_SQL_Query + '
        FROM ' + @TabNameFact + ' AS FactClaimTransactionLineWC
        INNER HASH JOIN Semantic.ClaimTransactionDescriptors AS ClaimTransactionDescriptors
            ON factClaimTransactionLineWC.ClaimTransactionLineCategoryKey = ClaimTransactionDescriptors.ClaimTransactionLineCategoryKey
            AND factClaimTransactionLineWC.ClaimTransactionWCKey = ClaimTransactionDescriptors.ClaimTransactionWCKey
            AND FactClaimTransactionLineWC.ClaimWCKey = ClaimTransactionDescriptors.ClaimWCkey
        INNER HASH JOIN Semantic.ClaimDescriptors AS ClaimDescriptors
            ON factClaimTransactionLineWC.ClaimWCKey = ClaimDescriptors.ClaimWCKey
        LEFT HASH JOIN Semantic.PolicyDescriptors AS polAgcy 
            ON FactClaimTransactionLineWC.PolicyWCKey = polAgcy.PolicyWCKey
        LEFT HASH JOIN EDSWH.dbo.dimBrand BK 
            ON PolAgcy.BrandKey = BK.BrandKey
        LEFT HASH JOIN ' + @TabNamePrs + ' AS rskState 
            ON FactClaimTransactionLineWC.PolicyWCKey = rskState.PolicyWCKey
            AND COALESCE(ClaimDescriptors.EmploymentLocationState, ClaimDescriptors.JurisdictionState) = rskState.RiskState
        OPTION(LABEL = ''Semantic.uspSemanticClaimTransactionMeasuresData - Main Query'', MAXDOP 8);
        ';
        EXECUTE(@Select_SQL_Query);
        
        -- Drop the fact table as it's no longer needed
        EXECUTE('DROP TABLE IF EXISTS ' + @TabNameFact);
        
        -- Create final table with hash values and change tracking
        -- Using CTE for better readability and performance in Fabric
        SET @Select_SQL_Query = '
        CREATE TABLE ' + @TabFinal + ' AS
        WITH C1 AS (
            SELECT 
                CC.*
                ,CONVERT(NVARCHAR(512), HASHBYTES(''SHA2_512'', CONCAT_WS(''~'',FactClaimTransactionLineWCKey
                ,RevisionNumber,PolicyWCKey,PolicyRiskStateWCKey,ClaimWCKey,ClaimTransactionLineCategoryKey,ClaimTransactionWCKey,ClaimCheckKey,AgencyKey
                ,SourceClaimTransactionCreateDate,SourceClaimTransactionCreateDateKey,TransactionCreateDate,TransactionSubmitDate
                ,NetPaidIndemnity,NetPaidMedical,NetPaidExpense,NetPaidEmployerLiability,NetPaidLegal,NetPaidLoss
                ,NetPaidLossAndExpense,NetIncurredIndemnity,NetIncurredMedical,NetIncurredExpense,NetIncurredEmployerLiability,NetIncurredLegal
                ,NetIncurredLoss,NetIncurredLossAndExpense,ReservesIndemnity,ReservesMedical,ReservesExpense,ReservesEmployerLiability
                ,ReservesLegal,ReservesLoss,ReservesLossAndExpense,GrossPaidIndemnity,GrossPaidMedical,GrossPaidExpense,GrossPaidEmployerLiability
                ,GrossPaidLegal,GrossPaidLoss,GrossPaidLossAndExpense,GrossIncurredIndemnity,GrossIncurredMedical,GrossIncurredExpense
                ,GrossIncurredEmployerLiability,GrossIncurredLegal,GrossIncurredLoss,GrossIncurredLossAndExpense,RecoveryIndemnity,RecoveryMedical,RecoveryExpense,RecoveryEmployerLiability
                ,RecoveryLegal,RecoveryDeductible,RecoveryOverpayment,RecoverySubrogation,RecoveryApportionmentContribution,RecoverySecondInjuryFund
                ,RecoveryLoss,RecoveryLossAndExpense,RecoveryNonSubroNonSecondInjuryFundEmployerLiability,RecoveryNonSubroNonSecondInjuryFundExpense
                ,RecoveryNonSubroNonSecondInjuryFundIndemnity,RecoveryNonSubroNonSecondInjuryFundLegal,RecoveryNonSubroNonSecondInjuryFundMedical
                ,SourceSystem,SourceSystemIdentifier,TransactionAmount,RecoveryDeductibleEmployerLiability,RecoveryDeductibleExpense,RecoveryDeductibleIndemnity,RecoveryDeductibleMedical
                ,RecoveryDeductibleLegal,RetiredInd)), 1) AS HashValue
            FROM ' + @TabName + ' AS CC
        )
        SELECT * FROM (
            SELECT DISTINCT c.*
            ,CASE
                WHEN cl.FactClaimTransactionLineWCKey IS NULL THEN 1
                WHEN cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue <> cl.HashValue THEN 0
                ELSE 3
             END AS InsertUpdates
            ,CASE
                WHEN cl.FactClaimTransactionLineWCKey IS NULL THEN ''Inserted''
                WHEN cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue <> cl.HashValue THEN ''Updated''
                ELSE NULL
             END AS AuditOperations
            ,GETDATE() AS LoadUpdateDate
            ,COALESCE(cl.LoadCreateDate, GETDATE()) LoadCreateDate
            FROM C1 AS c
            LEFT HASH JOIN ' + @ProdSource + ' AS cl 
                ON c.FactClaimTransactionLineWCKey = cl.FactClaimTransactionLineWCKey
                AND c.RevisionNumber = cl.RevisionNumber
            WHERE 
                cl.FactClaimTransactionLineWCKey IS NULL
                OR (cl.FactClaimTransactionLineWCKey IS NOT NULL AND c.HashValue <> cl.HashValue)
        ) x
        OPTION(LABEL = ''Semantic.uspSemanticClaimTransactionMeasuresData - Final Table'', MAXDOP 8);
        ';
        EXECUTE(@Select_SQL_Query);
        
        -- Drop the intermediate table
        EXECUTE('DROP TABLE IF EXISTS ' + @TabName);
        
        -- Return the final result set
        SET @Select_SQL_Query = '
        SELECT 
            FactClaimTransactionLineWCKey
            ,RevisionNumber
            ,PolicyWCKey
            ,PolicyRiskStateWCKey
            ,ClaimWCKey
            ,ClaimTransactionLineCategoryKey
            ,ClaimTransactionWCKey
            ,ClaimCheckKey
            ,AgencyKey
            ,SourceClaimTransactionCreateDate
            ,SourceClaimTransactionCreateDateKey
            ,TransactionCreateDate
            ,TransactionSubmitDate
            ,SourceSystem
            ,RecordEffectiveDate
            ,SourceSystemIdentifier
            ,GrossIncurredEmployerLiability
            ,GrossIncurredExpense
            ,GrossIncurredIndemnity
            ,GrossIncurredLegal
            ,GrossIncurredLoss
            ,GrossIncurredLossAndExpense
            ,GrossIncurredMedical
            ,GrossPaidEmployerLiability
            ,GrossPaidExpense
            ,GrossPaidIndemnity
            ,GrossPaidLegal
            ,GrossPaidLoss
            ,GrossPaidLossAndExpense
            ,GrossPaidMedical
            ,NetIncurredEmployerLiability
            ,NetIncurredExpense
            ,NetIncurredIndemnity
            ,NetIncurredLegal
            ,NetIncurredLoss
            ,NetIncurredLossAndExpense
            ,NetIncurredMedical
            ,NetPaidEmployerLiability
            ,NetPaidExpense
            ,NetPaidIndemnity
            ,NetPaidLegal
            ,NetPaidLoss
            ,NetPaidLossAndExpense
            ,NetPaidMedical
            ,RecoveryApportionmentContribution
            ,RecoveryDeductible
            ,RecoveryEmployerLiability
            ,RecoveryExpense
            ,RecoveryIndemnity
            ,RecoveryLegal
            ,RecoveryLoss
            ,RecoveryLossAndExpense
            ,RecoveryMedical
            ,RecoveryOverpayment
            ,RecoverySecondInjuryFund
            ,RecoverySubrogation
            ,ReservesEmployerLiability
            ,ReservesExpense
            ,ReservesIndemnity
            ,ReservesLegal
            ,ReservesLoss
            ,ReservesLossAndExpense
            ,ReservesMedical
            ,RecoveryNonSubroNonSecondInjuryFundEmployerLiability
            ,RecoveryNonSubroNonSecondInjuryFundExpense
            ,RecoveryNonSubroNonSecondInjuryFundIndemnity
            ,RecoveryNonSubroNonSecondInjuryFundLegal
            ,RecoveryNonSubroNonSecondInjuryFundMedical
            ,TransactionAmount
            ,RecoveryDeductibleEmployerLiability
            ,RecoveryDeductibleExpense
            ,RecoveryDeductibleIndemnity
            ,RecoveryDeductibleMedical
            ,RecoveryDeductibleLegal
            ,HashValue
            ,RetiredInd
            ,InsertUpdates
            ,AuditOperations
            ,LoadUpdateDate
            ,LoadCreateDate
        FROM ' + @TabFinal + '
        OPTION(LABEL = ''Semantic.uspSemanticClaimTransactionMeasuresData - Final Result'', MAXDOP 8);
        ';
        EXECUTE(@Select_SQL_Query);
        
        -- Clean up temporary tables
        EXECUTE('DROP TABLE IF EXISTS ' + @TabName);
        EXECUTE('DROP TABLE IF EXISTS ' + @ProdSource);
        EXECUTE('DROP TABLE IF EXISTS ' + @TabFinal);
        EXECUTE('DROP TABLE IF EXISTS ' + @TabNamePrs);
    END TRY
    BEGIN CATCH
        -- Enhanced error handling for Fabric
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();
            
        -- Clean up any temporary tables in case of error
        BEGIN TRY
            EXECUTE('DROP TABLE IF EXISTS ' + @TabName);
            EXECUTE('DROP TABLE IF EXISTS ' + @ProdSource);
            EXECUTE('DROP TABLE IF EXISTS ' + @TabFinal);
            EXECUTE('DROP TABLE IF EXISTS ' + @TabNameFact);
            EXECUTE('DROP TABLE IF EXISTS ' + @TabNamePrs);
        END TRY
        BEGIN CATCH
            -- Ignore errors in cleanup
        END CATCH;
        
        -- Log error to Fabric's logging system
        EXEC [dbo].[LogError] 
            @ErrorMessage = @ErrorMessage,
            @ErrorSeverity = @ErrorSeverity,
            @ErrorState = @ErrorState,
            @ProcedureName = 'Semantic.uspSemanticClaimTransactionMeasuresData';
            
        -- Re-throw error with additional context
        THROW;
    END CATCH;
END;