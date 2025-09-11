            -- Additional Recovery Measures (DAA-9476)
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'EmployerLiability'
                  AND ClaimTransactionDescriptors.RecoveryType NOT IN ('Subrogation', 'SecondInjuryFund')
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryNonSubroNonSecondInjuryFundEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Expense'
                  AND ClaimTransactionDescriptors.RecoveryType NOT IN ('Subrogation', 'SecondInjuryFund')
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryNonSubroNonSecondInjuryFundExpense,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
                  AND ClaimTransactionDescriptors.RecoveryType NOT IN ('Subrogation', 'SecondInjuryFund')
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryNonSubroNonSecondInjuryFundIndemnity,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Legal'
                  AND ClaimTransactionDescriptors.RecoveryType NOT IN ('Subrogation', 'SecondInjuryFund')
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryNonSubroNonSecondInjuryFundLegal,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Medical'
                  AND ClaimTransactionDescriptors.RecoveryType NOT IN ('Subrogation', 'SecondInjuryFund')
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryNonSubroNonSecondInjuryFundMedical,
            
            -- Recovery Deductible Breakouts (DAA-13691)
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Deductible'
                  AND ClaimTransactionDescriptors.CoverageType = 'EmployerLiability'
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryDeductibleEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Deductible'
                  AND ClaimTransactionDescriptors.CoverageType = 'Expense'
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryDeductibleExpense,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Deductible'
                  AND ClaimTransactionDescriptors.CoverageType = 'Indemnity'
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryDeductibleIndemnity,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Deductible'
                  AND ClaimTransactionDescriptors.CoverageType = 'Medical'
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryDeductibleMedical,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Deductible'
                  AND ClaimTransactionDescriptors.CoverageType = 'Legal'
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryDeductibleLegal
            
        FROM FactClaimTransactionData AS FactClaimTransactionLineWC
        INNER JOIN Semantic.ClaimTransactionDescriptors AS ClaimTransactionDescriptors
            ON FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey = ClaimTransactionDescriptors.ClaimTransactionLineCategoryKey
            AND FactClaimTransactionLineWC.ClaimTransactionWCKey = ClaimTransactionDescriptors.ClaimTransactionWCKey
            AND FactClaimTransactionLineWC.ClaimWCKey = ClaimTransactionDescriptors.ClaimWCKey
        INNER JOIN Semantic.ClaimDescriptors AS ClaimDescriptors
            ON FactClaimTransactionLineWC.ClaimWCKey = ClaimDescriptors.ClaimWCKey
        LEFT JOIN Semantic.PolicyDescriptors AS polAgcy 
            ON FactClaimTransactionLineWC.PolicyWCKey = polAgcy.PolicyWCKey
        LEFT JOIN EDSWH.dbo.DimBrand BK 
            ON polAgcy.BrandKey = BK.BrandKey
        LEFT JOIN PolicyRiskStateData AS rskState 
            ON FactClaimTransactionLineWC.PolicyWCKey = rskState.PolicyWCKey
            AND COALESCE(ClaimDescriptors.EmploymentLocationState, ClaimDescriptors.JurisdictionState) = rskState.RiskState
    )