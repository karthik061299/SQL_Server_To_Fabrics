            -- Gross Paid Measures
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Indemnity' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossPaidIndemnity,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Medical' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossPaidMedical,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Expense' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossPaidExpense,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'EmployerLiability' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossPaidEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Legal' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossPaidLegal,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType IN ('Indemnity', 'Medical', 'EmployerLiability') 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossPaidLoss,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossPaidLossAndExpense,
            
            -- Gross Incurred Measures
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Indemnity' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossIncurredIndemnity,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Medical' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossIncurredMedical,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Expense' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossIncurredExpense,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'EmployerLiability' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossIncurredEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Legal' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossIncurredLegal,
            CASE WHEN ClaimTransactionDescriptors.CoverageType IN ('Indemnity', 'Medical', 'EmployerLiability') 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS GrossIncurredLoss,
            FactClaimTransactionLineWC.TransactionAmount AS GrossIncurredLossAndExpense,
            
            -- Recovery Measures
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Indemnity' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryIndemnity,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Medical' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryMedical,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Expense' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryExpense,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'EmployerLiability' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Legal' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryLegal,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Deductible' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryDeductible,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Overpayment' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryOverpayment,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'Subrogation' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoverySubrogation,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'ApportionmentContribution' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryApportionmentContribution,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.RecoveryType = 'SecondInjuryFund' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoverySecondInjuryFund,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                  AND ClaimTransactionDescriptors.CoverageType IN ('Indemnity', 'Medical', 'EmployerLiability') 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryLoss,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Recovery' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS RecoveryLossAndExpense