    -- Main data processing with measures
    -- Note: In Fabric, we replace the dynamic SQL with static calculations
    ClaimTransactionMeasuresData AS (
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
            
            -- Net Paid Measures
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Indemnity' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetPaidIndemnity,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Medical' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetPaidMedical,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Expense' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetPaidExpense,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'EmployerLiability' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetPaidEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Legal' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetPaidLegal,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                  AND ClaimTransactionDescriptors.CoverageType IN ('Indemnity', 'Medical', 'EmployerLiability') 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetPaidLoss,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Payment' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetPaidLossAndExpense,
            
            -- Net Incurred Measures
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Indemnity' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetIncurredIndemnity,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Medical' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetIncurredMedical,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Expense' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetIncurredExpense,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'EmployerLiability' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetIncurredEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.CoverageType = 'Legal' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetIncurredLegal,
            CASE WHEN ClaimTransactionDescriptors.CoverageType IN ('Indemnity', 'Medical', 'EmployerLiability') 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS NetIncurredLoss,
            FactClaimTransactionLineWC.TransactionAmount AS NetIncurredLossAndExpense,
            
            -- Reserves Measures
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Indemnity' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS ReservesIndemnity,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Medical' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS ReservesMedical,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Expense' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS ReservesExpense,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                  AND ClaimTransactionDescriptors.CoverageType = 'EmployerLiability' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS ReservesEmployerLiability,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                  AND ClaimTransactionDescriptors.CoverageType = 'Legal' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS ReservesLegal,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                  AND ClaimTransactionDescriptors.CoverageType IN ('Indemnity', 'Medical', 'EmployerLiability') 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS ReservesLoss,
            CASE WHEN ClaimTransactionDescriptors.TransactionType = 'Reserve' 
                 THEN FactClaimTransactionLineWC.TransactionAmount ELSE 0 END AS ReservesLossAndExpense