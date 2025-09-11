_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: SQL Server to Fabric migration validation script for uspSemanticClaimTransactionMeasuresData
## *Version*: 1 
## *Updated on*: 
_____________________________________________

"""
SQL Server to Fabric Migration Validation Script

This script handles the end-to-end process of:
1. Executing SQL Server uspSemanticClaimTransactionMeasuresData stored procedure
2. Transferring results to Microsoft Fabric
3. Running equivalent Fabric SQL code
4. Validating that results match between both systems

Target Tables Identified:
- Semantic.ClaimTransactionMeasures (main output table)
- Temporary processing tables (##CTM