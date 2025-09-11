# Import integration tests from separate file
from SQL_Server_To_Fabric_UnitTest_2_integration import TestIntegrationUspSemanticClaimTransactionMeasuresData

if __name__ == "__main__":
    # Run tests
    pytest.main(["-v", "--tb=short"])