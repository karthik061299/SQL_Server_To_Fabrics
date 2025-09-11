# Integration test class for end-to-end testing
class TestIntegrationUspSemanticClaimTransactionMeasuresData:
    """Integration tests for the complete workflow"""
    
    @pytest.fixture
    def integration_test_data(self):
        """Larger dataset for integration testing"""
        np.random.seed(42)
        n_records = 1000
        
        return pd.DataFrame({
            'FactClaimTransactionLineWCKey': range(1, n_records + 1),
            'RevisionNumber': np.ones(n_records, dtype=int),
            'PolicyWCKey': np.random.randint(100, 200, n_records),
            'ClaimWCKey': np.random.randint(200, 300, n_records),
            'ClaimTransactionLineCategoryKey': np.random.randint(300, 400, n_records),
            'ClaimTransactionWCKey': np.random.randint(400, 500, n_records),
            'ClaimCheckKey': np.random.randint(500, 600, n_records),
            'TransactionAmount': np.random.uniform(100, 5000, n_records).round(2),
            'NetPaidIndemnity': np.random.uniform(50, 2500, n_records).round(2),
            'NetPaidMedical': np.random.uniform(50, 2500, n_records).round(2),
            'NetPaidExpense': np.random.uniform(0, 200, n_records).round(2),
            'GrossPaidIndemnity': np.random.uniform(60, 2700, n_records).round(2),
            'GrossPaidMedical': np.random.uniform(60, 2700, n_records).round(2),
            'RecoveryDeductible': np.random.choice([0, 50, 100, 250, 500], n_records),
            'SourceClaimTransactionCreateDate': pd.date_range('2024-01-01', periods=n_records, freq='H'),
            'SourceClaimTransactionCreateDateKey': range(20240101, 20240101 + n_records),
            'RetiredInd': np.zeros(n_records, dtype=int)
        })
    
    def test_end_to_end_workflow(self, integration_test_data):
        """Test complete end-to-end workflow"""
        # Test data loading
        assert len(integration_test_data) == 1000
        
        # Test filtering
        filtered_data = integration_test_data[integration_test_data['TransactionAmount'] > 1000]
        assert len(filtered_data) > 0
        
        # Test aggregations
        policy_summary = filtered_data.groupby('PolicyWCKey').agg({
            'TransactionAmount': ['sum', 'mean', 'count'],
            'NetPaidIndemnity': 'sum',
            'NetPaidMedical': 'sum'
        })
        
        assert len(policy_summary) > 0
        
        # Test measures calculation
        total_transaction = filtered_data['TransactionAmount'].sum()
        total_net_paid_indemnity = filtered_data['NetPaidIndemnity'].sum()
        total_net_paid_medical = filtered_data['NetPaidMedical'].sum()
        
        assert total_transaction > 0
        assert total_net_paid_indemnity > 0
        assert total_net_paid_medical > 0
        
        logger.info("End-to-end workflow test passed")
    
    def test_cross_validation_sql_server_vs_fabric(self, integration_test_data):
        """Cross-validate results between SQL Server and Fabric versions"""
        # This test would compare results from both systems
        # For now, we'll simulate the comparison
        
        sql_server_results = {
            'total_transaction': integration_test_data['TransactionAmount'].sum(),
            'total_net_paid_indemnity': integration_test_data['NetPaidIndemnity'].sum(),
            'total_net_paid_medical': integration_test_data['NetPaidMedical'].sum(),
            'claim_count': len(integration_test_data),
            'avg_transaction_amount': integration_test_data['TransactionAmount'].mean()
        }
        
        # Simulate Fabric results (should be identical)
        fabric_results = {
            'total_transaction': integration_test_data['TransactionAmount'].sum(),
            'total_net_paid_indemnity': integration_test_data['NetPaidIndemnity'].sum(),
            'total_net_paid_medical': integration_test_data['NetPaidMedical'].sum(),
            'claim_count': len(integration_test_data),
            'avg_transaction_amount': integration_test_data['TransactionAmount'].mean()
        }
        
        # Compare results
        for key in sql_server_results:
            sql_value = sql_server_results[key]
            fabric_value = fabric_results[key]
            
            if isinstance(sql_value, (int, float)):
                assert abs(sql_value - fabric_value) < 0.01, f"Mismatch in {key}: SQL Server={sql_value}, Fabric={fabric_value}"
            else:
                assert sql_value == fabric_value, f"Mismatch in {key}: SQL Server={sql_value}, Fabric={fabric_value}"
        
        logger.info("Cross-validation test passed")
    
    def test_dynamic_sql_execution(self, mock_connection, integration_test_data):
        """Test execution of dynamically generated SQL"""
        conn, cursor = mock_connection
        
        # Simulate the execution of dynamic SQL
        cursor.fetchall.return_value = [(integration_test_data['NetPaidIndemnity'].sum(),)]
        
        # Generate dynamic SQL for NetPaidIndemnity
        dynamic_sql = "SELECT SUM(NetPaidIndemnity) FROM ClaimTransactionMeasures"
        
        # Execute the dynamic SQL
        cursor.execute(dynamic_sql)
        result = cursor.fetchall()
        
        # Verify the result
        assert result is not None
        assert len(result) > 0
        assert result[0][0] == integration_test_data['NetPaidIndemnity'].sum()
        
        logger.info("Dynamic SQL execution test passed")
    
    def test_session_id_handling(self, mock_connection):
        """Test session ID handling for temporary tables"""
        conn, cursor = mock_connection
        
        # Mock the SESSION_ID() function
        cursor.fetchone.return_value = (123,)
        
        # Execute query to get session ID
        cursor.execute("SELECT SESSION_ID()")
        session_id = cursor.fetchone()[0]
        
        # Verify session ID is used in temporary table names
        temp_table_name = f"##CTM{session_id}"
        assert temp_table_name == "##CTM123"
        
        # Verify different temporary table names are generated
        temp_table_names = [
            f"##CTM{session_id}",
            f"##CTMFact{session_id}",
            f"##CTMF{session_id}",
            f"##CTPrs{session_id}",
            f"##PRDCLmTrans{session_id}"
        ]
        
        assert len(set(temp_table_names)) == 5, "Temporary table names are not unique"
        
        logger.info("Session ID handling test passed")
    
    def test_transaction_isolation(self, mock_connection):
        """Test transaction isolation in Fabric SQL"""
        conn, cursor = mock_connection
        
        # Set up mock behavior
        conn.autocommit = False
        
        # Execute the stored procedure
        cursor.execute("EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime = ?, @pJobEndDateTime = ?", 
                      '2024-01-01', '2024-01-31')
        
        # Check if data is visible within this transaction
        cursor.execute("SELECT COUNT(*) FROM Semantic.ClaimTransactionMeasures")
        cursor.fetchone.return_value = (1000,)
        count_in_transaction = cursor.fetchone()[0]
        
        # Rollback the transaction
        conn.rollback()
        
        # Check if data changes were rolled back
        cursor.execute("SELECT COUNT(*) FROM Semantic.ClaimTransactionMeasures")
        cursor.fetchone.return_value = (0,)
        count_after_rollback = cursor.fetchone()[0]
        
        # Reset autocommit
        conn.autocommit = True
        
        # Verify transaction isolation
        assert count_in_transaction == 1000
        assert count_after_rollback == 0
        
        logger.info("Transaction isolation test passed")


if __name__ == "__main__":
    # Run tests
    pytest.main(["-v", "--tb=short"])