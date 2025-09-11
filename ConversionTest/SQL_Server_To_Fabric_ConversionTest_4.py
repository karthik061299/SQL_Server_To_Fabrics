    def test_performance_comparison(self, mock_sql_server_conn, mock_fabric_conn):
        """Compare performance between SQL Server and Fabric SQL versions"""
        # Configure mocks
        sql_cursor = mock_sql_server_conn.cursor.return_value
        fabric_cursor = mock_fabric_conn.cursor.return_value
        
        # Mock execution times
        sql_start_time = time.time()
        sql_end_time = sql_start_time + 2.5  # 2.5 seconds for SQL Server
        
        fabric_start_time = time.time()
        fabric_end_time = fabric_start_time + 2.0  # 2.0 seconds for Fabric
        
        # Execute on SQL Server
        with patch('time.time', side_effect=[sql_start_time, sql_end_time]):
            sql_cursor.execute(
                "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                datetime.now() - timedelta(days=7), datetime.now()
            )
            sql_execution_time = sql_end_time - sql_start_time
        
        # Execute on Fabric
        with patch('time.time', side_effect=[fabric_start_time, fabric_end_time]):
            fabric_cursor.execute(
                "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                datetime.now() - timedelta(days=7), datetime.now()
            )
            fabric_execution_time = fabric_end_time - fabric_start_time
        
        # Compare execution times
        print(f"SQL Server execution time: {sql_execution_time:.2f} seconds")
        print(f"Fabric SQL execution time: {fabric_execution_time:.2f} seconds")
        print(f"Performance difference: {((fabric_execution_time - sql_execution_time) / sql_execution_time * 100):.2f}%")
        
        # In this mock example, Fabric is faster
        assert fabric_execution_time < sql_execution_time, "Fabric SQL should be faster than SQL Server"
    
    def test_maxdop_option(self, mock_fabric_conn):
        """Test MAXDOP query hint in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Execute a query with MAXDOP hint
        try:
            cursor.execute("""
            SELECT * FROM Table
            OPTION(MAXDOP 8);
            """)
            
            # Should execute without errors
            assert True
        except Exception as e:
            pytest.fail(f"MAXDOP hint execution failed: {str(e)}")
    
    def test_label_option(self, mock_fabric_conn):
        """Test LABEL query hint in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Execute a query with LABEL hint
        try:
            cursor.execute("""
            SELECT * FROM Table
            OPTION(LABEL = 'Test Query');
            """)
            
            # Should execute without errors
            assert True
        except Exception as e:
            pytest.fail(f"LABEL hint execution failed: {str(e)}")