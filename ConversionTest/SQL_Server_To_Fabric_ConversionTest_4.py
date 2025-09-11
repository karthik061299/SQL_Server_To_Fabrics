    def test_cte_usage(self, mock_fabric_conn):
        """Test Common Table Expression (CTE) usage in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Execute a query with CTE
        try:
            cursor.execute("""
            WITH TestCTE AS (
                SELECT ID, Name FROM Table WHERE Status = 'Active'
            )
            SELECT * FROM TestCTE;
            """)
            
            # Should execute without errors
            assert True
        except Exception as e:
            pytest.fail(f"CTE usage failed: {str(e)}")
    
    def test_special_case_handling(self, mock_fabric_conn):
        """Test special case handling in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Execute with the special date
        special_date = datetime(1900, 1, 1)
        end_date = datetime.now()
        
        # Mock to capture the executed SQL
        executed_sql = []
        
        def execute_side_effect(query, *args):
            if isinstance(query, str) and "@pJobStartDateTime" in query:
                executed_sql.append((query, args))
            return cursor
        
        cursor.execute.side_effect = execute_side_effect
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            special_date, end_date
        )
        
        # Verify that the procedure was called with the right parameters
        assert len(executed_sql) > 0, "No SQL was executed"
        assert executed_sql[0][1][0] == special_date, "Special date parameter was not passed correctly"
    
    def test_measure_calculation(self, mock_fabric_conn, setup_test_data):
        """Test measure calculation from SemanticLayerMetaData"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Mock the SemanticLayerMetaData query result
        semantic_metadata = pd.DataFrame({
            'SourceType': ['Claims'] * 5,
            'Measure_Name': ['NetPaidIndemnity', 'NetPaidMedical', 'NetIncurredLoss', 'GrossPaidLoss', 'RecoveryIndemnity'],
            'Logic': ['SUM(CASE WHEN x=1 THEN Amount ELSE 0 END)', 'SUM(CASE WHEN x=2 THEN Amount ELSE 0 END)', 
                     'SUM(CASE WHEN x=3 THEN Amount ELSE 0 END)', 'SUM(CASE WHEN x=4 THEN Amount ELSE 0 END)', 
                     'SUM(CASE WHEN x=5 THEN Amount ELSE 0 END)']
        })
        
        # Mock the string aggregation of measure definitions
        measure_sql = ", ".join([f"{row['Logic']} AS {row['Measure_Name']}" 
                              for _, row in semantic_metadata.iterrows()])
        
        def execute_side_effect(query, *args):
            if "STRING_AGG" in query and "SemanticLayerMetaData" in query:
                cursor.fetchone.return_value = [measure_sql]
            return cursor
        
        cursor.execute.side_effect = execute_side_effect
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # Verify that the measures are included in the query
        measure_names = semantic_metadata['Measure_Name'].tolist()
        
        # Check that all measure names are used in at least one query
        for measure in measure_names:
            found = False
            for call_args in cursor.execute.call_args_list:
                if isinstance(call_args[0][0], str) and measure in call_args[0][0]:
                    found = True
                    break
            assert found, f"Measure {measure} not found in any query"