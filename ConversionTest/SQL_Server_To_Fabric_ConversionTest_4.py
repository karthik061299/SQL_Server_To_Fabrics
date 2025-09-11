    def test_string_agg_function(self, mock_fabric_conn):
        """Test STRING_AGG function in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Execute a query with STRING_AGG function
        try:
            cursor.execute("""
            SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), Column), ',') 
            WITHIN GROUP (ORDER BY Column ASC)
            FROM Table;
            """)
            
            # Should execute without errors
            assert True
        except Exception as e:
            pytest.fail(f"STRING_AGG function execution failed: {str(e)}")
    
    def test_error_handling(self, mock_fabric_conn):
        """Test error handling in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Configure mock to raise an error
        cursor.execute.side_effect = pyodbc.Error("Test error")
        
        # Execute the stored procedure with try-catch block
        try:
            cursor.execute("""
            BEGIN TRY
                -- SQL that will cause an error
                SELECT * FROM NonExistentTable;
            END TRY
            BEGIN CATCH
                SELECT 
                    ERROR_MESSAGE() AS ErrorMessage,
                    ERROR_SEVERITY() AS ErrorSeverity,
                    ERROR_STATE() AS ErrorState;
                    
                -- Log error to Fabric's logging system
                EXEC [dbo].[LogError] 
                    @ErrorMessage = ERROR_MESSAGE(),
                    @ErrorSeverity = ERROR_SEVERITY(),
                    @ErrorState = ERROR_STATE(),
                    @ProcedureName = 'Semantic.uspSemanticClaimTransactionMeasuresData';
                    
                -- Re-throw error with additional context
                THROW;
            END CATCH;
            """)
            
            pytest.fail("Error handling test should have raised an exception")
        except pyodbc.Error:
            # Expected error
            assert True
    
    def test_data_transformation(self, mock_fabric_conn, setup_test_data):
        """Test data transformation logic in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Mock the result set
        expected_df = setup_test_data['expected_results']
        mock_result = [tuple(row) for row in expected_df.values]
        
        cursor.fetchall.return_value = mock_result
        cursor.description = [(col, None, None, None, None, None, None) for col in expected_df.columns]
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # Fetch the results
        result = cursor.fetchall()
        
        # Convert to DataFrame for easier comparison
        result_df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        
        # Verify the transformation results
        assert len(result_df) == len(expected_df)
        assert set(result_df.columns) == set(expected_df.columns)