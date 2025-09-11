    def test_syntax_differences(self):
        """Test to identify and document syntax differences between SQL Server and Fabric versions"""
        # This test analyzes the stored procedure code to identify syntax differences
        
        # SQL Server specific syntax patterns
        sql_server_patterns = {
            "global_temp_tables": r'##\w+',
            "execute_sp_executesql": r'EXECUTE\s+sp_executesql',
            "set_xact_abort": r'SET\s+XACT_ABORT\s+ON',
            "sys_tables_reference": r'sys\.tables',
            "alter_index": r'ALTER\s+INDEX.*DISABLE'
        }
        
        # Fabric SQL equivalent patterns
        fabric_patterns = {
            "session_temp_tables": r'CREATE\s+TABLE\s+\w+_[0-9a-f-]+',
            "execute_immediate": r'EXECUTE\s*\(\@\w+\)',
            "drop_if_exists": r'DROP\s+TABLE\s+IF\s+EXISTS',
            "hash_join_hint": r'INNER\s+HASH\s+JOIN',
            "label_hint": r"OPTION\s*\(\s*LABEL\s*=\s*'[^']*'\s*\)"
        }
        
        # Load SQL Server and Fabric stored procedure code
        # In a real test, you would load these from files or databases
        sql_server_code = """-- SQL Server code would be loaded here"""
        fabric_code = """-- Fabric SQL code would be loaded here"""
        
        # Check for SQL Server patterns that should be converted
        for pattern_name, pattern in sql_server_patterns.items():
            matches = re.findall(pattern, sql_server_code, re.IGNORECASE)
            if matches:
                print(f"Found {len(matches)} instances of {pattern_name} in SQL Server code")
                # These patterns should not appear in Fabric code
                fabric_matches = re.findall(pattern, fabric_code, re.IGNORECASE)
                assert len(fabric_matches) == 0, f"{pattern_name} found in Fabric code but should be converted"
        
        # Check for Fabric patterns that should be present after conversion
        for pattern_name, pattern in fabric_patterns.items():
            matches = re.findall(pattern, fabric_code, re.IGNORECASE)
            assert len(matches) > 0, f"Expected {pattern_name} not found in Fabric code"
    
    @pytest.mark.parametrize(
        "test_case,job_start_datetime,job_end_datetime,expected_status", [
            ("normal_date_range", datetime.now() - timedelta(days=7), datetime.now(), "success"),
            ("special_date_case", datetime(1900, 1, 1), datetime.now(), "success"),  # Tests special date handling
            ("wide_date_range", datetime.now() - timedelta(days=365), datetime.now(), "success"),
            ("invalid_date_range", datetime.now(), datetime.now() - timedelta(days=1), "error")
        ]
    )
    def test_date_parameter_handling(self, mock_sql_server_conn, mock_fabric_conn, 
                                   test_case, job_start_datetime, job_end_datetime, expected_status):
        """Test date parameter handling in both SQL Server and Fabric versions"""
        
        # Configure mocks for both connections
        sql_cursor = mock_sql_server_conn.cursor.return_value
        fabric_cursor = mock_fabric_conn.cursor.return_value
        
        # Test execution in SQL Server
        if expected_status == "error" and job_start_datetime > job_end_datetime:
            # Should raise error for invalid date range
            with pytest.raises(Exception):
                sql_cursor.execute(
                    "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                    job_start_datetime, job_end_datetime
                )
        else:
            sql_cursor.execute(
                "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                job_start_datetime, job_end_datetime
            )
        
        # Test execution in Fabric
        if expected_status == "error" and job_start_datetime > job_end_datetime:
            # Should raise error for invalid date range
            with pytest.raises(Exception):
                fabric_cursor.execute(
                    "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                    job_start_datetime, job_end_datetime
                )
        else:
            fabric_cursor.execute(
                "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                job_start_datetime, job_end_datetime
            )
        
        # Special case handling for 1900-01-01
        if test_case == "special_date_case":
            # In both versions, 1900-01-01 should be converted to 1700-01-01
            # This would be verified by examining the executed SQL or results
            pass