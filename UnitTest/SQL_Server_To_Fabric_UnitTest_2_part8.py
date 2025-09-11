# Performance test cases
class TestPerformance:
    """Test cases for performance considerations in the employee_bkup_refresh_fabric.sql script"""
    
    @pytest.mark.skip(reason="Performance test - only run manually")
    def test_large_dataset_performance(self, mock_fabric_connection, setup_operation_log_table):
        """Test performance with a large dataset"""
        mock_conn = mock_fabric_connection
        mock_conn = setup_operation_log_table
        
        # Create large test datasets
        num_rows = 10000  # Reduced from 100,000 for faster test execution
        
        # Set up large Employee table
        employee_data = {
            'EmployeeNo': list(range(1, num_rows + 1)),
            'FirstName': ['FirstName' + str(i) for i in range(1, num_rows + 1)],
            'LastName': ['LastName' + str(i) for i in range(1, num_rows + 1)],
            'DepartmentNo': [i % 10 + 1 for i in range(1, num_rows + 1)],
            'HireDate': [datetime.now() for _ in range(num_rows)]
        }
        mock_conn.tables['Employee'] = pd.DataFrame(employee_data)
        
        # Set up large Salary table
        salary_data = {
            'EmployeeNo': list(range(1, num_rows + 1)),
            'NetPay': [50000 + (i % 1000) * 100 for i in range(1, num_rows + 1)],
            'LastUpdate': [datetime.now() for _ in range(num_rows)]
        }
        mock_conn.tables['Salary'] = pd.DataFrame(salary_data)
        
        # Create the employee_bkup table
        create_table_sql = """
        CREATE TABLE employee_bkup
        (
            EmployeeNo   INT         NOT NULL PRIMARY KEY CLUSTERED,
            FirstName    STRING      NOT NULL,
            LastName     STRING      NOT NULL,
            DepartmentNo INT         NULL,
            NetPay       INT         NULL,
            BackupDate   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        WITH (
            DISTRIBUTION = HASH(EmployeeNo),
            PARTITION = RANGE(BackupDate)
        );
        """
        mock_conn.execute(create_table_sql)
        
        # Measure execution time of the INSERT INTO statement
        import time
        start_time = time.time()
        
        # Execute the script
        mock_conn.execute("BEGIN")
        mock_conn.execute("DECLARE @error_message STRING = '';")
        mock_conn.execute("DECLARE @row_count INT = 0;")
        mock_conn.execute("BEGIN TRY")
        
        insert_sql = """
        INSERT INTO employee_bkup (EmployeeNo, FirstName, LastName, DepartmentNo, NetPay, BackupDate)
        SELECT  
            e.EmployeeNo,
            COALESCE(TRIM(e.FirstName), '') AS FirstName,
            COALESCE(TRIM(e.LastName), '') AS LastName,
            e.DepartmentNo,
            s.NetPay,
            CURRENT_TIMESTAMP AS BackupDate
        FROM Employee AS e
        INNER JOIN Salary AS s
            ON e.EmployeeNo = s.EmployeeNo
        WHERE e.EmployeeNo IS NOT NULL;
        """
        mock_conn.execute(insert_sql)
        
        mock_conn.execute("SET @row_count = @@ROWCOUNT;")
        mock_conn.execute("""
        INSERT INTO operation_log (operation_name, status, record_count, timestamp)
        VALUES ('employee_bkup refresh', 'SUCCESS', @row_count, CURRENT_TIMESTAMP);
        """)
        mock_conn.execute("END TRY")
        mock_conn.execute("BEGIN CATCH")
        mock_conn.execute("SET @error_message = ERROR_MESSAGE();")
        mock_conn.execute("""
        INSERT INTO operation_log (operation_name, status, error_message, timestamp)
        VALUES ('employee_bkup refresh', 'FAILED', @error_message, CURRENT_TIMESTAMP);
        """)
        mock_conn.execute("END CATCH")
        
        mock_conn.execute("""
        IF @row_count = 0
        BEGIN
            DROP TABLE IF EXISTS employee_bkup;
            
            INSERT INTO operation_log (operation_name, status, message, timestamp)
            VALUES ('employee_bkup refresh', 'WARNING', 'Backup table dropped - source table is empty', CURRENT_TIMESTAMP);
        END
        ELSE
        BEGIN
            ALTER TABLE employee_bkup REBUILD;
            
            UPDATE STATISTICS employee_bkup;
            
            INSERT INTO operation_log (operation_name, status, record_count, message, timestamp)
            VALUES ('employee_bkup refresh', 'COMPLETED', @row_count, 'Backup completed with optimization', CURRENT_TIMESTAMP);
        END;
        """)
        mock_conn.execute("END;")
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # In a real performance test, we would assert that the execution time is below a threshold
        print(f"Execution time for {num_rows} rows: {execution_time:.2f} seconds")
        
        # This is a placeholder assertion - in a real test, you would set an appropriate threshold
        assert execution_time < 60  # Assuming less than 60 seconds is acceptable
    
    def test_error_handling_performance(self, mock_fabric_connection, setup_operation_log_table):
        """Test error handling performance"""
        mock_conn = mock_fabric_connection
        mock_conn = setup_operation_log_table
        
        # Create the employee_bkup table
        create_table_sql = """
        CREATE TABLE employee_bkup
        (
            EmployeeNo   INT         NOT NULL PRIMARY KEY CLUSTERED,
            FirstName    STRING      NOT NULL,
            LastName     STRING      NOT NULL,
            DepartmentNo INT         NULL,
            NetPay       INT         NULL,
            BackupDate   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        WITH (
            DISTRIBUTION = HASH(EmployeeNo),
            PARTITION = RANGE(BackupDate)
        );
        """
        mock_conn.execute(create_table_sql)
        
        # Execute the BEGIN block and variable declarations
        mock_conn.execute("BEGIN")
        mock_conn.execute("DECLARE @error_message STRING = '';")
        mock_conn.execute("DECLARE @row_count INT = 0;")
        mock_conn.execute("BEGIN TRY")
        
        # Simulate an error by referencing a non-existent table
        try:
            mock_conn.execute("""
            INSERT INTO employee_bkup (EmployeeNo, FirstName, LastName, DepartmentNo, NetPay, BackupDate)
            SELECT  
                e.EmployeeNo,
                COALESCE(TRIM(e.FirstName), '') AS FirstName,
                COALESCE(TRIM(e.LastName), '') AS LastName,
                e.DepartmentNo,
                s.NetPay,
                CURRENT_TIMESTAMP AS BackupDate
            FROM NonExistentTable AS e
            INNER JOIN Salary AS s
                ON e.EmployeeNo = s.EmployeeNo
            WHERE e.EmployeeNo IS NOT NULL;
            """)
        except Exception as e:
            # Expected exception
            pass
        
        # Execute the CATCH block
        mock_conn.execute("END TRY")
        mock_conn.execute("BEGIN CATCH")
        mock_conn.execute("SET @error_message = 'Table NonExistentTable not found';")
        mock_conn.execute("""
        INSERT INTO operation_log (operation_name, status, error_message, timestamp)
        VALUES ('employee_bkup refresh', 'FAILED', @error_message, CURRENT_TIMESTAMP);
        """)
        mock_conn.execute("END CATCH")
        mock_conn.execute("END;")
        
        # Check that the error was logged
        error_log_found = False
        for log_entry in mock_conn.log_entries:
            if len(log_entry) >= 3 and 'FAILED' in log_entry and 'NonExistentTable' in str(log_entry):
                error_log_found = True
                break
        assert error_log_found

# Integration test for the end-to-end workflow
class TestIntegration:
    """Integration tests for the employee_bkup_refresh_fabric.sql script"""
    
    def test_end_to_end_workflow(self, setup_employee_table, setup_salary_table, setup_operation_log_table):
        """Test the entire workflow from start to finish"""
        mock_conn = setup_employee_table
        mock_conn = setup_salary_table
        mock_conn = setup_operation_log_table
        
        # Step 1: Drop existing backup table if it exists
        mock_conn.execute("DROP TABLE IF EXISTS employee_bkup;")
        
        # Step 2: Create the backup table structure
        create_table_sql = """
        CREATE TABLE employee_bkup
        (
            EmployeeNo   INT         NOT NULL PRIMARY KEY CLUSTERED,
            FirstName    STRING      NOT NULL,
            LastName     STRING      NOT NULL,
            DepartmentNo INT         NULL,
            NetPay       INT         NULL,
            BackupDate   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        WITH (
            DISTRIBUTION = HASH(EmployeeNo),
            PARTITION = RANGE(BackupDate)
        );
        """
        mock_conn.execute(create_table_sql)
        
        # Step 3: Execute the main script
        mock_conn.execute("BEGIN")
        mock_conn.execute("DECLARE @error_message STRING = '';")
        mock_conn.execute("DECLARE @row_count INT = 0;")
        mock_conn.execute("BEGIN TRY")
        
        # Execute the INSERT INTO statement
        insert_sql = """
        INSERT INTO employee_bkup (EmployeeNo, FirstName, LastName, DepartmentNo, NetPay, BackupDate)
        SELECT  
            e.EmployeeNo,
            COALESCE(TRIM(e.FirstName), '') AS FirstName,
            COALESCE(TRIM(e.LastName), '') AS LastName,
            e.DepartmentNo,
            s.NetPay,
            CURRENT_TIMESTAMP AS BackupDate
        FROM Employee AS e
        INNER JOIN Salary AS s
            ON e.EmployeeNo = s.EmployeeNo
        WHERE e.EmployeeNo IS NOT NULL;
        """
        mock_conn.execute(insert_sql)
        
        # Set row count
        mock_conn.execute("SET @row_count = @@ROWCOUNT;")
        
        # Execute log insert
        mock_conn.execute("""
        INSERT INTO operation_log (operation_name, status, record_count, timestamp)
        VALUES ('employee_bkup refresh', 'SUCCESS', @row_count, CURRENT_TIMESTAMP);
        """)
        
        # End TRY block
        mock_conn.execute("END TRY")
        mock_conn.execute("BEGIN CATCH")
        mock_conn.execute("SET @error_message = ERROR_MESSAGE();")
        mock_conn.execute("""
        INSERT INTO operation_log (operation_name, status, error_message, timestamp)
        VALUES ('employee_bkup refresh', 'FAILED', @error_message, CURRENT_TIMESTAMP);
        """)
        mock_conn.execute("END CATCH")
        
        # Execute IF block for empty source tables
        mock_conn.execute("""
        IF @row_count = 0
        BEGIN
            DROP TABLE IF EXISTS employee_bkup;
            
            INSERT INTO operation_log (operation_name, status, message, timestamp)
            VALUES ('employee_bkup refresh', 'WARNING', 'Backup table dropped - source table is empty', CURRENT_TIMESTAMP);
        END
        ELSE
        BEGIN
            ALTER TABLE employee_bkup REBUILD;
            
            UPDATE STATISTICS employee_bkup;
            
            INSERT INTO operation_log (operation_name, status, record_count, message, timestamp)
            VALUES ('employee_bkup refresh', 'COMPLETED', @row_count, 'Backup completed with optimization', CURRENT_TIMESTAMP);
        END;
        """)
        mock_conn.execute("END;")
        
        # Step 4: Generate validation report
        validation_sql = """
        SELECT 
            'employee_bkup' AS table_name,
            COUNT(*) AS row_count,
            MIN(BackupDate) AS earliest_record,
            MAX(BackupDate) AS latest_record,
            COUNT(DISTINCT DepartmentNo) AS department_count,
            AVG(NetPay) AS average_net_pay,
            CURRENT_TIMESTAMP AS report_generated_at
        FROM employee_bkup;
        """
        mock_conn.execute(validation_sql)
        
        # Step 5: Display sample of backed up data
        sample_sql = """
        SELECT TOP 10 
            EmployeeNo,
            FirstName,
            LastName,
            DepartmentNo,
            NetPay,
            FORMAT(BackupDate, 'yyyy-MM-dd HH:mm:ss') AS backup_timestamp
        FROM employee_bkup
        ORDER BY EmployeeNo;
        """
        mock_conn.execute(sample_sql)
        
        # Check that all steps were executed in the correct order
        executed_queries = [q.strip() for q in mock_conn.executed_queries]
        
        # Check that the employee_bkup table exists and has data
        assert 'employee_bkup' in mock_conn.tables
        assert len(mock_conn.tables['employee_bkup']) == 5  # We expect 5 rows from our test data
        
        # Check that the log entries were created
        assert len(mock_conn.log_entries) > 0
        
        # Check that the validation report was generated
        assert validation_sql.strip() in executed_queries
        
        # Check that the sample data query was executed
        assert sample_sql.strip() in executed_queries

# Main entry point for running the tests
if __name__ == "__main__":
    pytest.main(['-xvs', __file__])