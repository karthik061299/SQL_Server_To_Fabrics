# Edge case tests
class TestEdgeCases:
    """Test cases for edge cases in the employee_bkup_refresh_fabric.sql script"""
    
    def test_no_matching_records_in_join(self, mock_fabric_connection, setup_operation_log_table):
        """Test behavior when there are no matching records in the join"""
        mock_conn = mock_fabric_connection
        mock_conn = setup_operation_log_table
        
        # Set up Employee table with data
        mock_conn.tables['Employee'] = pd.DataFrame({
            'EmployeeNo': [1, 2, 3],
            'FirstName': ['John', 'Jane', 'Bob'],
            'LastName': ['Doe', 'Smith', 'Johnson'],
            'DepartmentNo': [1, 2, 1],
            'HireDate': [datetime.now()] * 3
        })
        
        # Set up Salary table with non-matching EmployeeNo values
        mock_conn.tables['Salary'] = pd.DataFrame({
            'EmployeeNo': [4, 5, 6],
            'NetPay': [50000, 60000, 55000],
            'LastUpdate': [datetime.now()] * 3
        })
        
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
        
        # Execute the script
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
        
        # Complete the script execution
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
        
        # Check that the employee_bkup table was dropped due to no matching records
        assert 'employee_bkup' not in mock_conn.tables
        
        # Check that the appropriate log entry was created
        warning_log_found = False
        for log_entry in mock_conn.log_entries:
            if len(log_entry) >= 3 and 'WARNING' in log_entry and 'empty' in str(log_entry):
                warning_log_found = True
                break
        assert warning_log_found
    
    def test_null_values_in_non_key_fields(self, mock_fabric_connection, setup_operation_log_table):
        """Test handling of NULL values in non-key fields"""
        mock_conn = mock_fabric_connection
        mock_conn = setup_operation_log_table
        
        # Set up Employee table with NULL values in non-key fields
        mock_conn.tables['Employee'] = pd.DataFrame({
            'EmployeeNo': [1, 2, 3],
            'FirstName': ['John', None, 'Bob'],  # NULL FirstName
            'LastName': ['Doe', 'Smith', None],  # NULL LastName
            'DepartmentNo': [1, None, 3],        # NULL DepartmentNo
            'HireDate': [datetime.now(), None, datetime.now()]  # NULL HireDate
        })
        
        # Set up Salary table
        mock_conn.tables['Salary'] = pd.DataFrame({
            'EmployeeNo': [1, 2, 3],
            'NetPay': [50000, None, 55000],  # NULL NetPay
            'LastUpdate': [datetime.now(), datetime.now(), None]  # NULL LastUpdate
        })
        
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
        
        # Execute the script
        mock_conn.execute("BEGIN")
        mock_conn.execute("DECLARE @error_message STRING = '';")
        mock_conn.execute("DECLARE @row_count INT = 0;")
        mock_conn.execute("BEGIN TRY")
        
        # Execute the INSERT INTO statement with COALESCE handling
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
        
        # Complete the script execution
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
        
        # Check that the employee_bkup table exists
        assert 'employee_bkup' in mock_conn.tables
        
        # Check that NULL values were handled correctly
        df = mock_conn.tables['employee_bkup']
        
        # Check that NULL FirstName was replaced with empty string
        assert df[df['EmployeeNo'] == 2]['FirstName'].iloc[0] == ''
        
        # Check that NULL LastName was replaced with empty string
        assert df[df['EmployeeNo'] == 3]['LastName'].iloc[0] == ''
        
        # Check that NULL DepartmentNo remains NULL
        assert pd.isna(df[df['EmployeeNo'] == 2]['DepartmentNo'].iloc[0])
        
        # Check that NULL NetPay remains NULL
        assert pd.isna(df[df['EmployeeNo'] == 2]['NetPay'].iloc[0])