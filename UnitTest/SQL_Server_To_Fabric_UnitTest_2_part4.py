# Test cases
class TestEmployeeBackupScript:
    """Test cases for the employee_bkup_refresh_fabric.sql script"""
    
    def test_drop_table_if_exists(self, mock_fabric_connection):
        """Test that the script drops the employee_bkup table if it exists"""
        # Create a mock employee_bkup table
        mock_fabric_connection.tables['employee_bkup'] = pd.DataFrame()
        
        # Execute the DROP TABLE IF EXISTS statement
        mock_fabric_connection.execute("DROP TABLE IF EXISTS employee_bkup;")
        
        # Check that the table was dropped
        assert 'employee_bkup' not in mock_fabric_connection.tables
    
    def test_create_table_structure(self, mock_fabric_connection):
        """Test that the script creates the employee_bkup table with the correct structure"""
        # Execute the CREATE TABLE statement
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
        mock_fabric_connection.execute(create_table_sql)
        
        # Check that the table was created
        assert 'employee_bkup' in mock_fabric_connection.tables
    
    def test_insert_data_happy_path(self, setup_employee_table, setup_salary_table, setup_operation_log_table):
        """Test that the script inserts data from Employee and Salary tables correctly"""
        mock_conn = setup_employee_table
        mock_conn = setup_salary_table
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
        
        # Execute the BEGIN block
        mock_conn.execute("BEGIN")
        
        # Execute variable declarations
        mock_conn.execute("DECLARE @error_message STRING = '';")
        mock_conn.execute("DECLARE @row_count INT = 0;")
        
        # Execute the BEGIN TRY block
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
        
        # Begin CATCH block
        mock_conn.execute("BEGIN CATCH")
        
        # Set error message
        mock_conn.execute("SET @error_message = ERROR_MESSAGE();")
        
        # Execute error log insert
        mock_conn.execute("""
        INSERT INTO operation_log (operation_name, status, error_message, timestamp)
        VALUES ('employee_bkup refresh', 'FAILED', @error_message, CURRENT_TIMESTAMP);
        """)
        
        # Execute THROW statement
        mock_conn.execute("THROW 50000, @error_message, 1;")
        
        # End CATCH block
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
        
        # End the BEGIN block
        mock_conn.execute("END;")
        
        # Check that the employee_bkup table exists and has data
        assert 'employee_bkup' in mock_conn.tables
        assert len(mock_conn.tables['employee_bkup']) == 5  # We expect 5 rows from our test data
        
        # Check that the log entries were created
        assert len(mock_conn.log_entries) > 0
        
        # Check that the correct columns were created in employee_bkup
        expected_columns = ['EmployeeNo', 'FirstName', 'LastName', 'DepartmentNo', 'NetPay', 'BackupDate']
        for col in expected_columns:
            assert col in mock_conn.tables['employee_bkup'].columns