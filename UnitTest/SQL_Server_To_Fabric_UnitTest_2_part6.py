    def test_whitespace_handling(self, setup_whitespace_employee_table, setup_salary_table, setup_operation_log_table):
        """Test that the script correctly trims whitespace from string fields"""
        mock_conn = setup_whitespace_employee_table
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
        
        # Execute the BEGIN block and variable declarations
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
        
        # Complete the rest of the script execution
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
        
        # Check that the employee_bkup table exists and has data
        assert 'employee_bkup' in mock_conn.tables
        
        # Check that whitespace was trimmed from FirstName and LastName
        df = mock_conn.tables['employee_bkup']
        
        # Check first row which had whitespace in FirstName
        assert df[df['EmployeeNo'] == 1]['FirstName'].iloc[0] == 'John'
        assert df[df['EmployeeNo'] == 1]['LastName'].iloc[0] == 'Doe'
        
        # Check third row which had whitespace in both fields
        assert df[df['EmployeeNo'] == 3]['FirstName'].iloc[0] == 'Bob'
        assert df[df['EmployeeNo'] == 3]['LastName'].iloc[0] == 'Johnson'
    
    def test_validation_report_generation(self, setup_employee_table, setup_salary_table):
        """Test that the script correctly generates the validation report"""
        mock_conn = setup_employee_table
        mock_conn = setup_salary_table
        
        # Create and populate the employee_bkup table
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
        
        # Insert data directly for this test
        mock_conn.tables['employee_bkup'] = pd.DataFrame({
            'EmployeeNo': [1, 2, 3, 4, 5],
            'FirstName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'LastName': ['Doe', 'Smith', 'Johnson', 'Brown', 'Davis'],
            'DepartmentNo': [1, 2, 1, 3, 2],
            'NetPay': [50000, 60000, 55000, 65000, 70000],
            'BackupDate': [datetime.now()] * 5
        })
        
        # Execute the validation report query
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
        
        # Check that the query results contain the expected metrics
        results = mock_conn.fetchall()
        assert len(results) == 1
        
        result = results[0]
        assert result['table_name'] == 'employee_bkup'
        assert result['row_count'] == 5
        assert result['department_count'] == 3  # We have departments 1, 2, and 3
        assert 60000 <= result['average_net_pay'] <= 60001  # Average of [50000, 60000, 55000, 65000, 70000] is 60000
    
    def test_sample_data_query(self, setup_employee_table, setup_salary_table):
        """Test that the script correctly queries sample data from the backup table"""
        mock_conn = setup_employee_table
        mock_conn = setup_salary_table
        
        # Create and populate the employee_bkup table
        mock_conn.tables['employee_bkup'] = pd.DataFrame({
            'EmployeeNo': [1, 2, 3, 4, 5],
            'FirstName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'LastName': ['Doe', 'Smith', 'Johnson', 'Brown', 'Davis'],
            'DepartmentNo': [1, 2, 1, 3, 2],
            'NetPay': [50000, 60000, 55000, 65000, 70000],
            'BackupDate': [datetime.now()] * 5
        })
        
        # Execute the sample data query
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
        
        # Check that the query returns the expected number of rows
        results = mock_conn.fetchall()
        assert len(results) == 5  # We only have 5 rows in our test data