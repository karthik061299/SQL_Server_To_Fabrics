        # Handle SELECT queries
        elif query.strip().upper().startswith('SELECT'):
            if 'COUNT(*)' in query and 'employee_bkup' in query.lower():
                # Mock the count query for employee_bkup table
                if 'employee_bkup' in self.tables:
                    count = len(self.tables['employee_bkup'])
                    self.current_results = pd.DataFrame({'COUNT(*)': [count]})
                else:
                    self.current_results = pd.DataFrame({'COUNT(*)': [0]})
            elif 'COUNT(*)' in query and 'Employee' in query:
                # Mock the count query for Employee table
                if 'Employee' in self.tables:
                    if 'empty_source' in self.tables.get('Employee', pd.DataFrame()).get('test_case', []):
                        self.current_results = pd.DataFrame({'COUNT(*)': [0]})
                    else:
                        count = len(self.tables['Employee'])
                        self.current_results = pd.DataFrame({'COUNT(*)': [count]})
                else:
                    self.current_results = pd.DataFrame({'COUNT(*)': [0]})
            elif 'table_name' in query.lower() and 'row_count' in query.lower():
                # Mock the validation report query
                if 'employee_bkup' in self.tables:
                    df = self.tables['employee_bkup']
                    self.current_results = pd.DataFrame({
                        'table_name': ['employee_bkup'],
                        'row_count': [len(df)],
                        'earliest_record': [df['BackupDate'].min() if 'BackupDate' in df.columns and not df.empty else None],
                        'latest_record': [df['BackupDate'].max() if 'BackupDate' in df.columns and not df.empty else None],
                        'department_count': [df['DepartmentNo'].nunique() if 'DepartmentNo' in df.columns and not df.empty else 0],
                        'average_net_pay': [df['NetPay'].mean() if 'NetPay' in df.columns and not df.empty else 0],
                        'report_generated_at': [datetime.now()]
                    })
                else:
                    self.current_results = pd.DataFrame({
                        'table_name': ['employee_bkup'],
                        'row_count': [0],
                        'earliest_record': [None],
                        'latest_record': [None],
                        'department_count': [0],
                        'average_net_pay': [0],
                        'report_generated_at': [datetime.now()]
                    })
            elif 'TOP 10' in query and 'employee_bkup' in query:
                # Mock the sample data query
                if 'employee_bkup' in self.tables:
                    self.current_results = self.tables['employee_bkup'].head(10)
                else:
                    self.current_results = pd.DataFrame()
            return True
        
        # Handle ALTER TABLE
        elif query.strip().upper().startswith('ALTER TABLE'):
            return True
        
        # Handle UPDATE STATISTICS
        elif query.strip().upper().startswith('UPDATE STATISTICS'):
            return True
        
        # Handle DECLARE statements
        elif query.strip().upper().startswith('DECLARE'):
            # Extract variable name and value if it's a declaration with assignment
            if '=' in query:
                var_name = query.split('=')[0].strip().split(' ')[1].strip('@')
                # Store the variable value (simplified)
                if var_name == 'row_count':
                    # Use the row count from our last operation
                    pass
            return True
        
        # Handle SET statements
        elif query.strip().upper().startswith('SET'):
            # Extract variable name and value
            if '=' in query:
                var_name = query.split('=')[0].strip().split(' ')[1].strip('@')
                # Handle specific variables
                if var_name == 'row_count':
                    # Use the row count from our last operation
                    pass
                elif var_name == 'error_message':
                    # Store error message
                    self.error_message = query.split('=')[1].strip().strip(';')
            return True
        
        # Handle BEGIN/END blocks and TRY/CATCH
        elif query.strip().upper() in ['BEGIN', 'END', 'BEGIN TRY', 'END TRY', 'BEGIN CATCH', 'END CATCH']:
            return True
        
        # Handle IF statements
        elif query.strip().upper().startswith('IF'):
            # Check if it's the row count condition
            if '@row_count = 0' in query:
                if self.row_count == 0:
                    # Simulate dropping the table for empty result
                    if 'employee_bkup' in self.tables:
                        del self.tables['employee_bkup']
            return True
        
        # Handle THROW statements
        elif query.strip().upper().startswith('THROW'):
            # Simulate throwing an error
            raise Exception(self.error_message or "Simulated error from THROW statement")
        
        return False
    
    def fetchall(self):
        """Return the current results"""
        if self.current_results is not None:
            return self.current_results.to_dict('records')
        return []
    
    def get_rowcount(self):
        """Return the row count from the last operation"""
        return self.row_count