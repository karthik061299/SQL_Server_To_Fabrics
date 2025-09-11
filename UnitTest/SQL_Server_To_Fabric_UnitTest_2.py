_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Enhanced unit tests for employee_bkup_refresh_fabric.sql script
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import patch, MagicMock

# Mock classes to simulate Fabric SQL environment
class MockFabricConnection:
    """Mock class to simulate Fabric SQL connection"""
    def __init__(self):
        self.executed_queries = []
        self.tables = {}
        self.current_results = None
        self.error_message = None
        self.row_count = 0
        self.log_entries = []
    
    def execute(self, query):
        """Execute a query and store it in executed_queries"""
        self.executed_queries.append(query)
        
        # Handle DROP TABLE IF EXISTS
        if query.strip().upper().startswith('DROP TABLE IF EXISTS'):
            table_name = query.strip().split()[3].replace(';', '')
            if table_name in self.tables:
                del self.tables[table_name]
            return True
        
        # Handle CREATE TABLE
        elif query.strip().upper().startswith('CREATE TABLE'):
            table_name = query.strip().split()[2].split('(')[0].strip()
            self.tables[table_name] = pd.DataFrame()
            return True
        
        # Handle INSERT INTO
        elif query.strip().upper().startswith('INSERT INTO'):
            # Extract table name
            table_name = query.strip().split()[2].split('(')[0].strip()
            
            # Handle operation_log table inserts
            if table_name == 'operation_log':
                # Parse the values from the INSERT statement
                if 'VALUES' in query:
                    values_str = query.split('VALUES')[1].strip().strip(';').strip('(').strip(')')
                    values = [v.strip().strip('\'').strip('"') for v in values_str.split(',')]
                    self.log_entries.append(values)
                return True
            
            # Handle regular table inserts
            if 'Employee' in self.tables and 'Salary' in self.tables:
                if 'empty_source' in self.tables.get('Employee', pd.DataFrame()).get('test_case', []):
                    # No data to insert for empty source
                    self.row_count = 0
                else:
                    # Simulate joining Employee and Salary tables
                    emp_df = self.tables['Employee']
                    sal_df = self.tables['Salary']
                    
                    # Filter out NULL EmployeeNo values
                    emp_df = emp_df[emp_df['EmployeeNo'].notna()]
                    
                    # Perform the join
                    if not emp_df.empty and not sal_df.empty:
                        merged_df = pd.merge(emp_df, sal_df, on='EmployeeNo', how='inner')
                        
                        # Create the result DataFrame
                        result_df = pd.DataFrame({
                            'EmployeeNo': merged_df['EmployeeNo'],
                            'FirstName': merged_df['FirstName'].apply(lambda x: x.strip() if isinstance(x, str) else x),
                            'LastName': merged_df['LastName'].apply(lambda x: x.strip() if isinstance(x, str) else x),
                            'DepartmentNo': merged_df['DepartmentNo'],
                            'NetPay': merged_df['NetPay'],
                            'BackupDate': datetime.now()
                        })
                        
                        # Store in the target table
                        self.tables[table_name] = result_df
                        self.row_count = len(result_df)
                    else:
                        self.row_count = 0
            return True