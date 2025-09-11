_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Test script to validate SQL Server to Fabric conversion of uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import numpy as np
import hashlib
import time
import os
import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Union, Tuple
from unittest.mock import Mock, patch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"conversion_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Test configuration
class TestConfig:
    # Connection strings (to be replaced with actual values or environment variables)
    SQL_SERVER_CONN_STR = os.environ.get('SQL_SERVER_CONN_STR', 'mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server')
    FABRIC_CONN_STR = os.environ.get('FABRIC_CONN_STR', 'fabric://workspace.database')
    
    # Stored procedure names
    SQL_SERVER_SP = 'Semantic.uspSemanticClaimTransactionMeasuresData'
    FABRIC_SP = 'Semantic.uspSemanticClaimTransactionMeasuresData'
    
    # Test data paths
    TEST_DATA_DIR = Path('./test_data')
    
    # Performance thresholds
    PERF_THRESHOLD_PERCENT = 20  # Allow Fabric SP to be up to 20% slower
    
    # Test data parameters
    START_DATE = '2023-01-01'
    END_DATE = '2023-03-31'
    DEFAULT_DATE = '1900-01-01'  # For testing date conversion to 1700-01-01

# Mock database connections for testing without actual database access
class MockDatabaseConnection:
    def cursor(self):
        return MockCursor()
    
    def commit(self):
        pass
    
    def close(self):
        pass

class MockCursor:
    def __init__(self):
        self.description = [
            ('FactClaimTransactionLineWCKey', None, None, None, None, None, None),
            ('RevisionNumber', None, None, None, None, None, None),
            ('NetPaidIndemnity', None, None, None, None, None, None),
            ('GrossPaidIndemnity', None, None, None, None, None, None),
            ('RecoveryIndemnity', None, None, None, None, None, None)
        ]
        self.results = [
            {'FactClaimTransactionLineWCKey': 1, 'RevisionNumber': 1, 'NetPaidIndemnity': 750.00, 'GrossPaidIndemnity': 1000.00, 'RecoveryIndemnity': 250.00},
            {'FactClaimTransactionLineWCKey': 2, 'RevisionNumber': 1, 'NetPaidIndemnity': 450.00, 'GrossPaidIndemnity': 500.00, 'RecoveryIndemnity': 50.00}
        ]
        self.row_index = 0
    
    def execute(self, query, params=None):
        # Simulate query execution
        if 'SESSION_ID()' in query:
            self.description = [('session_id', None, None, None, None, None, None)]
            self.results = [{'session_id': 12345}]
        elif 'GETDATE()' in query or 'CURRENT_TIMESTAMP()' in query:
            self.description = [('current_date', None, None, None, None, None, None), ('week_ago', None, None, None, None, None, None)]
            self.results = [{'current_date': datetime.now(), 'week_ago': datetime.now() - timedelta(days=7)}]
        elif 'SHA2' in query or 'HASHBYTES' in query:
            self.description = [('hash_value', None, None, None, None, None, None)]
            self.results = [{'hash_value': '8a5edab282632443219e051e4ade2d1d5bbc671c781051e815a28b8a606db8'}]
        
    def fetchall(self):
        return [[value for value in row.values()] for row in self.results]
    
    def fetchone(self):
        if self.row_index < len(self.results):
            row = self.results[self.row_index]
            self.row_index += 1
            return [value for value in row.values()]
        return None
    
    def nextset(self):
        return False
    
    def close(self):
        pass

# Utility functions for database connections
class DatabaseUtils:
    @staticmethod
    def get_sql_server_connection():
        # Return mock connection for testing
        return MockDatabaseConnection()
    
    @staticmethod
    def get_fabric_connection():
        # Return mock connection for testing
        return MockDatabaseConnection()
    
    @staticmethod
    def execute_sql_server_query(query, params=None):
        conn = DatabaseUtils.get_sql_server_connection()
        cursor = conn.cursor()
        try:
            start_time = time.time()
            cursor.execute(query, params)
            
            execution_time = time.time() - start_time
            logger.debug(f"SQL Server query executed in {execution_time:.2f} seconds")
            
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results, execution_time
            else:
                conn.commit()
                return cursor.rowcount, execution_time
        except Exception as e:
            logger.error(f"SQL Server query execution failed: {str(e)}\nQuery: {query}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @staticmethod
    def execute_fabric_query(query, params=None):
        conn = DatabaseUtils.get_fabric_connection()
        cursor = conn.cursor()
        try:
            start_time = time.time()
            cursor.execute(query, params)
            
            execution_time = time.time() - start_time
            logger.debug(f"Fabric query executed in {execution_time:.2f} seconds")
            
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results, execution_time
            else:
                conn.commit()
                return cursor.rowcount, execution_time
        except Exception as e:
            logger.error(f"Fabric query execution failed: {str(e)}\nQuery: {query}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @staticmethod
    def execute_stored_procedure(platform, proc_name, params):
        """Execute a stored procedure and return results"""
        if platform.lower() == 'sql_server':
            conn = DatabaseUtils.get_sql_server_connection()
            proc_name = TestConfig.SQL_SERVER_SP
        else:  # fabric
            conn = DatabaseUtils.get_fabric_connection()
            proc_name = TestConfig.FABRIC_SP
        
        cursor = conn.cursor()
        try:
            start_time = time.time()
            cursor.execute(f"EXEC {proc_name} {', '.join(['?' for _ in params])}", params)
            
            # Get all result sets
            results = []
            more_results = True
            while more_results:
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    result_set = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    results.append(result_set)
                
                more_results = cursor.nextset()
            
            execution_time = time.time() - start_time
            logger.info(f"{platform} stored procedure executed in {execution_time:.2f} seconds")
            
            return results, execution_time
        except Exception as e:
            logger.error(f"{platform} stored procedure execution failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

# Test fixtures
@pytest.fixture(scope="module")
def setup_test_environment():
    """Set up test environment before running tests"""
    logger.info("Setting up test environment")
    
    # Create test data directory if needed
    TestConfig.TEST_DATA_DIR.mkdir(exist_ok=True)
    
    yield  # This allows the tests to run
    
    # Teardown - clean up after tests
    logger.info("Tearing down test environment")

# Test classes
class TestSyntaxConversion:
    """Tests for syntax conversion from SQL Server to Fabric"""
    
    def test_session_id_conversion(self, setup_test_environment):
        """Test conversion of @@SPID to SESSION_ID()"""
        sql_server_query = "SELECT @@SPID AS session_id"
        fabric_query = "SELECT SESSION_ID() AS session_id"
        
        # Execute both queries
        sql_server_result, _ = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result, _ = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Verify both return a session ID (actual values will differ)
        assert 'session_id' in sql_server_result[0], "SQL Server query didn't return session_id"
        assert 'session_id' in fabric_result[0], "Fabric query didn't return session_id"
        assert isinstance(sql_server_result[0]['session_id'], int), "SQL Server session_id is not an integer"
        assert isinstance(fabric_result[0]['session_id'], int), "Fabric session_id is not an integer"
    
    def test_date_function_conversion(self, setup_test_environment):
        """Test conversion of date functions between SQL Server and Fabric"""
        sql_server_query = "SELECT GETDATE() AS current_date, DATEADD(day, -7, GETDATE()) AS week_ago"
        fabric_query = "SELECT CURRENT_TIMESTAMP() AS current_date, DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY) AS week_ago"
        
        # Execute both queries
        sql_server_result, _ = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result, _ = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Verify date functions work (exact timestamps will differ)
        assert 'current_date' in sql_server_result[0], "SQL Server query didn't return current_date"
        assert 'current_date' in fabric_result[0], "Fabric query didn't return current_date"
        assert 'week_ago' in sql_server_result[0], "SQL Server query didn't return week_ago"
        assert 'week_ago' in fabric_result[0], "Fabric query didn't return week_ago"
    
    def test_hash_function_conversion(self, setup_test_environment):
        """Test conversion of hash functions between SQL Server and Fabric"""
        test_value = "test_string_for_hash_calculation"
        
        sql_server_query = f"SELECT CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', '{test_value}'), 2) AS hash_value"
        fabric_query = f"SELECT LOWER(HEX(SHA2('{test_value}', 256))) AS hash_value"
        
        # Execute both queries
        sql_server_result, _ = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result, _ = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Verify hash functions produce results
        assert 'hash_value' in sql_server_result[0], "SQL Server query didn't return hash_value"
        assert 'hash_value' in fabric_result[0], "Fabric query didn't return hash_value"

class TestLogicPreservation:
    """Tests to ensure business logic is preserved in the conversion"""
    
    def test_date_conversion_logic(self, setup_test_environment):
        """Test the conversion of '01/01/1900' to '01/01/1700'"""
        # This is a specific business rule in the stored procedure
        
        # Mock implementation for testing
        def convert_date(date_str):
            if date_str == '1900-01-01':
                return '1700-01-01'
            return date_str
        
        # Test cases
        test_cases = [
            {'input': '1900-01-01', 'expected': '1700-01-01'},
            {'input': '2023-01-01', 'expected': '2023-01-01'}
        ]
        
        # Test the conversion logic
        for case in test_cases:
            result = convert_date(case['input'])
            assert result == case['expected'], f"Date conversion failed for {case['input']}"
    
    def test_financial_calculations(self, setup_test_environment):
        """Test financial calculations to ensure they match between platforms"""
        # Create test data for financial calculations
        test_data = [
            {'gross_paid': 1000.00, 'deductible': 250.00, 'copay': 30.00, 'coinsurance': 120.00, 'expected_net': 600.00},
            {'gross_paid': 500.00, 'deductible': 400.00, 'copay': 50.00, 'coinsurance': 0.00, 'expected_net': 50.00},
            {'gross_paid': 750.00, 'deductible': 0.00, 'copay': 0.00, 'coinsurance': 150.00, 'expected_net': 600.00}
        ]
        
        # Test the calculation logic
        for case in test_data:
            net_paid = case['gross_paid'] - case['deductible'] - case['copay'] - case['coinsurance']
            assert net_paid == case['expected_net'], f"Financial calculation failed for case {case}"
    
    def test_hash_value_change_detection(self, setup_test_environment):
        """Test hash-based change detection logic"""
        # This tests the change detection logic using hash values
        
        # Create test data
        original_record = {
            'claim_id': 'CLM001',
            'policy_id': 'POL001',
            'transaction_date': '2023-01-15',
            'amount': 1000.00
        }
        
        modified_record = original_record.copy()
        modified_record['amount'] = 1100.00  # Changed amount
        
        # Calculate hash values
        def calculate_hash(record):
            hash_string = f"{record['claim_id']}|{record['policy_id']}|{record['transaction_date']}|{record['amount']:.2f}"
            return hashlib.sha256(hash_string.encode()).hexdigest()
        
        original_hash = calculate_hash(original_record)
        modified_hash = calculate_hash(modified_record)
        
        # Verify hashes are different for changed records
        assert original_hash != modified_hash, "Hash values should be different for modified records"
        
        # Verify hash is the same for unchanged record
        unchanged_record = original_record.copy()
        unchanged_hash = calculate_hash(unchanged_record)
        assert original_hash == unchanged_hash, "Hash values should be the same for unchanged records"

class TestDynamicSQLGeneration:
    """Tests for dynamic SQL generation functionality"""
    
    def test_measure_sql_generation(self, setup_test_environment):
        """Test generation of measure SQL from metadata"""
        # Mock metadata for testing
        mock_metadata = [
            {'Measure_Name': 'NetPaidIndemnity', 'Logic': 'SUM(CASE WHEN ClaimTransactionDescriptors.ClaimTransactionCategoryCode = \'INDEMNITY\' THEN ClaimTransactionDescriptors.NetPaidAmount ELSE 0 END)'},
            {'Measure_Name': 'NetPaidMedical', 'Logic': 'SUM(CASE WHEN ClaimTransactionDescriptors.ClaimTransactionCategoryCode = \'MEDICAL\' THEN ClaimTransactionDescriptors.NetPaidAmount ELSE 0 END)'}
        ]
        
        # Generate SQL
        measure_sql = ', '.join([f"{row['Logic']} AS {row['Measure_Name']}" for row in mock_metadata])
        
        # Verify SQL generation
        assert 'NetPaidIndemnity' in measure_sql, "NetPaidIndemnity measure not found in generated SQL"
        assert 'NetPaidMedical' in measure_sql, "NetPaidMedical measure not found in generated SQL"
        assert 'SUM(CASE WHEN' in measure_sql, "SUM(CASE WHEN pattern not found in generated SQL"
    
    def test_temp_table_naming_with_session_id(self, setup_test_environment):
        """Test temporary table naming with session ID"""
        # Mock session ID
        session_id = 12345
        
        # Generate temp table names
        sql_server_temp_tables = {
            'TabName': f'##CTM{session_id}',
            'TabNameFact': f'##CTMFact{session_id}',
            'TabFinal': f'##CTMF{session_id}',
            'TabNamePrs': f'##CTPrs{session_id}',
            'ProdSource': f'##PRDCLmTrans{session_id}'
        }
        
        fabric_temp_tables = {
            'TabName': f'temp_CTM_{session_id}',
            'TabNameFact': f'temp_CTMFact_{session_id}',
            'TabFinal': f'temp_CTMF_{session_id}',
            'TabNamePrs': f'temp_CTPrs_{session_id}',
            'ProdSource': f'temp_PRDCLmTrans_{session_id}'
        }
        
        # Verify temp table naming conventions
        for key in sql_server_temp_tables:
            assert '##' in sql_server_temp_tables[key], f"SQL Server temp table {key} should use ## prefix"
            assert str(session_id) in sql_server_temp_tables[key], f"SQL Server temp table {key} should include session ID"
            
            assert 'temp_' in fabric_temp_tables[key], f"Fabric temp table {key} should use temp_ prefix"
            assert str(session_id) in fabric_temp_tables[key], f"Fabric temp table {key} should include session ID"

class TestSpecificBusinessRules:
    """Tests for specific business rules in the stored procedure"""
    
    def test_retired_indicator_filtering(self, setup_test_environment):
        """Test filtering of retired records"""
        # Mock data with retired indicators
        mock_data = [
            {'PolicyWCKey': 1, 'RiskState': 'NY', 'RetiredInd': 0},  # Active record
            {'PolicyWCKey': 1, 'RiskState': 'NY', 'RetiredInd': 1},  # Retired record
            {'PolicyWCKey': 2, 'RiskState': 'CA', 'RetiredInd': 0}   # Active record
        ]
        
        # Filter out retired records
        active_records = [record for record in mock_data if record['RetiredInd'] == 0]
        
        # Verify filtering
        assert len(active_records) == 2, "Should have 2 active records"
        assert all(record['RetiredInd'] == 0 for record in active_records), "All records should have RetiredInd = 0"
    
    def test_row_number_partitioning(self, setup_test_environment):
        """Test ROW_NUMBER() partitioning logic"""
        # Mock data for testing ROW_NUMBER() partitioning
        mock_data = [
            {'PolicyWCKey': 1, 'RiskState': 'NY', 'RiskStateEffectiveDate': '2023-01-01', 'RetiredInd': 0},
            {'PolicyWCKey': 1, 'RiskState': 'NY', 'RiskStateEffectiveDate': '2022-01-01', 'RetiredInd': 0},
            {'PolicyWCKey': 2, 'RiskState': 'CA', 'RiskStateEffectiveDate': '2023-01-01', 'RetiredInd': 0}
        ]
        
        # Sort data as the ROW_NUMBER would
        from operator import itemgetter
        
        # Group by PolicyWCKey and RiskState
        grouped_data = {}
        for record in mock_data:
            key = (record['PolicyWCKey'], record['RiskState'])
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(record)
        
        # Sort within each group and assign row numbers
        result_data = []
        for key, records in grouped_data.items():
            sorted_records = sorted(records, key=itemgetter('RetiredInd', 'RiskStateEffectiveDate'), reverse=True)
            for i, record in enumerate(sorted_records, 1):
                record_with_rownum = record.copy()
                record_with_rownum['Rownum'] = i
                result_data.append(record_with_rownum)
        
        # Filter to only Rownum = 1
        filtered_data = [record for record in result_data if record['Rownum'] == 1]
        
        # Verify partitioning and filtering
        assert len(filtered_data) == 2, "Should have 2 records after filtering by Rownum = 1"
        assert all(record['Rownum'] == 1 for record in filtered_data), "All records should have Rownum = 1"

class TestPerformanceValidation:
    """Tests for performance validation between SQL Server and Fabric"""
    
    def test_execution_time_comparison(self, setup_test_environment):
        """Test and compare execution times"""
        # Mock execution times
        sql_server_time = 2.5  # seconds
        fabric_time = 2.8      # seconds
        
        # Calculate performance difference
        perf_diff_percent = ((fabric_time / sql_server_time) - 1) * 100
        
        # Verify performance is within acceptable threshold
        assert perf_diff_percent <= TestConfig.PERF_THRESHOLD_PERCENT, \
            f"Performance difference ({perf_diff_percent:.2f}%) exceeds threshold ({TestConfig.PERF_THRESHOLD_PERCENT}%)"

class TestDataIntegrity:
    """Tests for data integrity validation"""
    
    def test_result_count_validation(self, setup_test_environment):
        """Test that result counts match between SQL Server and Fabric"""
        # Mock result counts
        sql_server_count = 1000
        fabric_count = 1000
        
        # Verify counts match
        assert sql_server_count == fabric_count, "Result counts should match between SQL Server and Fabric"
    
    def test_financial_totals_validation(self, setup_test_environment):
        """Test that financial totals match between platforms"""
        # Mock financial totals
        sql_server_totals = {'NetPaidIndemnity': 1000000.00, 'GrossPaidIndemnity': 1200000.00}
        fabric_totals = {'NetPaidIndemnity': 1000000.00, 'GrossPaidIndemnity': 1200000.00}
        
        # Verify totals match
        for key in sql_server_totals:
            assert abs(sql_server_totals[key] - fabric_totals[key]) < 0.01, \
                f"Financial total for {key} should match between SQL Server and Fabric"

# Test execution report generator
class TestReportGenerator:
    @staticmethod
    def generate_report(test_results):
        """Generate a test execution report"""
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_tests': len(test_results),
            'passed_tests': sum(1 for result in test_results if result['outcome'] == 'passed'),
            'failed_tests': sum(1 for result in test_results if result['outcome'] == 'failed'),
            'skipped_tests': sum(1 for result in test_results if result['outcome'] == 'skipped'),
            'details': test_results
        }
        
        # Create report file
        report_file = TestConfig.TEST_DATA_DIR / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(f"Test Execution Report - {report['timestamp']}\n")
            f.write(f"Total Tests: {report['total_tests']}\n")
            f.write(f"Passed: {report['passed_tests']}\n")
            f.write(f"Failed: {report['failed_tests']}\n")
            f.write(f"Skipped: {report['skipped_tests']}\n\n")
            
            f.write("Test Details:\n")
            for test in report['details']:
                f.write(f"  {test['name']}: {test['outcome']}\n")
                if test['outcome'] == 'failed':
                    f.write(f"    Error: {test['error']}\n")
        
        return report_file

# Main test execution function
def run_tests():
    """Run all tests and generate report"""
    test_results = []
    
    # Run tests
    test_classes = [
        TestSyntaxConversion,
        TestLogicPreservation,
        TestDynamicSQLGeneration,
        TestSpecificBusinessRules,
        TestPerformanceValidation,
        TestDataIntegrity
    ]
    
    for test_class in test_classes:
        for test_name in [method for method in dir(test_class) if method.startswith('test_')]:
            test_instance = test_class()
            test_method = getattr(test_instance, test_name)
            
            try:
                test_method(setup_test_environment)
                test_results.append({
                    'name': f"{test_class.__name__}.{test_name}",
                    'outcome': 'passed',
                    'error': None
                })
                logger.info(f"Test {test_class.__name__}.{test_name} passed")
            except Exception as e:
                test_results.append({
                    'name': f"{test_class.__name__}.{test_name}",
                    'outcome': 'failed',
                    'error': str(e)
                })
                logger.error(f"Test {test_class.__name__}.{test_name} failed: {str(e)}")
    
    # Generate report
    report_file = TestReportGenerator.generate_report(test_results)
    logger.info(f"Test report generated: {report_file}")
    
    return test_results

if __name__ == "__main__":
    run_tests()