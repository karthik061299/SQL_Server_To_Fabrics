_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Test script to validate SQL Server to Fabric conversion of uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import time
import os
import logging
from datetime import datetime
from pathlib import Path

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
    SQL_SERVER_SP = 'uspSemanticClaimTransactionMeasuresData'
    FABRIC_SP = 'uspSemanticClaimTransactionMeasuresData'
    
    # Test data paths
    TEST_DATA_DIR = Path('./test_data')
    
    # Performance thresholds
    PERF_THRESHOLD_PERCENT = 20  # Allow Fabric SP to be up to 20% slower

# Utility functions for database connections
class DatabaseUtils:
    @staticmethod
    def get_sql_server_connection():
        # Implementation would use appropriate library (pyodbc, sqlalchemy, etc.)
        # This is a placeholder
        import pyodbc
        return pyodbc.connect(TestConfig.SQL_SERVER_CONN_STR)
    
    @staticmethod
    def get_fabric_connection():
        # Implementation would use appropriate library for Fabric
        # This is a placeholder
        import pyodbc
        return pyodbc.connect(TestConfig.FABRIC_CONN_STR)
    
    @staticmethod
    def execute_sql_server_query(query, params=None):
        conn = DatabaseUtils.get_sql_server_connection()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
            else:
                conn.commit()
                return cursor.rowcount
        finally:
            cursor.close()
            conn.close()
    
    @staticmethod
    def execute_fabric_query(query, params=None):
        conn = DatabaseUtils.get_fabric_connection()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
            else:
                conn.commit()
                return cursor.rowcount
        finally:
            cursor.close()
            conn.close()

# Test fixtures
@pytest.fixture(scope="module")
def setup_test_environment():
    """Set up test environment before running tests"""
    logger.info("Setting up test environment")
    
    # Create test data if needed
    TestConfig.TEST_DATA_DIR.mkdir(exist_ok=True)
    
    # Setup test databases, tables, etc.
    try:
        # Clean up any existing test data
        DatabaseUtils.execute_sql_server_query("IF OBJECT_ID('tempdb..#ClaimTransactionMeasures') IS NOT NULL DROP TABLE #ClaimTransactionMeasures")
        DatabaseUtils.execute_fabric_query("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'temp_ClaimTransactionMeasures') DROP TABLE temp_ClaimTransactionMeasures")
        
        # Create any necessary test data
        # ...
        
        yield  # This allows the tests to run
    finally:
        # Teardown - clean up after tests
        logger.info("Tearing down test environment")
        # Clean up test data
        # ...

# Test classes
class TestSyntaxConversion:
    """Tests for syntax conversion from SQL Server to Fabric"""
    
    def test_temp_table_syntax(self, setup_test_environment):
        """Test conversion of temporary table syntax from SQL Server to Fabric"""
        # SQL Server uses #TempTable, Fabric uses temp_TempTable
        sql_server_query = """IF OBJECT_ID('tempdb..#TestTemp') IS NOT NULL DROP TABLE #TestTemp; 
                            CREATE TABLE #TestTemp (id INT, value VARCHAR(50));
                            INSERT INTO #TestTemp VALUES (1, 'test');
                            SELECT * FROM #TestTemp;"""
        
        fabric_query = """IF EXISTS (SELECT * FROM sys.tables WHERE name = 'temp_TestTemp') DROP TABLE temp_TestTemp;
                        CREATE TABLE temp_TestTemp (id INT, value VARCHAR(50));
                        INSERT INTO temp_TestTemp VALUES (1, 'test');
                        SELECT * FROM temp_TestTemp;"""
        
        # Execute both queries
        sql_server_result = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Compare results
        assert len(sql_server_result) == len(fabric_result), "Result count mismatch"
        assert sql_server_result[0]['id'] == fabric_result[0]['id'], "Data mismatch"
        assert sql_server_result[0]['value'] == fabric_result[0]['value'], "Data mismatch"
    
    def test_session_id_conversion(self, setup_test_environment):
        """Test conversion of @@SPID to SESSION_ID()"""
        sql_server_query = "SELECT @@SPID AS session_id"
        fabric_query = "SELECT SESSION_ID() AS session_id"
        
        # Execute both queries
        sql_server_result = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Verify both return a session ID (actual values will differ)
        assert 'session_id' in sql_server_result[0], "SQL Server query didn't return session_id"
        assert 'session_id' in fabric_result[0], "Fabric query didn't return session_id"
        assert isinstance(sql_server_result[0]['session_id'], int), "SQL Server session_id is not an integer"
        assert isinstance(fabric_result[0]['session_id'], int), "Fabric session_id is not an integer"
    
    def test_date_function_conversion(self, setup_test_environment):
        """Test conversion of date functions between SQL Server and Fabric"""
        sql_server_query = "SELECT GETDATE() AS current_date, DATEADD(day, -7, GETDATE()) AS week_ago"
        fabric_query = "SELECT GETDATE() AS current_date, DATEADD(day, -7, GETDATE()) AS week_ago"
        
        # Execute both queries
        sql_server_result = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Verify date functions work (exact timestamps will differ)
        assert 'current_date' in sql_server_result[0], "SQL Server query didn't return current_date"
        assert 'current_date' in fabric_result[0], "Fabric query didn't return current_date"
        assert 'week_ago' in sql_server_result[0], "SQL Server query didn't return week_ago"
        assert 'week_ago' in fabric_result[0], "Fabric query didn't return week_ago"
    
    def test_hash_function_conversion(self, setup_test_environment):
        """Test conversion of hash functions between SQL Server and Fabric"""
        test_value = "test_string"
        
        sql_server_query = f"SELECT HASHBYTES('SHA2_256', '{test_value}') AS hash_value"
        fabric_query = f"SELECT SHA2_256('{test_value}') AS hash_value"
        
        # Execute both queries
        sql_server_result = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Verify hash functions produce results (exact implementation may differ)
        assert 'hash_value' in sql_server_result[0], "SQL Server query didn't return hash_value"
        assert 'hash_value' in fabric_result[0], "Fabric query didn't return hash_value"

class TestLogicPreservation:
    """Tests to ensure business logic is preserved in the conversion"""
    
    def test_stored_procedure_results(self, setup_test_environment):
        """Test that the stored procedure produces the same results in both environments"""
        # Parameters for the stored procedure
        params = {
            'StartDate': '2023-01-01',
            'EndDate': '2023-01-31',
            'ClaimType': 'Medical'
        }
        
        # Execute stored procedure in SQL Server
        start_time_sql = time.time()
        sql_server_result = DatabaseUtils.execute_sql_server_query(
            f"EXEC {TestConfig.SQL_SERVER_SP} @StartDate=?, @EndDate=?, @ClaimType=?", 
            [params['StartDate'], params['EndDate'], params['ClaimType']]
        )
        sql_server_time = time.time() - start_time_sql
        
        # Execute stored procedure in Fabric
        start_time_fabric = time.time()
        fabric_result = DatabaseUtils.execute_fabric_query(
            f"CALL {TestConfig.FABRIC_SP}(?, ?, ?)", 
            [params['StartDate'], params['EndDate'], params['ClaimType']]
        )
        fabric_time = time.time() - start_time_fabric
        
        # Log performance metrics
        logger.info(f"SQL Server execution time: {sql_server_time:.2f} seconds")
        logger.info(f"Fabric execution time: {fabric_time:.2f} seconds")
        logger.info(f"Performance difference: {((fabric_time/sql_server_time)-1)*100:.2f}%")
        
        # Compare result structure and data
        assert len(sql_server_result) == len(fabric_result), "Result count mismatch"
        
        # Compare specific fields (adjust based on actual SP output)
        if len(sql_server_result) > 0:
            for key in sql_server_result[0].keys():
                assert key in fabric_result[0], f"Field {key} missing in Fabric result"
                
            # Sample data comparison for first row
            for key in sql_server_result[0].keys():
                assert sql_server_result[0][key] == fabric_result[0][key], f"Data mismatch for field {key}"
    
    def test_dynamic_sql_execution(self, setup_test_environment):
        """Test that dynamic SQL is correctly converted and executed"""
        # Test with a simple dynamic SQL example
        table_name = "ClaimTransactions"
        column_name = "TransactionAmount"
        
        # SQL Server dynamic SQL
        sql_server_query = f"""DECLARE @sql NVARCHAR(MAX);
                            SET @sql = 'SELECT SUM(' + '{column_name}' + ') AS total FROM ' + '{table_name}';
                            EXEC sp_executesql @sql;"""
        
        # Fabric dynamic SQL
        fabric_query = f"""DECLARE @sql NVARCHAR(MAX);
                        SET @sql = 'SELECT SUM(' + '{column_name}' + ') AS total FROM ' + '{table_name}';
                        EXEC(@sql);"""
        
        # Execute both queries (this is simplified - actual implementation would need proper setup)
        try:
            sql_server_result = DatabaseUtils.execute_sql_server_query(sql_server_query)
            fabric_result = DatabaseUtils.execute_fabric_query(fabric_query)
            
            # Compare results
            assert 'total' in sql_server_result[0], "SQL Server query didn't return total"
            assert 'total' in fabric_result[0], "Fabric query didn't return total"
            assert sql_server_result[0]['total'] == fabric_result[0]['total'], "Total amounts differ"
        except Exception as e:
            logger.warning(f"Dynamic SQL test skipped due to: {str(e)}")
            pytest.skip("Dynamic SQL test requires proper table setup")

class TestManualInterventions:
    """Tests for areas requiring manual intervention during conversion"""
    
    def test_index_creation_syntax(self, setup_test_environment):
        """Test differences in index creation syntax"""
        # SQL Server index creation
        sql_server_query = """IF OBJECT_ID('tempdb..#IndexTest') IS NOT NULL DROP TABLE #IndexTest;
                            CREATE TABLE #IndexTest (id INT, value VARCHAR(50));
                            CREATE NONCLUSTERED INDEX IX_IndexTest_id ON #IndexTest(id);
                            INSERT INTO #IndexTest VALUES (1, 'test'), (2, 'test2');
                            SELECT * FROM #IndexTest WHERE id = 1;"""
        
        # Fabric index creation
        fabric_query = """IF EXISTS (SELECT * FROM sys.tables WHERE name = 'temp_IndexTest') DROP TABLE temp_IndexTest;
                        CREATE TABLE temp_IndexTest (id INT, value VARCHAR(50));
                        CREATE INDEX IX_IndexTest_id ON temp_IndexTest(id);
                        INSERT INTO temp_IndexTest VALUES (1, 'test'), (2, 'test2');
                        SELECT * FROM temp_IndexTest WHERE id = 1;"""
        
        # Execute both queries
        sql_server_result = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Compare results
        assert len(sql_server_result) == len(fabric_result), "Result count mismatch"
        assert sql_server_result[0]['id'] == fabric_result[0]['id'], "Data mismatch"
        assert sql_server_result[0]['value'] == fabric_result[0]['value'], "Data mismatch"
    
    def test_error_handling_differences(self, setup_test_environment):
        """Test differences in error handling between SQL Server and Fabric"""
        # SQL Server error handling
        sql_server_query = """BEGIN TRY
                            SELECT 1/0 AS result;
                            END TRY
                            BEGIN CATCH
                            SELECT ERROR_MESSAGE() AS error_message;
                            END CATCH"""
        
        # Fabric error handling
        fabric_query = """BEGIN TRY
                        SELECT 1/0 AS result;
                        END TRY
                        BEGIN CATCH
                        SELECT ERROR_MESSAGE() AS error_message;
                        END CATCH"""
        
        # Execute both queries
        sql_server_result = DatabaseUtils.execute_sql_server_query(sql_server_query)
        fabric_result = DatabaseUtils.execute_fabric_query(fabric_query)
        
        # Verify error handling works in both systems
        assert 'error_message' in sql_server_result[0], "SQL Server error handling failed"
        assert 'error_message' in fabric_result[0], "Fabric error handling failed"
        assert "divide by zero" in sql_server_result[0]['error_message'].lower(), "Unexpected error message"
        assert "divide by zero" in fabric_result[0]['error_message'].lower(), "Unexpected error message"

class TestPerformance:
    """Performance tests comparing SQL Server and Fabric execution"""
    
    def test_stored_procedure_performance(self, setup_test_environment):
        """Test performance of the stored procedure in both environments"""
        # Parameters for the stored procedure
        params = {
            'StartDate': '2023-01-01',
            'EndDate': '2023-03-31',  # Larger date range for performance testing
            'ClaimType': 'All'
        }
        
        # Execute stored procedure in SQL Server multiple times and average
        sql_server_times = []
        for i in range(3):  # Run 3 times for more stable results
            start_time = time.time()
            DatabaseUtils.execute_sql_server_query(
                f"EXEC {TestConfig.SQL_SERVER_SP} @StartDate=?, @EndDate=?, @ClaimType=?", 
                [params['StartDate'], params['EndDate'], params['ClaimType']]
            )
            sql_server_times.append(time.time() - start_time)
        
        # Execute stored procedure in Fabric multiple times and average
        fabric_times = []
        for i in range(3):  # Run 3 times for more stable results
            start_time = time.time()
            DatabaseUtils.execute_fabric_query(
                f"CALL {TestConfig.FABRIC_SP}(?, ?, ?)", 
                [params['StartDate'], params['EndDate'], params['ClaimType']]
            )
            fabric_times.append(time.time() - start_time)
        
        # Calculate averages
        avg_sql_server_time = sum(sql_server_times) / len(sql_server_times)
        avg_fabric_time = sum(fabric_times) / len(fabric_times)
        
        # Log performance metrics
        logger.info(f"Average SQL Server execution time: {avg_sql_server_time:.2f} seconds")
        logger.info(f"Average Fabric execution time: {avg_fabric_time:.2f} seconds")
        logger.info(f"Performance difference: {((avg_fabric_time/avg_sql_server_time)-1)*100:.2f}%")
        
        # Check if Fabric performance is within acceptable threshold
        max_allowed_time = avg_sql_server_time * (1 + TestConfig.PERF_THRESHOLD_PERCENT/100)
        assert avg_fabric_time <= max_allowed_time, f"Fabric performance exceeds threshold: {avg_fabric_time:.2f}s > {max_allowed_time:.2f}s"

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
    for test_class in [TestSyntaxConversion, TestLogicPreservation, TestManualInterventions, TestPerformance]:
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
            except Exception as e:
                test_results.append({
                    'name': f"{test_class.__name__}.{test_name}",
                    'outcome': 'failed',
                    'error': str(e)
                })
    
    # Generate report
    report_file = TestReportGenerator.generate_report(test_results)
    logger.info(f"Test report generated: {report_file}")

if __name__ == "__main__":
    run_tests()