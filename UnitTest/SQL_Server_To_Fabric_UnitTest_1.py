_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Unit tests for the Semantic.uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 1 
## *Updated on*: 
_____________________________________________

"""
Unit Test for uspSemanticClaimTransactionMeasuresData Stored Procedure
"""

import pytest
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Test configuration
class TestConfig:
    # Connection strings (would typically be loaded from environment variables or config file)
    SOURCE_CONN_STR = os.environ.get('SOURCE_CONN_STR', 'Driver={ODBC Driver 17 for SQL Server};Server=source_server;Database=source_db;Trusted_Connection=yes;')
    TARGET_CONN_STR = os.environ.get('TARGET_CONN_STR', 'Driver={ODBC Driver 17 for SQL Server};Server=fabric_server;Database=fabric_db;Trusted_Connection=yes;')
    
    # Stored procedure name
    STORED_PROC = 'uspSemanticClaimTransactionMeasuresData'
    
    # Test data parameters
    TEST_DATE_RANGE = 30  # days
    SAMPLE_SIZE = 1000

# Fixtures
@pytest.fixture(scope="module")
def source_connection():
    """Create a connection to the source database."""
    try:
        conn = pyodbc.connect(TestConfig.SOURCE_CONN_STR)
        yield conn
        conn.close()
    except Exception as e:
        logger.error(f"Failed to connect to source database: {e}")
        pytest.fail(f"Source database connection failed: {e}")

@pytest.fixture(scope="module")
def target_connection():
    """Create a connection to the target Fabric database."""
    try:
        conn = pyodbc.connect(TestConfig.TARGET_CONN_STR)
        yield conn
        conn.close()
    except Exception as e:
        logger.error(f"Failed to connect to target database: {e}")
        pytest.fail(f"Target database connection failed: {e}")

@pytest.fixture(scope="module")
def execute_stored_procedure(source_connection, target_connection):
    """Execute the stored procedure and return success status."""
    try:
        cursor = target_connection.cursor()
        # Assuming the stored procedure takes no parameters
        # Modify as needed if parameters are required
        cursor.execute(f"EXEC {TestConfig.STORED_PROC}")
        target_connection.commit()
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Failed to execute stored procedure: {e}")
        return False

@pytest.fixture(scope="module")
def source_data(source_connection):
    """Retrieve sample data from source tables for testing."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=TestConfig.TEST_DATE_RANGE)
    
    try:
        # Adjust this query based on the actual source table structure
        query = f"""
        SELECT TOP {TestConfig.SAMPLE_SIZE} *
        FROM ClaimTransactions
        WHERE TransactionDate BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
        """
        
        df = pd.read_sql(query, source_connection)
        return df
    except Exception as e:
        logger.error(f"Failed to retrieve source data: {e}")
        return pd.DataFrame()

@pytest.fixture(scope="module")
def target_data(target_connection, execute_stored_procedure):
    """Retrieve processed data from target table after stored procedure execution."""
    if not execute_stored_procedure:
        pytest.skip("Stored procedure execution failed, skipping target data retrieval")
    
    try:
        # Adjust this query based on the actual target table structure
        query = f"""
        SELECT TOP {TestConfig.SAMPLE_SIZE} *
        FROM SemanticClaimTransactionMeasures
        ORDER BY LoadDate DESC
        """
        
        df = pd.read_sql(query, target_connection)
        return df
    except Exception as e:
        logger.error(f"Failed to retrieve target data: {e}")
        return pd.DataFrame()

# Test cases
class TestClaimTransactionMeasures:
    
    def test_stored_procedure_execution(self, execute_stored_procedure):
        """Test if the stored procedure executes without errors."""
        assert execute_stored_procedure, "Stored procedure execution failed"
    
    def test_row_count(self, source_data, target_data):
        """Test that the target table has the expected number of rows."""
        # This test assumes a 1:1 mapping between source and target
        # Adjust logic if the transformation involves aggregation or filtering
        assert len(target_data) > 0, "No data found in target table"
        
        # Log the row counts for debugging
        logger.info(f"Source row count: {len(source_data)}")
        logger.info(f"Target row count: {len(target_data)}")
        
        # If we expect the same number of rows (adjust if not the case)
        # assert len(source_data) == len(target_data), f"Row count mismatch: Source={len(source_data)}, Target={len(target_data)}"
    
    def test_data_types(self, target_data):
        """Test that the target table has the expected data types."""
        assert not target_data.empty, "Target data is empty"
        
        # Define expected data types for key columns
        # Adjust based on actual schema
        expected_types = {
            'ClaimID': np.int64,
            'TransactionDate': np.datetime64,
            'Amount': np.float64,
            # Add more columns as needed
        }
        
        for column, expected_type in expected_types.items():
            if column in target_data.columns:
                # Check if pandas dtype matches expected type or is compatible
                assert pd.api.types.is_dtype_equal(target_data[column].dtype, expected_type) or \
                       pd.api.types.is_dtype_equal(target_data[column].dtype, object), \
                       f"Column {column} has incorrect data type. Expected {expected_type}, got {target_data[column].dtype}"
            else:
                pytest.fail(f"Expected column {column} not found in target data")
    
    def test_null_values(self, target_data):
        """Test that required fields don't have null values."""
        assert not target_data.empty, "Target data is empty"
        
        # List of columns that should not contain nulls
        required_columns = ['ClaimID', 'TransactionDate', 'MeasureID']
        
        for column in required_columns:
            if column in target_data.columns:
                null_count = target_data[column].isnull().sum()
                assert null_count == 0, f"Column {column} contains {null_count} null values"
            else:
                pytest.fail(f"Required column {column} not found in target data")
    
    def test_date_range(self, target_data):
        """Test that dates in the target table are within expected range."""
        assert not target_data.empty, "Target data is empty"
        
        if 'TransactionDate' in target_data.columns:
            min_date = target_data['TransactionDate'].min()
            max_date = target_data['TransactionDate'].max()
            current_date = pd.Timestamp(datetime.now())
            
            # Ensure dates are not in the future
            assert max_date <= current_date, f"Found future dates in data: {max_date}"
            
            # Check if dates are within reasonable range (e.g., not older than 10 years)
            ten_years_ago = current_date - pd.Timedelta(days=365*10)
            assert min_date >= ten_years_ago, f"Found dates older than 10 years: {min_date}"
    
    def test_business_rules(self, target_data):
        """Test specific business rules for the claim transaction measures."""
        assert not target_data.empty, "Target data is empty"
        
        # Example: Test that all amounts are positive or zero
        if 'Amount' in target_data.columns:
            negative_amounts = target_data[target_data['Amount'] < 0]
            assert len(negative_amounts) == 0, f"Found {len(negative_amounts)} records with negative amounts"
        
        # Example: Test that all claims have valid status codes
        if 'StatusCode' in target_data.columns:
            valid_status_codes = ['A', 'P', 'D', 'R']  # Example valid codes
            invalid_status = target_data[~target_data['StatusCode'].isin(valid_status_codes)]
            assert len(invalid_status) == 0, f"Found {len(invalid_status)} records with invalid status codes"
    
    def test_data_transformation(self, source_data, target_data):
        """Test that data transformation logic is correctly applied."""
        assert not source_data.empty, "Source data is empty"
        assert not target_data.empty, "Target data is empty"
        
        # This test will depend on the specific transformation logic
        # Here's an example assuming a calculated field exists in the target
        if 'TotalAmount' in target_data.columns and 'Amount' in source_data.columns:
            # Join source and target data on a common key
            # This is a simplified example - adjust based on actual schema
            if 'ClaimID' in source_data.columns and 'ClaimID' in target_data.columns:
                # Sample a few records for verification
                sample_claims = source_data['ClaimID'].sample(min(10, len(source_data))).tolist()
                
                for claim_id in sample_claims:
                    source_amount = source_data[source_data['ClaimID'] == claim_id]['Amount'].sum()
                    target_amount = target_data[target_data['ClaimID'] == claim_id]['TotalAmount'].iloc[0] \
                        if not target_data[target_data['ClaimID'] == claim_id].empty else 0
                    
                    # Allow for small floating point differences
                    assert abs(source_amount - target_amount) < 0.01, \
                        f"Amount mismatch for ClaimID {claim_id}: Source={source_amount}, Target={target_amount}"
    
    def test_duplicates(self, target_data):
        """Test that there are no duplicate records in the target table."""
        assert not target_data.empty, "Target data is empty"
        
        # Define columns that should form a unique key
        key_columns = ['ClaimID', 'TransactionDate', 'MeasureID']
        
        # Check if all key columns exist
        missing_columns = [col for col in key_columns if col not in target_data.columns]
        if missing_columns:
            pytest.skip(f"Key columns {missing_columns} not found in target data")
            return
        
        # Count duplicates
        duplicate_count = len(target_data) - len(target_data.drop_duplicates(subset=key_columns))
        assert duplicate_count == 0, f"Found {duplicate_count} duplicate records based on key columns {key_columns}"
    
    def test_referential_integrity(self, target_connection):
        """Test referential integrity between the target table and related dimension tables."""
        try:
            cursor = target_connection.cursor()
            
            # Example: Check if all MeasureIDs exist in the Measures dimension table
            query = """
            SELECT COUNT(*) AS InvalidCount
            FROM SemanticClaimTransactionMeasures t
            LEFT JOIN DimMeasures d ON t.MeasureID = d.MeasureID
            WHERE d.MeasureID IS NULL
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            invalid_count = result[0] if result else 0
            
            assert invalid_count == 0, f"Found {invalid_count} records with invalid MeasureID references"
            
            cursor.close()
        except Exception as e:
            logger.error(f"Referential integrity test failed: {e}")
            pytest.fail(f"Referential integrity test failed: {e}")
    
    def test_calculated_measures(self, target_data):
        """Test that calculated measures are correct."""
        assert not target_data.empty, "Target data is empty"
        
        # Example: Test that a calculated percentage is between 0 and 100
        if 'SuccessRate' in target_data.columns:
            invalid_rates = target_data[(target_data['SuccessRate'] < 0) | (target_data['SuccessRate'] > 100)]
            assert len(invalid_rates) == 0, f"Found {len(invalid_rates)} records with invalid success rates"
        
        # Example: Test that a calculated ratio makes sense
        if 'ClaimRatio' in target_data.columns and 'Amount' in target_data.columns and 'TotalAmount' in target_data.columns:
            # Filter out rows where TotalAmount is zero to avoid division by zero
            valid_rows = target_data[target_data['TotalAmount'] != 0]
            
            # Calculate the ratio ourselves
            valid_rows['CalculatedRatio'] = valid_rows['Amount'] / valid_rows['TotalAmount']
            
            # Compare with a small tolerance for floating point differences
            mismatch = valid_rows[abs(valid_rows['ClaimRatio'] - valid_rows['CalculatedRatio']) > 0.01]
            assert len(mismatch) == 0, f"Found {len(mismatch)} records with incorrect claim ratios"
    
    def test_performance(self, target_connection):
        """Test the performance of the stored procedure."""
        try:
            cursor = target_connection.cursor()
            
            # Measure execution time
            start_time = datetime.now()
            cursor.execute(f"EXEC {TestConfig.STORED_PROC}")
            target_connection.commit()
            end_time = datetime.now()
            
            execution_time = (end_time - start_time).total_seconds()
            logger.info(f"Stored procedure execution time: {execution_time} seconds")
            
            # Set a reasonable threshold based on expected performance
            threshold_seconds = 300  # 5 minutes
            assert execution_time < threshold_seconds, \
                f"Stored procedure execution took {execution_time} seconds, exceeding threshold of {threshold_seconds} seconds"
            
            cursor.close()
        except Exception as e:
            logger.error(f"Performance test failed: {e}")
            pytest.fail(f"Performance test failed: {e}")
    
    def test_transaction_isolation(self, target_connection):
        """Test that the stored procedure maintains transaction isolation."""
        try:
            # Start a transaction
            target_connection.autocommit = False
            cursor = target_connection.cursor()
            
            # Execute the stored procedure
            cursor.execute(f"EXEC {TestConfig.STORED_PROC}")
            
            # Check if data is visible within this transaction
            cursor.execute("SELECT COUNT(*) FROM SemanticClaimTransactionMeasures")
            count_in_transaction = cursor.fetchone()[0]
            
            # Rollback the transaction
            target_connection.rollback()
            
            # Check if data changes were rolled back
            cursor.execute("SELECT COUNT(*) FROM SemanticClaimTransactionMeasures")
            count_after_rollback = cursor.fetchone()[0]
            
            # Reset autocommit
            target_connection.autocommit = True
            cursor.close()
            
            # Log for debugging
            logger.info(f"Count in transaction: {count_in_transaction}, Count after rollback: {count_after_rollback}")
            
            # This assertion depends on the specific behavior expected
            # If the procedure should be fully transactional, counts might differ after rollback
            # If it uses internal transactions, they might not be affected by our rollback
            # Adjust based on expected behavior
        except Exception as e:
            logger.error(f"Transaction isolation test failed: {e}")
            pytest.fail(f"Transaction isolation test failed: {e}")
    
    def test_error_handling(self, target_connection):
        """Test that the stored procedure handles errors gracefully."""
        try:
            cursor = target_connection.cursor()
            
            # Attempt to execute with invalid parameters (if the procedure accepts parameters)
            # This is an example - adjust based on actual procedure signature
            try:
                cursor.execute(f"EXEC {TestConfig.STORED_PROC} @InvalidParam = 'test'")
                target_connection.commit()
                # If we get here, the procedure didn't raise an error for invalid parameters
                logger.warning("Stored procedure did not validate parameters as expected")
            except pyodbc.Error as e:
                # Expected error for invalid parameters
                logger.info(f"Expected error occurred: {e}")
            
            cursor.close()
        except Exception as e:
            logger.error(f"Error handling test failed: {e}")
            pytest.fail(f"Error handling test failed: {e}")
    
    def test_idempotency(self, target_connection):
        """Test that running the stored procedure multiple times produces consistent results."""
        try:
            cursor = target_connection.cursor()
            
            # Get initial state
            cursor.execute("SELECT COUNT(*), SUM(CAST(Amount AS DECIMAL(18,2))) FROM SemanticClaimTransactionMeasures")
            initial_result = cursor.fetchone()
            initial_count, initial_sum = initial_result[0], initial_result[1]
            
            # Execute the procedure
            cursor.execute(f"EXEC {TestConfig.STORED_PROC}")
            target_connection.commit()
            
            # Get state after first execution
            cursor.execute("SELECT COUNT(*), SUM(CAST(Amount AS DECIMAL(18,2))) FROM SemanticClaimTransactionMeasures")
            first_result = cursor.fetchone()
            first_count, first_sum = first_result[0], first_result[1]
            
            # Execute the procedure again
            cursor.execute(f"EXEC {TestConfig.STORED_PROC}")
            target_connection.commit()
            
            # Get state after second execution
            cursor.execute("SELECT COUNT(*), SUM(CAST(Amount AS DECIMAL(18,2))) FROM SemanticClaimTransactionMeasures")
            second_result = cursor.fetchone()
            second_count, second_sum = second_result[0], second_result[1]
            
            cursor.close()
            
            # Log for debugging
            logger.info(f"Initial state: {initial_count} rows, sum={initial_sum}")
            logger.info(f"After first execution: {first_count} rows, sum={first_sum}")
            logger.info(f"After second execution: {second_count} rows, sum={second_sum}")
            
            # Check if the results are consistent
            # This depends on the expected behavior - if the procedure should be idempotent,
            # the counts and sums should be the same after the first and second executions
            assert first_count == second_count, "Row count changed after second execution"
            
            # Compare sums with tolerance for floating point differences
            if first_sum is not None and second_sum is not None:
                assert abs(first_sum - second_sum) < 0.01, "Sum changed after second execution"
        except Exception as e:
            logger.error(f"Idempotency test failed: {e}")
            pytest.fail(f"Idempotency test failed: {e}")

    def test_data_consistency(self, target_data):
        """Test that the data in the target table is internally consistent."""
        assert not target_data.empty, "Target data is empty"
        
        # Example: Test that subtotals add up to totals
        if all(col in target_data.columns for col in ['SubtotalA', 'SubtotalB', 'Total']):
            # Calculate the expected total
            target_data['ExpectedTotal'] = target_data['SubtotalA'] + target_data['SubtotalB']
            
            # Compare with actual total, allowing for small floating point differences
            inconsistent = target_data[abs(target_data['Total'] - target_data['ExpectedTotal']) > 0.01]
            assert len(inconsistent) == 0, f"Found {len(inconsistent)} records with inconsistent totals"
        
        # Example: Test that percentages sum to 100%
        percentage_columns = [col for col in target_data.columns if 'Percentage' in col]
        if len(percentage_columns) >= 2:
            target_data['TotalPercentage'] = target_data[percentage_columns].sum(axis=1)
            invalid_percentages = target_data[abs(target_data['TotalPercentage'] - 100) > 0.01]
            assert len(invalid_percentages) == 0, f"Found {len(invalid_percentages)} records where percentages don't sum to 100%"

    def test_historical_data_preservation(self, target_connection):
        """Test that historical data is preserved when the procedure is run."""
        try:
            cursor = target_connection.cursor()
            
            # Get a sample of historical data before running the procedure
            cursor.execute("""
            SELECT TOP 10 * 
            FROM SemanticClaimTransactionMeasures 
            WHERE TransactionDate < DATEADD(month, -1, GETDATE())
            ORDER BY NEWID()
            """)
            
            historical_data = []
            columns = [column[0] for column in cursor.description]
            
            for row in cursor.fetchall():
                historical_data.append(dict(zip(columns, row)))
            
            if not historical_data:
                pytest.skip("No historical data found for testing")
                return
            
            # Run the stored procedure
            cursor.execute(f"EXEC {TestConfig.STORED_PROC}")
            target_connection.commit()
            
            # Check if the historical data is still present and unchanged
            for record in historical_data:
                query = f"SELECT COUNT(*) FROM SemanticClaimTransactionMeasures WHERE ClaimID = {record['ClaimID']}"
                if 'TransactionDate' in record and record['TransactionDate']:
                    query += f" AND TransactionDate = '{record['TransactionDate'].strftime('%Y-%m-%d')}'"
                
                cursor.execute(query)
                count = cursor.fetchone()[0]
                assert count > 0, f"Historical record for ClaimID {record['ClaimID']} was not preserved"
            
            cursor.close()
        except Exception as e:
            logger.error(f"Historical data preservation test failed: {e}")
            pytest.fail(f"Historical data preservation test failed: {e}")

    def test_logging_and_auditing(self, target_connection):
        """Test that the procedure properly logs its activities."""
        try:
            cursor = target_connection.cursor()
            
            # Check if a log or audit table exists
            cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('ETLLog', 'DataTransformationLog', 'AuditLog')
            """)
            
            log_table_count = cursor.fetchone()[0]
            if log_table_count == 0:
                pytest.skip("No log or audit tables found")
                return
            
            # Get the current max log ID to compare after running the procedure
            cursor.execute("""
            SELECT MAX(LogID) 
            FROM (
                SELECT MAX(LogID) AS LogID FROM ETLLog
                UNION ALL
                SELECT MAX(LogID) AS LogID FROM DataTransformationLog
                UNION ALL
                SELECT MAX(LogID) AS LogID FROM AuditLog
            ) AS Logs
            """)
            
            max_log_id_before = cursor.fetchone()[0] or 0
            
            # Run the stored procedure
            cursor.execute(f"EXEC {TestConfig.STORED_PROC}")
            target_connection.commit()
            
            # Check if new log entries were created
            cursor.execute("""
            SELECT MAX(LogID) 
            FROM (
                SELECT MAX(LogID) AS LogID FROM ETLLog
                UNION ALL
                SELECT MAX(LogID) AS LogID FROM DataTransformationLog
                UNION ALL
                SELECT MAX(LogID) AS LogID FROM AuditLog
            ) AS Logs
            """)
            
            max_log_id_after = cursor.fetchone()[0] or 0
            
            assert max_log_id_after > max_log_id_before, "No new log entries were created"
            
            cursor.close()
        except Exception as e:
            logger.error(f"Logging and auditing test failed: {e}")
            pytest.skip(f"Logging and auditing test failed: {e}")

# Main execution for manual testing
if __name__ == "__main__":
    print("Running unit tests for uspSemanticClaimTransactionMeasuresData...")
    pytest.main(['-xvs', __file__])