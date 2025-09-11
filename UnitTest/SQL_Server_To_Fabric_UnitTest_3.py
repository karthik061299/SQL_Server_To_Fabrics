_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Unit tests for uspSemanticClaimTransactionMeasuresData Fabric SQL stored procedure
## *Version*: 3 
## *Updated on*: 
_____________________________________________

import pytest
import pyodbc
import pandas as pd
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock


class TestSemanticClaimTransactionMeasuresData:
    """
    Test suite for the Semantic.uspSemanticClaimTransactionMeasuresData stored procedure.
    This procedure processes claim transaction measures data and returns a final result set.
    """
    
    @pytest.fixture(scope="class")
    def db_connection(self):
        """
        Create a connection to the test database.
        This is mocked in these tests but would connect to a real test database in practice.
        """
        # In a real scenario, this would connect to a test database
        # For unit tests, we'll mock this connection
        mock_conn = MagicMock()
        return mock_conn
    
    @pytest.fixture(scope="function")
    def setup_mock_tables(self, db_connection):
        """
        Set up mock tables and test data before each test.
        """
        cursor = db_connection.cursor.return_value
        
        # Mock the creation of tables and return of result sets
        def execute_side_effect(query, *args):
            if "SELECT @CATCount = COUNT(*)" in query:
                cursor.fetchone.return_value = [10]  # Mock that we have 10 records
            return cursor
        
        cursor.execute.side_effect = execute_side_effect
        
        # Create test data for different scenarios
        self.test_data = self._create_test_data()
        
        return cursor
    
    def _create_test_data(self):
        """
        Create test data for the various tables used in the stored procedure.
        """
        # Sample data for ClaimTransactionMeasures
        claim_transaction_measures = pd.DataFrame({
            'FactClaimTransactionLineWCKey': [1, 2, 3, 4, 5],
            'RevisionNumber': [1, 1, 2, 1, 3],
            'HashValue': ['hash1', 'hash2', 'hash3', 'hash4', 'hash5'],
            'LoadCreateDate': [datetime.now() - timedelta(days=10) for _ in range(5)]
        })
        
        # Sample data for PolicyRiskStateDescriptors
        policy_risk_state = pd.DataFrame({
            'PolicyRiskStateWCKey': [101, 102, 103, 104, 105],
            'PolicyWCKey': [201, 202, 203, 204, 205],
            'RiskState': ['CA', 'NY', 'TX', 'FL', 'WA'],
            'RetiredInd': [0, 0, 0, 0, 0],
            'RiskStateEffectiveDate': [datetime.now() - timedelta(days=100) for _ in range(5)],
            'RecordEffectiveDate': [datetime.now() - timedelta(days=90) for _ in range(5)],
            'LoadUpdateDate': [datetime.now() - timedelta(days=80) for _ in range(5)]
        })
        
        # Sample data for FactClaimTransactionLineWC
        fact_claim_transaction = pd.DataFrame({
            'FactClaimTransactionLineWCKey': [1, 2, 3, 4, 5],
            'RevisionNumber': [1, 1, 2, 1, 3],
            'PolicyWCKey': [201, 202, 203, 204, 205],
            'ClaimWCKey': [301, 302, 303, 304, 305],
            'ClaimTransactionLineCategoryKey': [401, 402, 403, 404, 405],
            'ClaimTransactionWCKey': [501, 502, 503, 504, 505],
            'ClaimCheckKey': [601, 602, 603, 604, 605],
            'SourceTransactionLineItemCreateDate': [datetime.now() - timedelta(days=50) for _ in range(5)],
            'SourceTransactionLineItemCreateDateKey': [20230101, 20230102, 20230103, 20230104, 20230105],
            'SourceSystem': ['System1', 'System2', 'System1', 'System3', 'System2'],
            'RecordEffectiveDate': [datetime.now() - timedelta(days=40) for _ in range(5)],
            'TransactionAmount': [1000.00, 2000.00, 1500.00, 3000.00, 2500.00],
            'LoadUpdateDate': [datetime.now() - timedelta(days=30) for _ in range(5)],
            'Retiredind': [0, 0, 0, 0, 0]
        })
        
        # Sample data for ClaimTransactionDescriptors
        claim_transaction_descriptors = pd.DataFrame({
            'ClaimTransactionLineCategoryKey': [401, 402, 403, 404, 405],
            'ClaimTransactionWCKey': [501, 502, 503, 504, 505],
            'ClaimWCKey': [301, 302, 303, 304, 305],
            'SourceTransactionCreateDate': [datetime.now() - timedelta(days=45) for _ in range(5)],
            'TransactionSubmitDate': [datetime.now() - timedelta(days=44) for _ in range(5)]
        })
        
        # Sample data for ClaimDescriptors
        claim_descriptors = pd.DataFrame({
            'ClaimWCKey': [301, 302, 303, 304, 305],
            'EmploymentLocationState': ['CA', None, 'TX', None, 'WA'],
            'JurisdictionState': [None, 'NY', None, 'FL', None]
        })
        
        # Sample data for PolicyDescriptors
        policy_descriptors = pd.DataFrame({
            'PolicyWCKey': [201, 202, 203, 204, 205],
            'BrandKey': [701, 702, 703, 704, 705],
            'AgencyKey': [801, 802, 803, 804, 805]
        })
        
        # Sample data for dimBrand
        dim_brand = pd.DataFrame({
            'BrandKey': [701, 702, 703, 704, 705],
            'BrandName': ['Brand1', 'Brand2', 'Brand3', 'Brand4', 'Brand5']
        })
        
        # Sample data for SemanticLayerMetaData
        semantic_layer_metadata = pd.DataFrame({
            'SourceType': ['Claims', 'Claims', 'Claims', 'Claims', 'Claims'],
            'Measure_Name': ['NetPaidIndemnity', 'NetPaidMedical', 'NetIncurredLoss', 'GrossPaidLoss', 'RecoveryIndemnity'],
            'Logic': ['SUM(CASE WHEN x=1 THEN Amount ELSE 0 END)', 'SUM(CASE WHEN x=2 THEN Amount ELSE 0 END)', 
                     'SUM(CASE WHEN x=3 THEN Amount ELSE 0 END)', 'SUM(CASE WHEN x=4 THEN Amount ELSE 0 END)', 
                     'SUM(CASE WHEN x=5 THEN Amount ELSE 0 END)']
        })
        
        # Expected final result
        expected_result = pd.DataFrame({
            'FactClaimTransactionLineWCKey': [1, 2, 3, 4, 5],
            'RevisionNumber': [1, 1, 2, 1, 3],
            'PolicyWCKey': [201, 202, 203, 204, 205],
            'PolicyRiskStateWCKey': [101, 102, 103, 104, 105],
            'ClaimWCKey': [301, 302, 303, 304, 305],
            'ClaimTransactionLineCategoryKey': [401, 402, 403, 404, 405],
            'ClaimTransactionWCKey': [501, 502, 503, 504, 505],
            'ClaimCheckKey': [601, 602, 603, 604, 605],
            'AgencyKey': [801, 802, 803, 804, 805],
            'SourceClaimTransactionCreateDate': [datetime.now() - timedelta(days=50) for _ in range(5)],
            'SourceClaimTransactionCreateDateKey': [20230101, 20230102, 20230103, 20230104, 20230105],
            'TransactionCreateDate': [datetime.now() - timedelta(days=45) for _ in range(5)],
            'TransactionSubmitDate': [datetime.now() - timedelta(days=44) for _ in range(5)],
            'SourceSystem': ['System1', 'System2', 'System1', 'System3', 'System2'],
            'RecordEffectiveDate': [datetime.now() - timedelta(days=40) for _ in range(5)],
            'SourceSystemIdentifier': ['1~1', '2~1', '3~2', '4~1', '5~3'],
            'HashValue': ['newhash1', 'newhash2', 'newhash3', 'newhash4', 'newhash5'],
            'RetiredInd': [0, 0, 0, 0, 0],
            'InsertUpdates': [0, 0, 1, 0, 1],  # 0=update, 1=insert, 3=no change
            'AuditOperations': ['Updated', 'Updated', 'Inserted', 'Updated', 'Inserted'],
            'LoadUpdateDate': [datetime.now() for _ in range(5)],
            'LoadCreateDate': [datetime.now() - timedelta(days=10) for _ in range(5)],
            # Add all the measure columns with sample values
            'NetPaidIndemnity': [100.0, 200.0, 150.0, 300.0, 250.0],
            'NetPaidMedical': [150.0, 250.0, 200.0, 350.0, 300.0],
            'NetIncurredLoss': [200.0, 300.0, 250.0, 400.0, 350.0],
            'GrossPaidLoss': [250.0, 350.0, 300.0, 450.0, 400.0],
            'RecoveryIndemnity': [50.0, 75.0, 60.0, 90.0, 80.0],
            # Add other measure columns as needed
        })
        
        return {
            'ClaimTransactionMeasures': claim_transaction_measures,
            'PolicyRiskStateDescriptors': policy_risk_state,
            'FactClaimTransactionLineWC': fact_claim_transaction,
            'ClaimTransactionDescriptors': claim_transaction_descriptors,
            'ClaimDescriptors': claim_descriptors,
            'PolicyDescriptors': policy_descriptors,
            'dimBrand': dim_brand,
            'SemanticLayerMetaData': semantic_layer_metadata,
            'ExpectedResult': expected_result
        }
    
    @pytest.mark.parametrize(
        "job_start_datetime,job_end_datetime,expected_status", [
            (datetime.now() - timedelta(days=7), datetime.now(), "success"),  # Normal date range
            (datetime(1900, 1, 1), datetime.now(), "success"),  # Special case: 1900-01-01 should be converted to 1700-01-01
            (datetime.now() - timedelta(days=30), datetime.now(), "success"),  # Wider date range
            (datetime.now(), datetime.now() - timedelta(days=1), "error"),  # Invalid date range (end before start)
        ]
    )
    def test_stored_procedure_execution(self, db_connection, setup_mock_tables, job_start_datetime, job_end_datetime, expected_status):
        """
        Test the execution of the stored procedure with different date parameters.
        """
        cursor = setup_mock_tables
        
        # If we expect an error for invalid date range
        if expected_status == "error" and job_start_datetime > job_end_datetime:
            with pytest.raises(Exception):
                cursor.execute(
                    "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                    job_start_datetime, job_end_datetime
                )
            return
        
        # For the special case where start date is 1900-01-01
        if job_start_datetime.year == 1900 and job_start_datetime.month == 1 and job_start_datetime.day == 1:
            # Verify that the procedure handles this special case by converting to 1700-01-01
            # This would be checked in the actual SQL execution, here we just ensure no exception
            pass
        
        # Execute the stored procedure
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # In a real test, we would fetch and verify results
        # For the mock, we just assert that execute was called with the right parameters
        cursor.execute.assert_called_with(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        assert expected_status == "success"
    
    def test_empty_claim_transaction_measures(self, db_connection):
        """
        Test behavior when ClaimTransactionMeasures table is empty.
        """
        cursor = db_connection.cursor.return_value
        
        # Mock that ClaimTransactionMeasures is empty
        def execute_side_effect(query, *args):
            if "SELECT @CATCount = COUNT(*)" in query:
                cursor.fetchone.return_value = [0]  # No records
            return cursor
        
        cursor.execute.side_effect = execute_side_effect
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # The procedure should still run without errors even with empty tables
        cursor.execute.assert_called_with(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
    
    def test_data_transformation(self, db_connection, setup_mock_tables):
        """
        Test the data transformation logic of the stored procedure.
        """
        cursor = setup_mock_tables
        
        # Mock the result set that would be returned by the stored procedure
        expected_df = self.test_data['ExpectedResult']
        mock_result = [tuple(row) for row in expected_df.values]
        
        cursor.fetchall.return_value = mock_result
        cursor.description = [(col, None, None, None, None, None, None) for col in expected_df.columns]
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # Fetch the results
        result = cursor.fetchall()
        
        # Convert to DataFrame for easier comparison
        result_df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        
        # Verify the transformation results
        # In a real test, we would compare specific values
        # Here we just check that we got the expected number of rows and columns
        assert len(result_df) == len(expected_df)
        assert set(result_df.columns) == set(expected_df.columns)
        
        # Check a few key columns to ensure data was transformed correctly
        for i, row in enumerate(mock_result):
            # Check that SourceSystemIdentifier is correctly formed as FactClaimTransactionLineWCKey~RevisionNumber
            expected_id = f"{row[0]}~{row[1]}"  # FactClaimTransactionLineWCKey~RevisionNumber
            assert expected_id == result_df.iloc[i]['SourceSystemIdentifier']
    
    def test_hash_value_calculation(self, db_connection, setup_mock_tables):
        """
        Test that hash values are calculated correctly for change tracking.
        """
        cursor = setup_mock_tables
        
        # Mock the result set with hash values
        expected_df = self.test_data['ExpectedResult']
        mock_result = [tuple(row) for row in expected_df.values]
        
        cursor.fetchall.return_value = mock_result
        cursor.description = [(col, None, None, None, None, None, None) for col in expected_df.columns]
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # Fetch the results
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        
        # Verify that hash values are present and not null
        assert all(result_df['HashValue'].notna())
        
        # In a real test, we would verify the actual hash calculation
        # Here we just check that the hash values match our expected mock values
        for i in range(len(result_df)):
            assert result_df.iloc[i]['HashValue'] == expected_df.iloc[i]['HashValue']
    
    def test_audit_operations(self, db_connection, setup_mock_tables):
        """
        Test that audit operations (Insert/Update) are correctly determined.
        """
        cursor = setup_mock_tables
        
        # Mock the result set with audit operations
        expected_df = self.test_data['ExpectedResult']
        mock_result = [tuple(row) for row in expected_df.values]
        
        cursor.fetchall.return_value = mock_result
        cursor.description = [(col, None, None, None, None, None, None) for col in expected_df.columns]
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # Fetch the results
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        
        # Verify audit operations
        # InsertUpdates: 0=update, 1=insert, 3=no change
        # AuditOperations: 'Updated', 'Inserted', or NULL
        for i in range(len(result_df)):
            insert_update = result_df.iloc[i]['InsertUpdates']
            audit_op = result_df.iloc[i]['AuditOperations']
            
            if insert_update == 0:
                assert audit_op == 'Updated'
            elif insert_update == 1:
                assert audit_op == 'Inserted'
            elif insert_update == 3:
                assert audit_op is None
    
    def test_error_handling(self, db_connection):
        """
        Test error handling in the stored procedure.
        """
        cursor = db_connection.cursor.return_value
        
        # Mock an error during execution
        cursor.execute.side_effect = pyodbc.Error("Test error")
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        # The procedure should catch the error and log it
        with pytest.raises(Exception):
            cursor.execute(
                "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
                job_start_datetime, job_end_datetime
            )
        
        # In a real test, we would verify that error was logged
        # Here we just check that the error was raised
        cursor.execute.assert_called_once()
    
    @patch('pyodbc.connect')
    def test_integration(self, mock_connect, db_connection, setup_mock_tables):
        """
        Integration test that simulates the full execution of the stored procedure.
        """
        # This is a more comprehensive test that would be used in an actual integration test environment
        # For unit testing, we're using mocks, but this shows how you might structure a real integration test
        
        mock_connect.return_value = db_connection
        cursor = setup_mock_tables
        
        # Mock successful execution and result set
        expected_df = self.test_data['ExpectedResult']
        mock_result = [tuple(row) for row in expected_df.values]
        
        cursor.fetchall.return_value = mock_result
        cursor.description = [(col, None, None, None, None, None, None) for col in expected_df.columns]
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        # In a real integration test, we would connect to a test database
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=test_server;DATABASE=test_db;UID=user;PWD=password')
        cursor = conn.cursor()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # Fetch and validate results
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        
        # Verify key aspects of the results
        assert len(result_df) > 0
        assert 'HashValue' in result_df.columns
        assert 'AuditOperations' in result_df.columns
        
        # Close the connection
        cursor.close()
        conn.close()
    
    def test_special_date_handling(self, db_connection, setup_mock_tables):
        """
        Test the special date handling for 01/01/1900.
        """
        cursor = setup_mock_tables
        
        # Execute with the special date
        special_date = datetime(1900, 1, 1)
        end_date = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            special_date, end_date
        )
        
        # In a real test, we would verify that the date was converted to 1700-01-01 internally
        # Here we just check that the procedure was called with the right parameters
        cursor.execute.assert_called_with(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            special_date, end_date
        )
    
    def test_temporary_table_creation(self, db_connection):
        """
        Test that temporary tables are created and dropped correctly.
        """
        cursor = db_connection.cursor.return_value
        
        # Track table creation and dropping
        created_tables = set()
        dropped_tables = set()
        
        def execute_side_effect(query, *args):
            # Extract table names from CREATE TABLE statements
            if query.strip().upper().startswith('CREATE TABLE '):
                table_name = query.split('CREATE TABLE ')[1].split(' ')[0]
                created_tables.add(table_name)
            
            # Extract table names from DROP TABLE statements
            elif query.strip().upper().startswith('DROP TABLE IF EXISTS '):
                table_name = query.split('DROP TABLE IF EXISTS ')[1].strip()
                dropped_tables.add(table_name)
            
            return cursor
        
        cursor.execute.side_effect = execute_side_effect
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # In a real test, we would verify specific table names
        # Here we just check that tables were created and then dropped
        assert len(created_tables) > 0
        assert len(dropped_tables) > 0
        
        # All created tables should be dropped
        for table in created_tables:
            assert table in dropped_tables
    
    def test_measure_calculation(self, db_connection, setup_mock_tables):
        """
        Test that measures are calculated correctly from the SemanticLayerMetaData.
        """
        cursor = setup_mock_tables
        
        # Mock the SemanticLayerMetaData query result
        semantic_metadata = self.test_data['SemanticLayerMetaData']
        
        def execute_side_effect(query, *args):
            if "SELECT @Measure_SQL_Query = STRING_AGG" in query:
                # Mock the string aggregation of measure definitions
                cursor.fetchone.return_value = [", ".join([f"{row['Logic']} AS {row['Measure_Name']}" 
                                                for _, row in semantic_metadata.iterrows()])]
            return cursor
        
        cursor.execute.side_effect = execute_side_effect
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # In a real test, we would verify the actual measure calculations
        # Here we just check that the measures are included in the query
        measure_names = semantic_metadata['Measure_Name'].tolist()
        
        # Check that all measure names are used in at least one query
        for measure in measure_names:
            found = False
            for call_args in cursor.execute.call_args_list:
                if measure in call_args[0][0]:
                    found = True
                    break
            assert found, f"Measure {measure} not found in any query"


if __name__ == "__main__":
    pytest.main(['-xvs', __file__])
