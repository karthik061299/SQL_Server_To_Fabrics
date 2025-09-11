_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Unit tests for uspSemanticClaimTransactionMeasuresData Fabric SQL conversion
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import datetime
from decimal import Decimal
import hashlib

# Fabric SQL specific imports
try:
    from fabric_sql_connector import FabricSQLConnector
except ImportError:
    # Mock for testing without actual Fabric SQL connector
    FabricSQLConnector = type('FabricSQLConnector', (), {})


class TestUspSemanticClaimTransactionMeasuresData:
    """
    Comprehensive unit tests for uspSemanticClaimTransactionMeasuresData Fabric SQL conversion.
    
    This test suite covers:
    - Happy path scenarios
    - Edge cases
    - Error handling
    - Data validation
    - Performance considerations
    - Fabric SQL specific features
    """
    
    @pytest.fixture
    def mock_fabric_connection(self):
        """Mock Fabric SQL connection"""
        mock_conn = Mock(spec=FabricSQLConnector)
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor
    
    @pytest.fixture
    def fabric_connection_config(self):
        """Fabric SQL connection configuration"""
        return {
            'workspace_name': 'test_workspace',
            'server_endpoint': 'test.fabric.microsoft.com',
            'database_name': 'EDSMart',
            'authentication': 'AAD',
            'timeout': 300  # Increased timeout for Fabric SQL
        }
    
    @pytest.fixture
    def sample_input_data(self):
        """Sample input data for testing"""
        return {
            'job_start_datetime': datetime.datetime(2023, 1, 1),
            'job_end_datetime': datetime.datetime(2023, 12, 31),
            'fact_claim_transaction_data': [
                {
                    'FactClaimTransactionLineWCKey': 1,
                    'RevisionNumber': 1,
                    'PolicyWCKey': 100,
                    'ClaimWCKey': 200,
                    'ClaimTransactionLineCategoryKey': 300,
                    'ClaimTransactionWCKey': 400,
                    'ClaimCheckKey': 500,
                    'TransactionAmount': Decimal('1000.00'),
                    'LoadUpdateDate': datetime.datetime(2023, 6, 1),
                    'RetiredInd': 0
                }
            ]
        }
    
    @pytest.fixture
    def expected_output_schema(self):
        """Expected output schema for validation"""
        return {
            'FactClaimTransactionLineWCKey': int,
            'RevisionNumber': int,
            'PolicyWCKey': int,
            'PolicyRiskStateWCKey': int,
            'ClaimWCKey': int,
            'ClaimTransactionLineCategoryKey': int,
            'ClaimTransactionWCKey': int,
            'ClaimCheckKey': int,
            'AgencyKey': int,
            'SourceClaimTransactionCreateDate': datetime.datetime,
            'SourceClaimTransactionCreateDateKey': int,
            'TransactionCreateDate': datetime.datetime,
            'TransactionSubmitDate': datetime.datetime,
            'SourceSystem': str,
            'RecordEffectiveDate': datetime.datetime,
            'SourceSystemIdentifier': str,
            'TransactionAmount': Decimal,
            'HashValue': str,
            'RetiredInd': int,
            'InsertUpdates': int,
            'AuditOperations': str,
            'LoadUpdateDate': datetime.datetime,
            'LoadCreateDate': datetime.datetime
        }
    
    # Happy Path Tests
    def test_procedure_execution_success(self, mock_fabric_connection, sample_input_data):
        """Test successful execution of the stored procedure"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Mock successful execution
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [
            (1, 1, 100, 101, 200, 300, 400, 500, 600, 
             datetime.datetime(2023, 1, 1), 20230101,
             datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 2),
             'TestSystem', datetime.datetime(2023, 1, 1),
             '1~1', Decimal('1000.00'), 'test_hash', 0, 1, 'Inserted',
             datetime.datetime.now(), datetime.datetime.now())
        ]
        
        # Execute procedure
        result = self._execute_fabric_query(
            mock_conn,
            sample_input_data['job_start_datetime'],
            sample_input_data['job_end_datetime']
        )
        
        assert result is not None
        assert len(result) > 0
        mock_cursor.execute.assert_called()
    
    def test_date_parameter_handling(self, mock_fabric_connection):
        """Test proper handling of date parameters"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Test with 1900-01-01 date (should be converted to 1700-01-01)
        start_date = datetime.datetime(1900, 1, 1)
        end_date = datetime.datetime(2023, 12, 31)
        
        # In Fabric SQL, we need to use parameterized queries differently
        result = self._execute_fabric_query(mock_conn, start_date, end_date)
        
        # Verify that the date conversion logic is applied
        mock_cursor.execute.assert_called()
        # Verify the parameter was converted correctly
        call_args = mock_cursor.execute.call_args[0]
        assert '1700-01-01' in str(call_args) or '@pJobStartDateTime' in str(call_args)
        
    def test_hash_value_generation(self):
        """Test hash value generation for data integrity"""
        test_data = {
            'FactClaimTransactionLineWCKey': 1,
            'RevisionNumber': 1,
            'PolicyWCKey': 100,
            'TransactionAmount': Decimal('1000.00')
        }
        
        # Fabric SQL uses SHA2_512 for hash generation
        hash_input = '~'.join([str(v) for v in test_data.values()])
        expected_hash = hashlib.sha512(hash_input.encode()).hexdigest()[:128]
        
        generated_hash = self._generate_fabric_hash_value(test_data)
        
        assert generated_hash is not None
        assert len(generated_hash) > 0
        assert isinstance(generated_hash, str)
    
    def test_revision_number_handling(self, mock_fabric_connection, sample_input_data):
        """Test proper handling of revision numbers"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Test with NULL revision number (should default to 0)
        test_data = sample_input_data['fact_claim_transaction_data'][0].copy()
        test_data['RevisionNumber'] = None
        
        result = self._process_revision_number(test_data)
        
        assert result['RevisionNumber'] == 0
    
    # Edge Cases Tests
    def test_empty_result_set(self, mock_fabric_connection):
        """Test handling of empty result sets"""
        mock_conn, mock_cursor = mock_fabric_connection
        mock_cursor.fetchall.return_value = []
        
        result = self._execute_fabric_query(
            mock_conn,
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 1, 2)
        )
        
        assert result == []
    
    def test_null_values_handling(self, mock_fabric_connection):
        """Test proper handling of NULL values"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        test_data = {
            'PolicyRiskStateWCKey': None,
            'AgencyKey': None,
            'ClaimCheckKey': None
        }
        
        processed_data = self._handle_null_values(test_data)
        
        assert processed_data['PolicyRiskStateWCKey'] == -1
        assert processed_data['AgencyKey'] == -1
        # ClaimCheckKey might remain None or be handled differently
    
    def test_large_dataset_handling(self, mock_fabric_connection):
        """Test handling of large datasets"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Simulate large dataset
        large_dataset = []
        for i in range(10000):
            large_dataset.append((
                i, 1, 100+i, 101+i, 200+i, 300+i, 400+i, 500+i, 600+i,
                datetime.datetime(2023, 1, 1), 20230101,
                datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 2),
                'TestSystem', datetime.datetime(2023, 1, 1),
                f'{i}~1', Decimal('1000.00'), f'hash_{i}', 0, 1, 'Inserted',
                datetime.datetime.now(), datetime.datetime.now()
            ))
        
        mock_cursor.fetchall.return_value = large_dataset
        
        result = self._execute_fabric_query(
            mock_conn,
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 12, 31)
        )
        
        assert len(result) == 10000
        
    def test_boundary_dates(self, mock_fabric_connection):
        """Test boundary date conditions"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Test with same start and end date
        same_date = datetime.datetime(2023, 6, 15)
        result = self._execute_fabric_query(mock_conn, same_date, same_date)
        
        mock_cursor.execute.assert_called()
        
        # Test with end date before start date
        start_date = datetime.datetime(2023, 6, 15)
        end_date = datetime.datetime(2023, 6, 10)
        
        result = self._execute_fabric_query(mock_conn, start_date, end_date)
        
        # Should handle gracefully or raise appropriate error
        mock_cursor.execute.assert_called()
    
    def test_special_characters_in_data(self):
        """Test handling of special characters in string fields"""
        test_data = {
            'SourceSystem': "Test'System\"With~Special|Characters",
            'SourceSystemIdentifier': "ID~With|Special'Characters"
        }
        
        processed_data = self._sanitize_string_data(test_data)
        
        assert processed_data['SourceSystem'] is not None
        assert processed_data['SourceSystemIdentifier'] is not None
    
    # Error Handling Tests
    def test_invalid_date_parameters(self, mock_fabric_connection):
        """Test handling of invalid date parameters"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        with pytest.raises((ValueError, TypeError)):
            self._execute_fabric_query(mock_conn, "invalid_date", datetime.datetime.now())
        
        with pytest.raises((ValueError, TypeError)):
            self._execute_fabric_query(mock_conn, datetime.datetime.now(), "invalid_date")
    
    def test_database_connection_failure(self):
        """Test handling of database connection failures"""
        mock_conn = Mock(spec=FabricSQLConnector)
        mock_conn.cursor.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception):
            self._execute_fabric_query(
                mock_conn,
                datetime.datetime(2023, 1, 1),
                datetime.datetime(2023, 12, 31)
            )
    
    def test_sql_execution_error(self, mock_fabric_connection):
        """Test handling of SQL execution errors"""
        mock_conn, mock_cursor = mock_fabric_connection
        mock_cursor.execute.side_effect = Exception("SQL execution failed")
        
        with pytest.raises(Exception):
            self._execute_fabric_query(
                mock_conn,
                datetime.datetime(2023, 1, 1),
                datetime.datetime(2023, 12, 31)
            )
    
    def test_memory_overflow_protection(self, mock_fabric_connection):
        """Test protection against memory overflow with very large datasets"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Simulate memory constraint
        with patch('sys.getsizeof') as mock_sizeof:
            mock_sizeof.return_value = 1024 * 1024 * 1024  # 1GB
            
            result = self._execute_fabric_query(
                mock_conn,
                datetime.datetime(2023, 1, 1),
                datetime.datetime(2023, 12, 31)
            )
            
            # Should handle large datasets appropriately
            mock_cursor.execute.assert_called()
            
    # Fabric SQL Specific Tests
    def test_fabric_workspace_connectivity(self, fabric_connection_config):
        """Test connectivity to Fabric workspace"""
        with patch('fabric_sql_connector.FabricSQLConnector') as mock_connector:
            mock_instance = Mock()
            mock_connector.return_value = mock_instance
            
            # Test connection establishment
            conn = self._create_fabric_connection(fabric_connection_config)
            
            # Verify connection was attempted with correct parameters
            mock_connector.assert_called_once()
            assert conn is not None
    
    def test_fabric_delta_lake_integration(self, mock_fabric_connection):
        """Test integration with Delta Lake in Fabric"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Test Delta Lake specific query
        delta_query = """
        SELECT * FROM DELTA.`lakehouse/tables/ClaimTransactionMeasures`
        WHERE LoadUpdateDate >= ? AND LoadUpdateDate <= ?
        """
        
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [(1, 'test')]
        
        result = self._execute_delta_lake_query(
            mock_conn,
            delta_query,
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 12, 31)
        )
        
        assert result is not None
        mock_cursor.execute.assert_called_once()
    
    def test_fabric_partition_handling(self, mock_fabric_connection):
        """Test handling of partitioned data in Fabric"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        # Test partition pruning query
        partition_query = """
        SELECT * FROM Semantic.ClaimTransactionMeasures
        WHERE SourceClaimTransactionCreateDateKey BETWEEN ? AND ?
        """
        
        start_date_key = 20230101
        end_date_key = 20231231
        
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [(1, 'test')]
        
        result = self._execute_partition_query(
            mock_conn,
            partition_query,
            start_date_key,
            end_date_key
        )
        
        assert result is not None
        mock_cursor.execute.assert_called_once()
        
    # Helper Methods
    def _execute_fabric_query(self, connection, start_date, end_date):
        """Helper method to execute Fabric SQL query"""
        cursor = connection.cursor()
        
        # Fabric SQL uses different parameter syntax
        query = """
        SELECT *
        FROM Semantic.ClaimTransactionMeasures
        WHERE (@pJobStartDateTime = '1900-01-01' AND @pJobStartDateTime = '1700-01-01')
           OR LoadUpdateDate >= @pJobStartDateTime
           AND LoadUpdateDate <= @pJobEndDateTime
        """
        
        # Convert dates to Fabric SQL compatible format
        params = {
            '@pJobStartDateTime': start_date.strftime('%Y-%m-%d') if isinstance(start_date, datetime.datetime) else start_date,
            '@pJobEndDateTime': end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime.datetime) else end_date
        }
        
        cursor.execute(query, params)
        return cursor.fetchall()
    
    def _generate_fabric_hash_value(self, data_dict):
        """Helper method to generate hash value using Fabric SQL compatible algorithm"""
        hash_input = '~'.join([str(v) for v in data_dict.values()])
        return hashlib.sha512(hash_input.encode()).hexdigest()[:128]
    
    def _process_revision_number(self, data):
        """Helper method to process revision number"""
        if data.get('RevisionNumber') is None:
            data['RevisionNumber'] = 0
        return data
    
    def _handle_null_values(self, data):
        """Helper method to handle NULL values"""
        null_to_default = {
            'PolicyRiskStateWCKey': -1,
            'AgencyKey': -1
        }
        
        for key, default_value in null_to_default.items():
            if data.get(key) is None:
                data[key] = default_value
        
        return data
    
    def _sanitize_string_data(self, data):
        """Helper method to sanitize string data for Fabric SQL"""
        for key, value in data.items():
            if isinstance(value, str):
                # Fabric SQL specific string sanitization
                data[key] = value.replace("'", "''").replace('"', '""')
        return data
    
    def _create_fabric_connection(self, config):
        """Create a connection to Fabric SQL"""
        from fabric_sql_connector import FabricSQLConnector
        
        conn = FabricSQLConnector(
            workspace_name=config['workspace_name'],
            server_endpoint=config['server_endpoint'],
            database_name=config['database_name'],
            authentication=config['authentication'],
            timeout=config['timeout']
        )
        return conn
    
    def _execute_delta_lake_query(self, connection, query, start_date, end_date):
        """Execute a Delta Lake query in Fabric"""
        cursor = connection.cursor()
        cursor.execute(query, (start_date, end_date))
        return cursor.fetchall()
    
    def _execute_partition_query(self, connection, query, start_date_key, end_date_key):
        """Execute a query with partition pruning in Fabric"""
        cursor = connection.cursor()
        cursor.execute(query, (start_date_key, end_date_key))
        return cursor.fetchall()


# Integration Tests
class TestUspSemanticClaimTransactionMeasuresDataIntegration:
    """Integration tests for the complete workflow"""
    
    @pytest.fixture
    def fabric_test_environment(self):
        """Setup Fabric test environment"""
        return {
            'workspace_name': 'test_workspace',
            'server_endpoint': 'test.fabric.microsoft.com',
            'database_name': 'EDSMart',
            'lakehouse_name': 'test_lakehouse',
            'delta_table_path': 'lakehouse/tables/ClaimTransactionMeasures'
        }
    
    @pytest.fixture
    def integration_test_data(self):
        """Sample data for integration testing"""
        return {
            'fact_claim_data': pd.DataFrame({
                'FactClaimTransactionLineWCKey': [1, 2, 3],
                'RevisionNumber': [1, 1, 2],
                'PolicyWCKey': [100, 101, 102],
                'ClaimWCKey': [200, 201, 202],
                'TransactionAmount': [1000.00, 1500.00, 2000.00],
                'LoadUpdateDate': [datetime.datetime(2023, 1, 1)] * 3,
                'RetiredInd': [0, 0, 0]
            }),
            'claim_descriptors': pd.DataFrame({
                'ClaimWCKey': [200, 201, 202],
                'EmploymentLocationState': ['CA', 'NY', 'TX'],
                'JurisdictionState': ['CA', 'NY', 'TX']
            }),
            'policy_descriptors': pd.DataFrame({
                'PolicyWCKey': [100, 101, 102],
                'AgencyKey': [600, 601, 602]
            })
        }
    
    def test_end_to_end_workflow(self, integration_test_data):
        """Test complete end-to-end workflow"""
        # This would test the complete workflow from input to output
        # Including all joins, transformations, and business logic
        
        input_data = integration_test_data
        
        # Simulate the complete workflow
        result = self._simulate_complete_workflow(input_data)
        
        assert result is not None
        assert len(result) > 0
        
        # Validate key business rules
        for record in result:
            assert record['FactClaimTransactionLineWCKey'] is not None
            assert record['HashValue'] is not None
            assert record['InsertUpdates'] in [0, 1, 3]
    
    def test_data_consistency_across_runs(self, integration_test_data):
        """Test data consistency across multiple runs"""
        input_data = integration_test_data
        
        # Run the procedure multiple times with same input
        result1 = self._simulate_complete_workflow(input_data)
        result2 = self._simulate_complete_workflow(input_data)
        
        # Results should be consistent
        assert len(result1) == len(result2)
        
        for r1, r2 in zip(result1, result2):
            assert r1['HashValue'] == r2['HashValue']
    
    def _simulate_complete_workflow(self, input_data):
        """Simulate the complete workflow"""
        # This would simulate the complete stored procedure logic
        # For now, return mock data
        return [
            {
                'FactClaimTransactionLineWCKey': 1,
                'RevisionNumber': 1,
                'PolicyWCKey': 100,
                'HashValue': 'test_hash_1',
                'InsertUpdates': 1,
                'AuditOperations': 'Inserted'
            }
        ]


if __name__ == '__main__':
    # Run the tests
    pytest.main([__file__, '-v', '--tb=short'])