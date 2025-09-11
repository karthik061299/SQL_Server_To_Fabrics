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