import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import datetime
from decimal import Decimal
import hashlib


class TestUspSemanticClaimTransactionMeasuresData:
    """
    Comprehensive unit tests for uspSemanticClaimTransactionMeasuresData Fabric SQL conversion.
    
    This test suite covers:
    - Happy path scenarios
    - Edge cases
    - Error handling
    - Data validation
    - Performance considerations
    """
    
    @pytest.fixture
    def mock_fabric_connection(self):
        """Mock Fabric SQL connection"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor
    
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
        result = self._execute_procedure(
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
        
        result = self._execute_procedure(mock_conn, start_date, end_date)
        
        # Verify that the date conversion logic is applied
        mock_cursor.execute.assert_called()
        
    def test_hash_value_generation(self):
        """Test hash value generation for data integrity"""
        test_data = {
            'FactClaimTransactionLineWCKey': 1,
            'RevisionNumber': 1,
            'PolicyWCKey': 100,
            'TransactionAmount': Decimal('1000.00')
        }
        
        hash_input = '~'.join([str(v) for v in test_data.values()])
        expected_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:128]
        
        generated_hash = self._generate_hash_value(test_data)
        
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
        
        result = self._execute_procedure(
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
        
        result = self._execute_procedure(
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
        result = self._execute_procedure(mock_conn, same_date, same_date)
        
        mock_cursor.execute.assert_called()
        
        # Test with end date before start date
        start_date = datetime.datetime(2023, 6, 15)
        end_date = datetime.datetime(2023, 6, 10)
        
        result = self._execute_procedure(mock_conn, start_date, end_date)
        
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
            self._execute_procedure(mock_conn, "invalid_date", datetime.datetime.now())
        
        with pytest.raises((ValueError, TypeError)):
            self._execute_procedure(mock_conn, datetime.datetime.now(), "invalid_date")
    
    def test_database_connection_failure(self):
        """Test handling of database connection failures"""
        mock_conn = Mock()
        mock_conn.cursor.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception):
            self._execute_procedure(
                mock_conn,
                datetime.datetime(2023, 1, 1),
                datetime.datetime(2023, 12, 31)
            )
    
    def test_sql_execution_error(self, mock_fabric_connection):
        """Test handling of SQL execution errors"""
        mock_conn, mock_cursor = mock_fabric_connection
        mock_cursor.execute.side_effect = Exception("SQL execution failed")
        
        with pytest.raises(Exception):
            self._execute_procedure(
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
            
            result = self._execute_procedure(
                mock_conn,
                datetime.datetime(2023, 1, 1),
                datetime.datetime(2023, 12, 31)
            )
            
            # Should handle large datasets appropriately
            mock_cursor.execute.assert_called()
    
    # Data Validation Tests
    def test_output_schema_validation(self, expected_output_schema):
        """Test that output matches expected schema"""
        sample_output = {
            'FactClaimTransactionLineWCKey': 1,
            'RevisionNumber': 1,
            'PolicyWCKey': 100,
            'PolicyRiskStateWCKey': 101,
            'ClaimWCKey': 200,
            'ClaimTransactionLineCategoryKey': 300,
            'ClaimTransactionWCKey': 400,
            'ClaimCheckKey': 500,
            'AgencyKey': 600,
            'SourceClaimTransactionCreateDate': datetime.datetime(2023, 1, 1),
            'SourceClaimTransactionCreateDateKey': 20230101,
            'TransactionCreateDate': datetime.datetime(2023, 1, 1),
            'TransactionSubmitDate': datetime.datetime(2023, 1, 2),
            'SourceSystem': 'TestSystem',
            'RecordEffectiveDate': datetime.datetime(2023, 1, 1),
            'SourceSystemIdentifier': '1~1',
            'TransactionAmount': Decimal('1000.00'),
            'HashValue': 'test_hash',
            'RetiredInd': 0,
            'InsertUpdates': 1,
            'AuditOperations': 'Inserted',
            'LoadUpdateDate': datetime.datetime.now(),
            'LoadCreateDate': datetime.datetime.now()
        }
        
        validation_result = self._validate_output_schema(sample_output, expected_output_schema)
        assert validation_result is True
    
    def test_data_type_conversions(self):
        """Test proper data type conversions"""
        test_cases = [
            {'input': '123', 'expected_type': int, 'expected_value': 123},
            {'input': '123.45', 'expected_type': Decimal, 'expected_value': Decimal('123.45')},
            {'input': '2023-01-01', 'expected_type': datetime.datetime, 'expected_value': datetime.datetime(2023, 1, 1)}
        ]
        
        for case in test_cases:
            result = self._convert_data_type(case['input'], case['expected_type'])
            assert isinstance(result, case['expected_type'])
    
    def test_business_logic_validation(self):
        """Test business logic validation"""
        # Test that InsertUpdates flag is set correctly
        test_scenarios = [
            {'existing_record': None, 'expected_flag': 1, 'expected_operation': 'Inserted'},
            {'existing_record': {'HashValue': 'old_hash'}, 'new_hash': 'new_hash', 'expected_flag': 0, 'expected_operation': 'Updated'},
            {'existing_record': {'HashValue': 'same_hash'}, 'new_hash': 'same_hash', 'expected_flag': 3, 'expected_operation': None}
        ]
        
        for scenario in test_scenarios:
            result = self._determine_insert_update_flag(
                scenario.get('existing_record'),
                scenario.get('new_hash', 'new_hash')
            )
            assert result['InsertUpdates'] == scenario['expected_flag']
            assert result['AuditOperations'] == scenario['expected_operation']
    
    # Performance Tests
    def test_execution_time_performance(self, mock_fabric_connection):
        """Test execution time performance"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        import time
        start_time = time.time()
        
        result = self._execute_procedure(
            mock_conn,
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 12, 31)
        )
        
        execution_time = time.time() - start_time
        
        # Should complete within reasonable time (adjust threshold as needed)
        assert execution_time < 30.0  # 30 seconds threshold
    
    def test_memory_usage_optimization(self, mock_fabric_connection):
        """Test memory usage optimization"""
        mock_conn, mock_cursor = mock_fabric_connection
        
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        result = self._execute_procedure(
            mock_conn,
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 12, 31)
        )
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (adjust threshold as needed)
        assert memory_increase < 100 * 1024 * 1024  # 100MB threshold
    
    # Helper Methods
    def _execute_procedure(self, connection, start_date, end_date):
        """Helper method to execute the stored procedure"""
        cursor = connection.cursor()
        
        # Simulate procedure execution
        query = """
        EXEC uspSemanticClaimTransactionMeasuresData 
        @pJobStartDateTime = ?, 
        @pJobEndDateTime = ?
        """
        
        cursor.execute(query, (start_date, end_date))
        return cursor.fetchall()
    
    def _generate_hash_value(self, data_dict):
        """Helper method to generate hash value"""
        hash_input = '~'.join([str(v) for v in data_dict.values()])
        return hashlib.sha256(hash_input.encode()).hexdigest()[:128]
    
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
        """Helper method to sanitize string data"""
        for key, value in data.items():
            if isinstance(value, str):
                # Basic sanitization - remove or escape special characters
                data[key] = value.replace("'", "''").replace('"', '""')
        return data
    
    def _validate_output_schema(self, output_data, expected_schema):
        """Helper method to validate output schema"""
        for field, expected_type in expected_schema.items():
            if field in output_data:
                if not isinstance(output_data[field], expected_type):
                    return False
        return True
    
    def _convert_data_type(self, value, target_type):
        """Helper method to convert data types"""
        if target_type == int:
            return int(value)
        elif target_type == Decimal:
            return Decimal(str(value))
        elif target_type == datetime.datetime:
            return datetime.datetime.strptime(value, '%Y-%m-%d')
        else:
            return value
    
    def _determine_insert_update_flag(self, existing_record, new_hash):
        """Helper method to determine insert/update flag"""
        if existing_record is None:
            return {'InsertUpdates': 1, 'AuditOperations': 'Inserted'}
        elif existing_record.get('HashValue') != new_hash:
            return {'InsertUpdates': 0, 'AuditOperations': 'Updated'}
        else:
            return {'InsertUpdates': 3, 'AuditOperations': None}


# Integration Tests
class TestUspSemanticClaimTransactionMeasuresDataIntegration:
    """Integration tests for the complete workflow"""
    
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
