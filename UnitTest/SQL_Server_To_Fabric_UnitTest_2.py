_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Unit tests for the Semantic.uspSemanticClaimTransactionMeasuresData stored procedure converted to Fabric SQL
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, date
import pyodbc
import logging
from typing import List, Dict, Any, Optional

# Configure logging for test execution
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestUspSemanticClaimTransactionMeasuresData:
    """
    Comprehensive unit test suite for SQL Server to Fabric conversion
    of uspSemanticClaimTransactionMeasuresData stored procedure.
    
    This test suite covers:
    - Data integrity validation
    - Complex financial calculations
    - Dynamic SQL generation
    - Temporary table operations
    - Edge cases and error handling
    - Performance validation
    """
    
    @pytest.fixture
    def mock_connection(self):
        """Mock database connection for testing"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def sample_claim_data(self):
        """Sample claim transaction data for testing"""
        return pd.DataFrame({
            'FactClaimTransactionLineWCKey': [1, 2, 3, 4, 5],
            'RevisionNumber': [1, 1, 1, 1, 1],
            'PolicyWCKey': [101, 102, 103, 104, 105],
            'ClaimWCKey': [201, 202, 203, 204, 205],
            'ClaimTransactionLineCategoryKey': [301, 302, 303, 304, 305],
            'ClaimTransactionWCKey': [401, 402, 403, 404, 405],
            'ClaimCheckKey': [501, 502, 503, 504, 505],
            'TransactionAmount': [1000.50, 2500.75, 750.25, 3200.00, 1800.90],
            'NetPaidIndemnity': [500.25, 1200.50, 350.75, 1600.00, 900.45],
            'NetPaidMedical': [450.25, 1300.25, 399.50, 1600.00, 900.45],
            'NetPaidExpense': [50.00, 0.00, 0.00, 0.00, 0.00],
            'GrossPaidIndemnity': [550.25, 1250.50, 375.75, 1650.00, 925.45],
            'GrossPaidMedical': [500.25, 1350.25, 424.50, 1650.00, 925.45],
            'RecoveryDeductible': [50.00, 100.00, 50.00, 100.00, 50.00],
            'SourceClaimTransactionCreateDate': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'],
            'SourceClaimTransactionCreateDateKey': [20240115, 20240116, 20240117, 20240118, 20240119],
            'RetiredInd': [0, 0, 0, 0, 0]
        })
    
    @pytest.fixture
    def sample_measures_config(self):
        """Sample configuration for measures calculation"""
        return {
            'measure_types': ['NetPaidIndemnity', 'NetPaidMedical', 'NetPaidExpense', 'GrossPaidIndemnity', 'GrossPaidMedical'],
            'aggregation_levels': ['PolicyWCKey', 'ClaimWCKey', 'ClaimTransactionLineCategoryKey'],
            'date_ranges': ['Monthly', 'Quarterly', 'Yearly'],
            'filters': {
                'min_transaction_amount': 0,
                'max_transaction_amount': 100000,
                'retired_ind': 0
            }
        }
    
    def test_basic_data_retrieval(self, mock_connection, sample_claim_data):
        """Test basic data retrieval functionality"""
        conn, cursor = mock_connection
        cursor.fetchall.return_value = sample_claim_data.values.tolist()
        
        # Simulate the stored procedure call
        result = self._execute_fabric_procedure(conn, {
            'pJobStartDateTime': '2024-01-01',
            'pJobEndDateTime': '2024-01-31'
        })
        
        assert result is not None
        assert len(result) == len(sample_claim_data)
        logger.info("Basic data retrieval test passed")
    
    def test_financial_calculations_accuracy(self, sample_claim_data):
        """Test accuracy of financial calculations"""
        # Test total net paid indemnity calculation
        expected_total_net_paid_indemnity = sample_claim_data['NetPaidIndemnity'].sum()
        calculated_total = self._calculate_total_net_paid_indemnity(sample_claim_data)
        
        assert abs(expected_total_net_paid_indemnity - calculated_total) < 0.01, f"Expected {expected_total_net_paid_indemnity}, got {calculated_total}"
        
        # Test total net paid medical calculation
        expected_total_net_paid_medical = sample_claim_data['NetPaidMedical'].sum()
        calculated_total = self._calculate_total_net_paid_medical(sample_claim_data)
        
        assert abs(expected_total_net_paid_medical - calculated_total) < 0.01, f"Expected {expected_total_net_paid_medical}, got {calculated_total}"
        
        # Test gross vs net ratio
        expected_ratio = sample_claim_data['GrossPaidIndemnity'].sum() / sample_claim_data['NetPaidIndemnity'].sum()
        calculated_ratio = self._calculate_gross_net_ratio(sample_claim_data, 'Indemnity')
        
        assert abs(expected_ratio - calculated_ratio) < 0.01, f"Expected {expected_ratio}, got {calculated_ratio}"
        
        logger.info("Financial calculations accuracy test passed")
    
    def test_aggregation_by_policy(self, sample_claim_data):
        """Test aggregation functionality by policy"""
        policy_aggregation = self._aggregate_by_policy(sample_claim_data)
        
        # Verify all policies are included
        unique_policies = sample_claim_data['PolicyWCKey'].unique()
        assert len(policy_aggregation) == len(unique_policies)
        
        # Verify aggregation accuracy for first policy
        policy_101_data = sample_claim_data[sample_claim_data['PolicyWCKey'] == 101]
        policy_101_agg = policy_aggregation[policy_aggregation['PolicyWCKey'] == 101]
        
        expected_total = policy_101_data['TransactionAmount'].sum()
        actual_total = policy_101_agg['TotalTransactionAmount'].iloc[0]
        
        assert abs(expected_total - actual_total) < 0.01
        logger.info("Policy aggregation test passed")
    
    def test_aggregation_by_claim_transaction_category(self, sample_claim_data):
        """Test aggregation functionality by claim transaction category"""
        category_aggregation = self._aggregate_by_claim_transaction_category(sample_claim_data)
        
        # Verify all categories are included
        unique_categories = sample_claim_data['ClaimTransactionLineCategoryKey'].unique()
        assert len(category_aggregation) == len(unique_categories)
        
        # Test first category aggregation
        category_301_data = sample_claim_data[sample_claim_data['ClaimTransactionLineCategoryKey'] == 301]
        category_301_agg = category_aggregation[category_aggregation['ClaimTransactionLineCategoryKey'] == 301]
        
        expected_count = len(category_301_data)
        actual_count = category_301_agg['ClaimCount'].iloc[0]
        
        assert expected_count == actual_count
        logger.info("Claim transaction category aggregation test passed")
    
    def test_date_range_filtering(self, sample_claim_data):
        """Test date range filtering functionality"""
        # Test date range filtering
        start_date = '2024-01-15'
        end_date = '2024-01-17'
        
        filtered_data = self._filter_by_date_range(sample_claim_data, start_date, end_date)
        
        # Verify filtering worked correctly
        assert len(filtered_data) <= len(sample_claim_data)
        
        # Verify all dates are within range
        for _, row in filtered_data.iterrows():
            processed_date = pd.to_datetime(row['SourceClaimTransactionCreateDate'])
            assert pd.to_datetime(start_date) <= processed_date <= pd.to_datetime(end_date)
        
        logger.info("Date range filtering test passed")
    
    def test_edge_case_empty_dataset(self):
        """Test handling of empty dataset"""
        empty_data = pd.DataFrame()
        
        result = self._calculate_total_net_paid_indemnity(empty_data)
        assert result == 0 or pd.isna(result)
        
        result = self._calculate_total_net_paid_medical(empty_data)
        assert result == 0 or pd.isna(result)
        
        logger.info("Empty dataset edge case test passed")
    
    def test_edge_case_null_values(self):
        """Test handling of null values in critical fields"""
        data_with_nulls = pd.DataFrame({
            'FactClaimTransactionLineWCKey': [1, 2, 3],
            'NetPaidIndemnity': [500.25, None, 350.75],
            'NetPaidMedical': [450.25, 1300.25, None],
            'SourceClaimTransactionCreateDate': ['2024-01-15', None, '2024-01-17']
        })
        
        # Test that null handling doesn't break calculations
        try:
            total_indemnity = self._calculate_total_net_paid_indemnity_with_null_handling(data_with_nulls)
            assert total_indemnity >= 0
            
            total_medical = self._calculate_total_net_paid_medical_with_null_handling(data_with_nulls)
            assert total_medical >= 0
            
        except Exception as e:
            pytest.fail(f"Null value handling failed: {str(e)}")
        
        logger.info("Null values edge case test passed")
    
    def test_edge_case_extreme_values(self):
        """Test handling of extreme values"""
        extreme_data = pd.DataFrame({
            'FactClaimTransactionLineWCKey': [1, 2, 3],
            'NetPaidIndemnity': [0.01, 999999.99, 0.00],
            'NetPaidMedical': [0.01, 999999.99, 0.00],
            'TransactionAmount': [0.01, 999999.99, 0.00]
        })
        
        # Test calculations with extreme values
        total_indemnity = self._calculate_total_net_paid_indemnity(extreme_data)
        assert total_indemnity >= 0
        
        total_medical = self._calculate_total_net_paid_medical(extreme_data)
        assert total_medical >= 0
        
        logger.info("Extreme values edge case test passed")
    
    def test_dynamic_sql_generation(self, sample_measures_config):
        """Test dynamic SQL generation functionality"""
        # Test SQL generation for different measure types
        for measure_type in sample_measures_config['measure_types']:
            sql_query = self._generate_dynamic_sql(measure_type, sample_measures_config)
            
            # Basic SQL validation
            assert sql_query is not None
            assert len(sql_query) > 0
            assert 'SELECT' in sql_query.upper()
            assert measure_type.lower() in sql_query.lower()
        
        logger.info("Dynamic SQL generation test passed")
    
    def test_temporary_table_operations(self, mock_connection, sample_claim_data):
        """Test temporary table creation and operations"""
        conn, cursor = mock_connection
        
        # Simulate temporary table creation
        temp_table_name = "##CTM" + str(123)  # Simulating session ID
        
        # Test temp table creation SQL
        create_sql = self._generate_temp_table_sql(temp_table_name, sample_claim_data.columns.tolist())
        
        assert create_sql is not None
        assert temp_table_name in create_sql
        assert 'CREATE' in create_sql.upper()
        
        # Test temp table population
        insert_sql = self._generate_temp_table_insert_sql(temp_table_name, sample_claim_data)
        
        assert insert_sql is not None
        assert 'INSERT' in insert_sql.upper()
        assert temp_table_name in insert_sql
        
        logger.info("Temporary table operations test passed")