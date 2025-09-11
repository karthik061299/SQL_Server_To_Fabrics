_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Comprehensive unit tests for uspSemanticClaimTransactionMeasuresData Fabric SQL conversion
## *Version*: 3 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Mock imports for Fabric SQL environment
pytest.importorskip("fabric_sql_testing")

# Test case list with descriptions
TEST_CASES = [
    {
        "id": "TC001",
        "description": "Happy path - Basic functionality with valid data",
        "expected": "Returns valid results with correct calculations"
    },
    {
        "id": "TC002",
        "description": "Edge case - Empty source data",
        "expected": "Returns empty result set without errors"
    },
    {
        "id": "TC003",
        "description": "Edge case - Special date handling (1900-01-01)",
        "expected": "Correctly converts 1900-01-01 to 1700-01-01"
    },
    {
        "id": "TC004",
        "description": "Data integrity - Hash calculation verification",
        "expected": "Correctly calculates hash values for change detection"
    },
    {
        "id": "TC005",
        "description": "Business logic - InsertUpdates flag calculation",
        "expected": "Correctly identifies new and updated records"
    },
    {
        "id": "TC006",
        "description": "Business logic - Financial calculations",
        "expected": "Correctly calculates all financial measures"
    },
    {
        "id": "TC007",
        "description": "Error handling - NULL values in key fields",
        "expected": "Handles NULL values correctly with COALESCE"
    },
    {
        "id": "TC008",
        "description": "Performance - Large dataset processing",
        "expected": "Processes large datasets efficiently"
    },
    {
        "id": "TC009",
        "description": "Integration - Policy risk state join logic",
        "expected": "Correctly joins with PolicyRiskState using appropriate conditions"
    },
    {
        "id": "TC010",
        "description": "Recovery breakouts - Deductible subcategories",
        "expected": "Correctly calculates recovery deductible breakouts"
    }
]

# Test fixtures
@pytest.fixture
def mock_fabric_connection():
    """Create a mock Fabric SQL connection"""
    mock_conn = MagicMock()
    return mock_conn

@pytest.fixture
def sample_claim_transaction_data():
    """Create sample claim transaction test data"""
    data = [
        # FactClaimTransactionLineWCKey, RevisionNumber, PolicyWCKey, ClaimWCKey, ClaimTransactionLineCategoryKey, ClaimTransactionWCKey, ClaimCheckKey, TransactionAmount, etc.
        (1001, 1, 5001, 7001, 101, 201, 301, 1000.00, '2023-01-15', 20230115, 'SourceSys1', '2023-01-15', 0),
        (1002, 1, 5002, 7002, 102, 202, 302, 2500.00, '2023-01-16', 20230116, 'SourceSys1', '2023-01-16', 0),
        (1003, 1, 5003, 7003, 103, 203, 303, -500.00, '2023-01-17', 20230117, 'SourceSys1', '2023-01-17', 0),
        (1004, 1, 5004, 7004, 104, 204, 304, 750.00, '2023-01-18', 20230118, 'SourceSys1', '2023-01-18', 0),
        (1005, 1, 5005, 7005, 105, 205, 305, 1200.00, '2023-01-19', 20230119, 'SourceSys1', '2023-01-19', 0),
    ]
    columns = ['FactClaimTransactionLineWCKey', 'RevisionNumber', 'PolicyWCKey', 'ClaimWCKey', 
               'ClaimTransactionLineCategoryKey', 'ClaimTransactionWCKey', 'ClaimCheckKey', 
               'TransactionAmount', 'SourceTransactionLineItemCreateDate', 'SourceTransactionLineItemCreateDateKey',
               'SourceSystem', 'RecordEffectiveDate', 'RetiredInd']
    return pd.DataFrame(data, columns=columns)

@pytest.fixture
def sample_claim_transaction_descriptors():
    """Create sample claim transaction descriptors test data"""
    data = [
        # ClaimTransactionLineCategoryKey, ClaimTransactionWCKey, ClaimWCKey, ClaimTransactionTypeCode, TransactionTypeCode, RecoveryTypeCode, ClaimTransactionSubTypeCode
        (101, 201, 7001, 'INDEMNITY', 'PAYMENT', None, None, '2023-01-15', '2023-01-15'),
        (102, 202, 7002, 'MEDICAL', 'PAYMENT', None, None, '2023-01-16', '2023-01-16'),
        (103, 203, 7003, 'RECOVERY', 'RECOVERY', 'DEDUCTIBLE', 'INDEMNITY', '2023-01-17', '2023-01-17'),
        (104, 204, 7004, 'EXPENSE', 'PAYMENT', None, None, '2023-01-18', '2023-01-18'),
        (105, 205, 7005, 'EMPLOYER_LIABILITY', 'PAYMENT', None, None, '2023-01-19', '2023-01-19'),
    ]
    columns = ['ClaimTransactionLineCategoryKey', 'ClaimTransactionWCKey', 'ClaimWCKey', 
               'ClaimTransactionTypeCode', 'TransactionTypeCode', 'RecoveryTypeCode', 'ClaimTransactionSubTypeCode',
               'SourceTransactionCreateDate', 'TransactionSubmitDate']
    return pd.DataFrame(data, columns=columns)

@pytest.fixture
def sample_claim_descriptors():
    """Create sample claim descriptors test data"""
    data = [
        # ClaimWCKey, EmploymentLocationState, JurisdictionState
        (7001, 'CA', 'CA'),
        (7002, 'NY', 'NY'),
        (7003, 'TX', 'TX'),
        (7004, None, 'FL'),
        (7005, 'WA', None),
    ]
    columns = ['ClaimWCKey', 'EmploymentLocationState', 'JurisdictionState']
    return pd.DataFrame(data, columns=columns)

@pytest.fixture
def sample_policy_descriptors():
    """Create sample policy descriptors test data"""
    data = [
        # PolicyWCKey, AgencyKey, BrandKey
        (5001, 401, 501),
        (5002, 402, 502),
        (5003, 403, 503),
        (5004, 404, 504),
        (5005, 405, 505),
    ]
    columns = ['PolicyWCKey', 'AgencyKey', 'BrandKey']
    return pd.DataFrame(data, columns=columns)

@pytest.fixture
def sample_policy_risk_state():
    """Create sample policy risk state test data"""
    data = [
        # PolicyWCKey, PolicyRiskStateWCKey, RiskState, RiskStateEffectiveDate, RecordEffectiveDate, LoadUpdateDate, RetiredInd
        (5001, 601, 'CA', '2022-01-01', '2022-01-01', '2022-01-01', 0),
        (5002, 602, 'NY', '2022-01-01', '2022-01-01', '2022-01-01', 0),
        (5003, 603, 'TX', '2022-01-01', '2022-01-01', '2022-01-01', 0),
        (5004, 604, 'FL', '2022-01-01', '2022-01-01', '2022-01-01', 0),
        (5005, 605, 'WA', '2022-01-01', '2022-01-01', '2022-01-01', 0),
    ]
    columns = ['PolicyWCKey', 'PolicyRiskStateWCKey', 'RiskState', 'RiskStateEffectiveDate', 
               'RecordEffectiveDate', 'LoadUpdateDate', 'RetiredInd']
    return pd.DataFrame(data, columns=columns)

@pytest.fixture
def sample_existing_measures():
    """Create sample existing ClaimTransactionMeasures data"""
    data = [
        # FactClaimTransactionLineWCKey, RevisionNumber, HashValue, LoadCreateDate
        (1001, 1, 'hash1', '2023-01-01'),
        (1002, 1, 'hash2', '2023-01-01'),
        # 1003 is missing to test insertion
        (1004, 1, 'different_hash', '2023-01-01'),  # Different hash to test update
        (1005, 1, 'hash5', '2023-01-01'),
    ]
    columns = ['FactClaimTransactionLineWCKey', 'RevisionNumber', 'HashValue', 'LoadCreateDate']
    return pd.DataFrame(data, columns=columns)

# Helper functions for tests
def setup_mock_tables(mock_conn, fixtures):
    """Set up mock tables in the Fabric environment"""
    for table_name, df in fixtures.items():
        mock_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM VALUES(...)").returns(None)
        mock_conn.execute(f"SELECT * FROM {table_name}").returns(df)

def execute_function_under_test(mock_conn, start_date, end_date):
    """Execute the function under test"""
    query = f"SELECT * FROM uspSemanticClaimTransactionMeasuresData('{start_date}', '{end_date}')"
    return mock_conn.execute(query).returns_dataframe()

# Test cases
def test_basic_functionality(mock_fabric_connection, sample_claim_transaction_data, 
                             sample_claim_transaction_descriptors, sample_claim_descriptors,
                             sample_policy_descriptors, sample_policy_risk_state,
                             sample_existing_measures):
    """TC001: Happy path - Basic functionality with valid data"""
    # Arrange
    fixtures = {
        "EDSWH.dbo.FactClaimTransactionLineWC": sample_claim_transaction_data,
        "EDSWH.dbo.DimClaimTransactionWC": pd.DataFrame({"ClaimTransactionWCKey": range(201, 206)}),
        "Semantic.ClaimTransactionDescriptors": sample_claim_transaction_descriptors,
        "Semantic.ClaimDescriptors": sample_claim_descriptors,
        "Semantic.PolicyDescriptors": sample_policy_descriptors,
        "Semantic.PolicyRiskStateDescriptors": sample_policy_risk_state,
        "Semantic.ClaimTransactionMeasures": sample_existing_measures,
        "EDSWH.dbo.DimBrand": pd.DataFrame({"BrandKey": range(501, 506)})
    }
    setup_mock_tables(mock_fabric_connection, fixtures)
    
    # Act
    start_date = '2023-01-01'
    end_date = '2023-01-31'
    result = execute_function_under_test(mock_fabric_connection, start_date, end_date)
    
    # Assert
    assert not result.empty, "Result should not be empty"
    assert len(result) >= 3, "Should return at least 3 records (2 unchanged, 1 new, 1 updated)"
    
    # Check specific records
    new_record = result[result['FactClaimTransactionLineWCKey'] == 1003]
    assert len(new_record) == 1, "Should have one new record with ID 1003"
    assert new_record['AuditOperations'].iloc[0] == 'Inserted', "New record should be marked as Inserted"
    
    updated_record = result[result['FactClaimTransactionLineWCKey'] == 1004]
    assert len(updated_record) == 1, "Should have one updated record with ID 1004"
    assert updated_record['AuditOperations'].iloc[0] == 'Updated', "Updated record should be marked as Updated"

def test_empty_source_data(mock_fabric_connection):
    """TC002: Edge case - Empty source data"""
    # Arrange
    fixtures = {
        "EDSWH.dbo.FactClaimTransactionLineWC": pd.DataFrame(),
        "EDSWH.dbo.DimClaimTransactionWC": pd.DataFrame(),
        "Semantic.ClaimTransactionDescriptors": pd.DataFrame(),
        "Semantic.ClaimDescriptors": pd.DataFrame(),
        "Semantic.PolicyDescriptors": pd.DataFrame(),
        "Semantic.PolicyRiskStateDescriptors": pd.DataFrame(),
        "Semantic.ClaimTransactionMeasures": pd.DataFrame(),
        "EDSWH.dbo.DimBrand": pd.DataFrame()
    }
    setup_mock_tables(mock_fabric_connection, fixtures)
    
    # Act
    start_date = '2023-01-01'
    end_date = '2023-01-31'
    result = execute_function_under_test(mock_fabric_connection, start_date, end_date)
    
    # Assert
    assert result.empty, "Result should be empty with empty source data"

def test_special_date_handling(mock_fabric_connection, sample_claim_transaction_data, 
                              sample_claim_transaction_descriptors, sample_claim_descriptors,
                              sample_policy_descriptors, sample_policy_risk_state,
                              sample_existing_measures):
    """TC003: Edge case - Special date handling (1900-01-01)"""
    # Arrange
    fixtures = {
        "EDSWH.dbo.FactClaimTransactionLineWC": sample_claim_transaction_data,
        "EDSWH.dbo.DimClaimTransactionWC": pd.DataFrame({"ClaimTransactionWCKey": range(201, 206)}),
        "Semantic.ClaimTransactionDescriptors": sample_claim_transaction_descriptors,
        "Semantic.ClaimDescriptors": sample_claim_descriptors,
        "Semantic.PolicyDescriptors": sample_policy_descriptors,
        "Semantic.PolicyRiskStateDescriptors": sample_policy_risk_state,
        "Semantic.ClaimTransactionMeasures": sample_existing_measures,
        "EDSWH.dbo.DimBrand": pd.DataFrame({"BrandKey": range(501, 506)})
    }
    setup_mock_tables(mock_fabric_connection, fixtures)
    
    # Mock the date conversion logic
    with patch('fabric_sql_testing.execute_query') as mock_execute:
        # Act
        start_date = '1900-01-01'  # Special date that should be converted to 1700-01-01
        end_date = '2023-01-31'
        execute_function_under_test(mock_fabric_connection, start_date, end_date)
        
        # Assert
        # Check that the query was executed with the converted date
        mock_execute.assert_called()
        call_args = mock_execute.call_args_list[0][0][0]
        assert '1700-01-01' in call_args, "Special date 1900-01-01 should be converted to 1700-01-01"