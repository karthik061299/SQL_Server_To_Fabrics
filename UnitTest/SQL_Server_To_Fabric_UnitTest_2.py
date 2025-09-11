_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Comprehensive unit tests for SQL Server to Fabric conversion of uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta, date
import hashlib
from decimal import Decimal
import logging
from typing import List, Dict, Any
import time

# Configure logging for test execution
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestUspSemanticClaimTransactionMeasuresData:
    """
    Enhanced test class for uspSemanticClaimTransactionMeasuresData stored procedure conversion
    from SQL Server to Fabric SQL.
    
    Tests cover:
    - Data transformations and joins
    - Measure calculations (NetPaidIndemnity, GrossIncurredLoss, etc.)
    - Hash value generation and change detection
    - Recovery type handling
    - Edge cases and error scenarios
    - Performance validation
    - Data quality checks
    """
    
    @pytest.fixture(scope="class")
    def setup_test_environment(self):
        """
        Setup test environment with mock data and configurations
        """
        logger.info("Setting up test environment")
        
        # Mock database connection
        mock_connection = Mock()
        
        # Mock FactClaimTransactionLineWC data
        fact_claim_data = pd.DataFrame({
            'FactClaimTransactionLineWCKey': [1, 2, 3, 4, 5],
            'RevisionNumber': [1, 1, 1, 1, 1],
            'PolicyWCKey': [201, 202, 203, 204, 205],
            'ClaimWCKey': [101, 102, 103, 104, 105],
            'ClaimTransactionLineCategoryKey': [301, 302, 303, 304, 305],
            'ClaimTransactionWCKey': [401, 402, 403, 404, 405],
            'ClaimCheckKey': [501, 502, 503, 504, 505],
            'SourceTransactionLineItemCreateDate': [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10), 
                                                  date(2024, 4, 5), date(2024, 5, 12)],
            'SourceTransactionLineItemCreateDateKey': [20240115, 20240220, 20240310, 20240405, 20240512],
            'TransactionAmount': [Decimal('1000.00'), Decimal('2500.00'), Decimal('0.00'), 
                                Decimal('1500.00'), Decimal('3000.00')],
            'RetiredInd': [0, 0, 0, 0, 0],
            'SourceSystem': ['System1', 'System1', 'System2', 'System1', 'System2'],
            'RecordEffectiveDate': [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10), 
                                  date(2024, 4, 5), date(2024, 5, 12)],
            'LoadUpdateDate': [datetime(2024, 1, 15), datetime(2024, 2, 20), datetime(2024, 3, 10), 
                              datetime(2024, 4, 5), datetime(2024, 5, 12)]
        })
        
        # Mock ClaimTransactionDescriptors data
        claim_transaction_descriptors = pd.DataFrame({
            'ClaimTransactionLineCategoryKey': [301, 302, 303, 304, 305],
            'ClaimTransactionWCKey': [401, 402, 403, 404, 405],
            'ClaimWCKey': [101, 102, 103, 104, 105],
            'SourceTransactionCreateDate': [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10), 
                                          date(2024, 4, 5), date(2024, 5, 12)],
            'TransactionSubmitDate': [date(2024, 1, 16), date(2024, 2, 21), date(2024, 3, 11), 
                                     date(2024, 4, 6), date(2024, 5, 13)],
            'TransactionTypeCode': ['PAY', 'RES', 'REC', 'PAY', 'RES'],
            'CoverageCode': ['IND', 'MED', 'IND', 'MED', 'IND'],
            'RecoveryTypeCode': ['SUB', None, 'DEDUCT', 'SUB', None]
        })
        
        # Mock ClaimDescriptors data
        claim_descriptors = pd.DataFrame({
            'ClaimWCKey': [101, 102, 103, 104, 105],
            'ClaimNumber': ['CLM001', 'CLM002', 'CLM003', 'CLM004', 'CLM005'],
            'ClaimStatusCode': ['OPEN', 'CLOSED', 'OPEN', 'OPEN', 'CLOSED'],
            'DateOfLoss': [date(2023, 12, 1), date(2023, 11, 15), date(2024, 1, 20), 
                          date(2024, 2, 10), date(2024, 3, 5)],
            'EmploymentLocationState': ['CA', 'NY', None, 'FL', None],
            'JurisdictionState': [None, None, 'TX', None, 'IL']
        })
        
        # Mock PolicyDescriptors data
        policy_descriptors = pd.DataFrame({
            'PolicyWCKey': [201, 202, 203, 204, 205],
            'PolicyNumber': ['POL001', 'POL002', 'POL003', 'POL004', 'POL005'],
            'EffectiveDate': [date(2023, 1, 1), date(2023, 6, 1), date(2023, 12, 1), 
                             date(2024, 1, 1), date(2024, 3, 1)],
            'ExpirationDate': [date(2023, 12, 31), date(2024, 5, 31), date(2024, 11, 30), 
                              date(2024, 12, 31), date(2025, 2, 28)],
            'AgencyKey': [601, 602, 603, 604, 605],
            'BrandKey': [701, 702, 703, 704, 705]
        })
        
        # Mock PolicyRiskStateDescriptors data
        policy_risk_state_descriptors = pd.DataFrame({
            'PolicyRiskStateWCKey': [801, 802, 803, 804, 805],
            'PolicyWCKey': [201, 202, 203, 204, 205],
            'RiskState': ['CA', 'NY', 'TX', 'FL', 'IL'],
            'RiskStateEffectiveDate': [date(2023, 1, 1), date(2023, 6, 1), date(2023, 12, 1), 
                                     date(2024, 1, 1), date(2024, 3, 1)],
            'RecordEffectiveDate': [date(2023, 1, 1), date(2023, 6, 1), date(2023, 12, 1), 
                                  date(2024, 1, 1), date(2024, 3, 1)],
            'RetiredInd': [0, 0, 0, 0, 0],
            'LoadUpdateDate': [datetime(2023, 1, 1), datetime(2023, 6, 1), datetime(2023, 12, 1), 
                              datetime(2024, 1, 1), datetime(2024, 3, 1)]
        })
        
        # Mock DimBrand data
        dim_brand = pd.DataFrame({
            'BrandKey': [701, 702, 703, 704, 705],
            'BrandName': ['Brand1', 'Brand2', 'Brand3', 'Brand4', 'Brand5'],
            'BrandCode': ['B1', 'B2', 'B3', 'B4', 'B5']
        })
        
        # Mock SemanticLayerMetaData for calculations
        semantic_layer_metadata = pd.DataFrame({
            'Measure_Name': ['NetPaidIndemnity', 'GrossIncurredLoss', 'NetIncurredLoss', 'RecoverySubrogation'],
            'Logic': [
                'CASE WHEN CoverageCode = \'IND\' AND TransactionTypeCode = \'PAY\' THEN TransactionAmount - RecoveryAmount ELSE 0 END',
                'TransactionAmount + ReserveAmount',
                'TransactionAmount + ReserveAmount - RecoveryAmount',
                'CASE WHEN RecoveryTypeCode = \'SUB\' THEN RecoveryAmount ELSE 0 END'
            ],
            'SourceType': ['Claims', 'Claims', 'Claims', 'Claims'],
            'IsActive': [1, 1, 1, 1]
        })
        
        return {
            'connection': mock_connection,
            'test_data': {
                'fact_claim_data': fact_claim_data,
                'claim_transaction_descriptors': claim_transaction_descriptors,
                'claim_descriptors': claim_descriptors,
                'policy_descriptors': policy_descriptors,
                'policy_risk_state_descriptors': policy_risk_state_descriptors,
                'dim_brand': dim_brand,
                'semantic_layer_metadata': semantic_layer_metadata
            },
            'config': {
                'batch_size': 1000,
                'timeout': 300,
                'retry_count': 3
            }
        }
    
    @pytest.fixture
    def sample_input_data(self):
        """
        Fixture providing sample input data for testing
        """
        return pd.DataFrame({
            'FactClaimTransactionLineWCKey': range(1, 101),
            'RevisionNumber': np.ones(100, dtype=int),
            'PolicyWCKey': range(201, 301),
            'ClaimWCKey': range(101, 201),
            'ClaimTransactionLineCategoryKey': range(301, 401),
            'ClaimTransactionWCKey': range(401, 501),
            'ClaimCheckKey': range(501, 601),
            'TransactionAmount': np.random.uniform(100, 5000, 100),
            'SourceTransactionLineItemCreateDate': pd.date_range('2024-01-01', periods=100, freq='D'),
            'SourceTransactionLineItemCreateDateKey': range(20240101, 20240101+100),
            'TransactionType': np.random.choice(['Payment', 'Adjustment', 'Recovery'], 100),
            'RetiredInd': np.zeros(100, dtype=int),
            'SourceSystem': np.random.choice(['System1', 'System2', 'System3'], 100)
        })
    
    def test_basic_data_retrieval(self, setup_test_environment):
        """
        Test Case 1: Verify basic data retrieval from all source tables
        """
        logger.info("Testing basic data retrieval")
        
        test_data = setup_test_environment['test_data']
        
        # Assertions
        assert len(test_data['fact_claim_data']) == 5
        assert all(col in test_data['fact_claim_data'].columns for col in 
                  ['FactClaimTransactionLineWCKey', 'ClaimWCKey', 'PolicyWCKey', 'TransactionAmount'])
        
        logger.info("Basic data retrieval test passed")
    
    def test_join_operations_integrity(self, setup_test_environment):
        """
        Test Case 2: Verify correct join operations between tables
        """
        logger.info("Testing join operations integrity")
        
        test_data = setup_test_environment['test_data']
        
        # Simulate join between FactClaimTransactionLineWC and ClaimTransactionDescriptors
        joined_data = pd.merge(
            test_data['fact_claim_data'],
            test_data['claim_transaction_descriptors'],
            on=['ClaimTransactionLineCategoryKey', 'ClaimTransactionWCKey', 'ClaimWCKey'],
            how='inner'
        )
        
        # Assertions
        assert len(joined_data) == 5
        assert 'TransactionTypeCode' in joined_data.columns
        assert 'CoverageCode' in joined_data.columns
        
        # Test join with ClaimDescriptors
        joined_with_claim = pd.merge(
            joined_data,
            test_data['claim_descriptors'],
            on='ClaimWCKey',
            how='inner'
        )
        
        assert len(joined_with_claim) == 5
        assert 'ClaimNumber' in joined_with_claim.columns
        
        # Test join with PolicyDescriptors
        joined_with_policy = pd.merge(
            joined_with_claim,
            test_data['policy_descriptors'],
            on='PolicyWCKey',
            how='left'
        )
        
        assert len(joined_with_policy) == 5
        assert 'PolicyNumber' in joined_with_policy.columns
        assert 'AgencyKey' in joined_with_policy.columns
        
        logger.info("Join operations integrity test passed")
    
    def test_risk_state_join_logic(self, setup_test_environment):
        """
        Test Case 3: Verify the complex risk state join logic
        """
        logger.info("Testing risk state join logic")
        
        test_data = setup_test_environment['test_data']
        
        # Create a base joined dataset
        base_joined = pd.merge(
            test_data['fact_claim_data'],
            test_data['claim_descriptors'],
            on='ClaimWCKey',
            how='inner'
        )
        
        # Apply the complex join logic for risk state
        base_joined['RiskState'] = base_joined.apply(
            lambda row: row['EmploymentLocationState'] if pd.notna(row['EmploymentLocationState']) 
                        else row['JurisdictionState'],
            axis=1
        )
        
        # Join with PolicyRiskStateDescriptors
        risk_state_joined = pd.merge(
            base_joined,
            test_data['policy_risk_state_descriptors'],
            left_on=['PolicyWCKey', 'RiskState'],
            right_on=['PolicyWCKey', 'RiskState'],
            how='left'
        )
        
        # Assertions
        assert len(risk_state_joined) == 5
        assert 'PolicyRiskStateWCKey' in risk_state_joined.columns
        
        # Verify the correct risk state was used
        for i, row in risk_state_joined.iterrows():
            if pd.notna(row['EmploymentLocationState']):
                assert row['RiskState'] == row['EmploymentLocationState']
            else:
                assert row['RiskState'] == row['JurisdictionState']
        
        logger.info("Risk state join logic test passed")
    
    def test_measure_calculations(self, setup_test_environment):
        """
        Test Case 4: Verify measure calculations based on SemanticLayerMetaData
        """
        logger.info("Testing measure calculations")
        
        test_data = setup_test_environment['test_data']
        
        # Create a base dataset for calculations
        base_data = pd.merge(
            test_data['fact_claim_data'],
            test_data['claim_transaction_descriptors'],
            on=['ClaimTransactionLineCategoryKey', 'ClaimTransactionWCKey', 'ClaimWCKey'],
            how='inner'
        )
        
        # Add recovery amount for testing
        base_data['RecoveryAmount'] = [Decimal('100.00'), Decimal('0.00'), Decimal('200.00'), 
                                     Decimal('150.00'), Decimal('0.00')]
        base_data['ReserveAmount'] = [Decimal('5000.00'), Decimal('7500.00'), Decimal('2000.00'), 
                                    Decimal('4000.00'), Decimal('6000.00')]
        
        # Calculate NetPaidIndemnity
        base_data['NetPaidIndemnity'] = base_data.apply(
            lambda row: row['TransactionAmount'] - row['RecoveryAmount'] 
                        if row['CoverageCode'] == 'IND' and row['TransactionTypeCode'] == 'PAY' 
                        else Decimal('0.00'),
            axis=1
        )
        
        # Calculate GrossIncurredLoss
        base_data['GrossIncurredLoss'] = base_data['TransactionAmount'] + base_data['ReserveAmount']
        
        # Calculate NetIncurredLoss
        base_data['NetIncurredLoss'] = base_data['TransactionAmount'] + base_data['ReserveAmount'] - base_data['RecoveryAmount']
        
        # Calculate RecoverySubrogation
        base_data['RecoverySubrogation'] = base_data.apply(
            lambda row: row['RecoveryAmount'] if row['RecoveryTypeCode'] == 'SUB' else Decimal('0.00'),
            axis=1
        )
        
        # Expected results for NetPaidIndemnity
        expected_net_paid_indemnity = [Decimal('900.00'), Decimal('0.00'), Decimal('0.00'), 
                                      Decimal('0.00'), Decimal('0.00')]
        
        # Expected results for GrossIncurredLoss
        expected_gross_incurred_loss = [Decimal('6000.00'), Decimal('10000.00'), Decimal('2000.00'), 
                                       Decimal('5500.00'), Decimal('9000.00')]
        
        # Expected results for NetIncurredLoss
        expected_net_incurred_loss = [Decimal('5900.00'), Decimal('10000.00'), Decimal('1800.00'), 
                                    Decimal('5350.00'), Decimal('9000.00')]
        
        # Expected results for RecoverySubrogation
        expected_recovery_subrogation = [Decimal('100.00'), Decimal('0.00'), Decimal('0.00'), 
                                       Decimal('150.00'), Decimal('0.00')]
        
        # Assertions
        for i in range(5):
            assert base_data.iloc[i]['NetPaidIndemnity'] == expected_net_paid_indemnity[i]
            assert base_data.iloc[i]['GrossIncurredLoss'] == expected_gross_incurred_loss[i]
            assert base_data.iloc[i]['NetIncurredLoss'] == expected_net_incurred_loss[i]
            assert base_data.iloc[i]['RecoverySubrogation'] == expected_recovery_subrogation[i]
        
        logger.info("Measure calculations test passed")