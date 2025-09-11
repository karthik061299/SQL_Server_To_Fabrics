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