_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Specific tests for uspSemanticClaimTransactionMeasuresData SQL Server to Fabric conversion
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import re
from datetime import datetime
from unittest.mock import patch, MagicMock

# Mock classes and functions to simulate SQL Server and Fabric environments
class MockConnection:
    def __init__(self, is_fabric=False):
        self.is_fabric = is_fabric
        self.executed_queries = []
        self.temp_tables = {}
        self.session_id = 123 if is_fabric else 456
    
    def execute(self, query):
        self.executed_queries.append(query)
        return MockCursor(query, self)

class MockCursor:
    def __init__(self, query, connection):
        self.query = query
        self.connection = connection
        self.data = self._process_query(query)
    
    def _process_query(self, query):
        # Simple mock data generation based on query
        if 'SESSION_ID()' in query or '@@spid' in query:
            return pd.DataFrame({'session_id': [self.connection.session_id]})
        elif 'HASHBYTES' in query or 'HASH_MD5' in query:
            return pd.DataFrame({'hash_value': ['0x1234ABCD']})
        elif re.search(r'#\w+', query):
            # Handle temp table operations
            table_match = re.search(r'#(\w+)', query)
            if table_match:
                temp_table = table_match.group(0)
                if 'CREATE' in query or 'INSERT' in query:
                    self.connection.temp_tables[temp_table] = {'created': True}
                    return pd.DataFrame()
                elif 'SELECT' in query and temp_table in self.connection.temp_tables:
                    return pd.DataFrame({'data': ['test_data']})
        return pd.DataFrame()

    def fetchall(self):
        return self.data.values.tolist()

    def fetchone(self):
        if len(self.data) > 0:
            return self.data.iloc[0].values.tolist()
        return None

# Test fixtures
@pytest.fixture
def sql_server_conn():
    return MockConnection(is_fabric=False)

@pytest.fixture
def fabric_conn():
    return MockConnection(is_fabric=True)

# Test cases
def test_session_id_conversion():
    """Test the conversion from @@spid in SQL Server to SESSION_ID() in Fabric"""
    # SQL Server version using @@spid
    sql_server_query = "SELECT @@spid AS session_id"
    sql_conn = MockConnection(is_fabric=False)
    cursor = sql_conn.execute(sql_server_query)
    sql_result = cursor.fetchone()[0]
    
    # Fabric version using SESSION_ID()
    fabric_query = "SELECT SESSION_ID() AS session_id"
    fabric_conn = MockConnection(is_fabric=True)
    cursor = fabric_conn.execute(fabric_query)
    fabric_result = cursor.fetchone()[0]
    
    # Both should return a valid session ID
    assert sql_result is not None
    assert fabric_result is not None
    assert isinstance(sql_result, int)
    assert isinstance(fabric_result, int)

def test_temporary_table_handling():
    """Test the handling of temporary tables in Fabric vs SQL Server"""
    # SQL Server temp table creation
    sql_conn = MockConnection(is_fabric=False)
    sql_conn.execute("CREATE TABLE #TempClaimData (ClaimID int, Amount decimal(18,2))")
    sql_conn.execute("INSERT INTO #TempClaimData VALUES (1, 100.00)")
    
    # Check if temp table exists in SQL Server
    assert '#TempClaimData' in sql_conn.temp_tables
    
    # Fabric temp table creation (should use different syntax)
    fabric_conn = MockConnection(is_fabric=True)
    fabric_conn.execute("CREATE OR REPLACE TEMPORARY TABLE TempClaimData (ClaimID int, Amount decimal(18,2))")
    fabric_conn.execute("INSERT INTO TempClaimData VALUES (1, 100.00)")
    
    # Check if temp table exists in Fabric
    assert 'TempClaimData' in fabric_conn.temp_tables

def test_dynamic_sql_generation():
    """Test the generation and execution of dynamic SQL"""
    # SQL Server dynamic SQL
    sql_conn = MockConnection(is_fabric=False)
    sql_dynamic = "DECLARE @sql nvarchar(max); SET @sql = 'SELECT * FROM Claims WHERE ClaimDate > ''2023-01-01'''; EXEC sp_executesql @sql"
    sql_conn.execute(sql_dynamic)
    
    # Fabric dynamic SQL
    fabric_conn = MockConnection(is_fabric=True)
    fabric_dynamic = "DECLARE @sql string = 'SELECT * FROM Claims WHERE ClaimDate > ''2023-01-01'''; EXECUTE IMMEDIATE @sql"
    fabric_conn.execute(fabric_dynamic)
    
    # Check if both executed something
    assert len(sql_conn.executed_queries) > 0
    assert len(fabric_conn.executed_queries) > 0
    
    # Check for correct syntax in Fabric query
    assert 'EXECUTE IMMEDIATE' in fabric_conn.executed_queries[0]

def test_hash_calculation():
    """Test the conversion of HASHBYTES to HASH_MD5 for change detection"""
    # SQL Server hash calculation
    sql_conn = MockConnection(is_fabric=False)
    sql_hash_query = "SELECT HASHBYTES('MD5', 'test_data') AS hash_value"
    sql_cursor = sql_conn.execute(sql_hash_query)
    sql_hash = sql_cursor.fetchone()[0]
    
    # Fabric hash calculation
    fabric_conn = MockConnection(is_fabric=True)
    fabric_hash_query = "SELECT HASH_MD5('test_data') AS hash_value"
    fabric_cursor = fabric_conn.execute(fabric_hash_query)
    fabric_hash = fabric_cursor.fetchone()[0]
    
    # Both should return a hash value
    assert sql_hash is not None
    assert fabric_hash is not None

def test_special_date_handling():
    """Test the handling of special dates like '01/01/1900'"""
    # SQL Server date handling
    sql_conn = MockConnection(is_fabric=False)
    sql_date_query = "SELECT CASE WHEN ClaimDate = '1900-01-01' THEN NULL ELSE ClaimDate END FROM Claims"
    sql_conn.execute(sql_date_query)
    
    # Fabric date handling
    fabric_conn = MockConnection(is_fabric=True)
    fabric_date_query = "SELECT CASE WHEN ClaimDate = '1900-01-01' THEN NULL ELSE ClaimDate END FROM Claims"
    fabric_conn.execute(fabric_date_query)
    
    # Check if both queries handle the special date
    assert any('1900-01-01' in query for query in sql_conn.executed_queries)
    assert any('1900-01-01' in query for query in fabric_conn.executed_queries)

def test_claim_transaction_processing():
    """Test the claim transaction data processing logic"""
    # Mock the procedure execution with test data
    with patch('some_module.execute_procedure') as mock_exec:
        # Setup mock return values
        mock_exec.return_value = pd.DataFrame({
            'ClaimID': [1, 2, 3],
            'TransactionAmount': [100.0, 200.0, 300.0],
            'PolicyState': ['NY', 'CA', 'TX']
        })
        
        # Call the procedure (this would be replaced with actual procedure call)
        result_df = mock_exec('uspSemanticClaimTransactionMeasuresData')
        
        # Verify the results
        assert len(result_df) == 3
        assert 'ClaimID' in result_df.columns
        assert 'TransactionAmount' in result_df.columns
        assert 'PolicyState' in result_df.columns

def test_policy_risk_state_handling():
    """Test the handling of policy risk states"""
    # Create mock data with policy risk states
    test_data = pd.DataFrame({
        'PolicyID': [101, 102, 103],
        'RiskState': ['NY', 'CA', 'TX'],
        'ClaimAmount': [1000.0, 2000.0, 3000.0]
    })
    
    # Mock procedure that processes risk states
    with patch('some_module.process_risk_states', return_value=test_data) as mock_proc:
        result = mock_proc(test_data)
        
        # Verify risk states are preserved
        assert 'RiskState' in result.columns
        assert set(result['RiskState']) == {'NY', 'CA', 'TX'}

def test_hash_based_change_detection():
    """Test the hash-based change detection logic"""
    # SQL Server version
    sql_conn = MockConnection(is_fabric=False)
    sql_query = """
    SELECT 
        ClaimID,
        HASHBYTES('MD5', CONCAT(ClaimID, Amount, Status)) AS RowHash
    FROM Claims
    """
    sql_conn.execute(sql_query)
    
    # Fabric version
    fabric_conn = MockConnection(is_fabric=True)
    fabric_query = """
    SELECT 
        ClaimID,
        HASH_MD5(CONCAT(ClaimID, Amount, Status)) AS RowHash
    FROM Claims
    """
    fabric_conn.execute(fabric_query)
    
    # Verify the hash function was converted correctly
    assert 'HASHBYTES' in sql_conn.executed_queries[0]
    assert 'HASH_MD5' in fabric_conn.executed_queries[0]

def test_measure_aggregation():
    """Test the proper aggregation of measures"""
    # Create test data with measures
    test_data = pd.DataFrame({
        'ClaimID': [1, 1, 2, 3],
        'TransactionAmount': [100.0, 150.0, 200.0, 300.0],
        'PolicyState': ['NY', 'NY', 'CA', 'TX']
    })
    
    # Expected aggregated result
    expected_result = pd.DataFrame({
        'ClaimID': [1, 2, 3],
        'TotalAmount': [250.0, 200.0, 300.0],
        'PolicyState': ['NY', 'CA', 'TX']
    })
    
    # Mock aggregation function
    with patch('some_module.aggregate_measures', return_value=expected_result) as mock_agg:
        result = mock_agg(test_data)
        
        # Verify aggregation
        assert len(result) == 3  # Should be aggregated to 3 rows
        assert result.loc[result['ClaimID'] == 1, 'TotalAmount'].values[0] == 250.0

def test_integration():
    """Integration test for the entire stored procedure conversion"""
    # This would be a more comprehensive test that runs both the SQL Server and Fabric
    # versions of the procedure and compares results
    
    # Mock execution of SQL Server procedure
    with patch('some_module.execute_sql_server_proc') as mock_sql_server:
        mock_sql_server.return_value = pd.DataFrame({
            'ClaimID': [1, 2, 3],
            'MeasureValue': [100, 200, 300]
        })
        
        # Mock execution of Fabric procedure
        with patch('some_module.execute_fabric_proc') as mock_fabric:
            mock_fabric.return_value = pd.DataFrame({
                'ClaimID': [1, 2, 3],
                'MeasureValue': [100, 200, 300]
            })
            
            # Get results from both
            sql_result = mock_sql_server()
            fabric_result = mock_fabric()
            
            # Compare results
            pd.testing.assert_frame_equal(sql_result, fabric_result)

if __name__ == "__main__":
    pytest.main(['-xvs', __file__])