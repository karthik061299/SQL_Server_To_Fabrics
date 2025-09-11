    @pytest.fixture(scope="class")
    def mock_sql_server_conn(self, sql_server_config):
        """Create a mock connection to SQL Server for unit testing"""
        mock_conn = MagicMock()
        return mock_conn
    
    @pytest.fixture(scope="class")
    def mock_fabric_conn(self, fabric_config):
        """Create a mock connection to Fabric SQL for unit testing"""
        mock_conn = MagicMock()
        return mock_conn
    
    @pytest.fixture(scope="function")
    def setup_test_data(self):
        """Set up test data for the stored procedure"""
        # Sample test data for different scenarios
        return {
            "claim_transaction_measures": pd.DataFrame({
                "FactClaimTransactionLineWCKey": range(1, 11),
                "RevisionNumber": [1] * 10,
                "HashValue": [f"hash{i}" for i in range(1, 11)],
                "LoadCreateDate": [datetime.now() - timedelta(days=i) for i in range(10)]
            }),
            "policy_risk_state": pd.DataFrame({
                "PolicyRiskStateWCKey": range(101, 111),
                "PolicyWCKey": range(201, 211),
                "RiskState": ["CA", "NY", "TX", "FL", "WA", "OR", "AZ", "NV", "ID", "MT"],
                "RetiredInd": [0] * 10,
                "RiskStateEffectiveDate": [datetime.now() - timedelta(days=100) for _ in range(10)],
                "RecordEffectiveDate": [datetime.now() - timedelta(days=90) for _ in range(10)],
                "LoadUpdateDate": [datetime.now() - timedelta(days=80) for _ in range(10)]
            }),
            "expected_results": pd.DataFrame({
                "FactClaimTransactionLineWCKey": range(1, 11),
                "RevisionNumber": [1] * 10,
                "PolicyWCKey": range(201, 211),
                "HashValue": [f"newhash{i}" for i in range(1, 11)],
                "InsertUpdates": [0, 0, 1, 0, 1, 0, 1, 0, 1, 0],  # Mix of inserts and updates
                "AuditOperations": ["Updated", "Updated", "Inserted", "Updated", "Inserted", 
                                  "Updated", "Inserted", "Updated", "Inserted", "Updated"]
            })
        }