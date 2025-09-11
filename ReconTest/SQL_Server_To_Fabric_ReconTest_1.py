_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: SQL Server to Fabric reconciliation test for uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import pyodbc
import pandas as pd
import numpy as np
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('recon_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ConnectionConfig:
    """Configuration class for database connections"""
    sql_server: str
    sql_database: str
    fabric_endpoint: str
    fabric_database: str
    azure_storage_account: str
    azure_container: str
    key_vault_url: str
    tenant_id: str
    client_id: str
    client_secret: str

@dataclass
class ReconciliationResult:
    """Class to store reconciliation results"""
    test_name: str
    sql_server_count: int
    fabric_count: int
    matches: int
    mismatches: int
    match_percentage: float
    execution_time: float
    status: str
    error_details: Optional[str] = None
    sample_mismatches: Optional[List[Dict]] = None

class SQLServerToFabricReconTest:
    """Main class for SQL Server to Fabric reconciliation testing"""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.credential = None
        self.blob_service_client = None
        self.sql_connection = None
        self.fabric_connection = None
        self.test_results = []
        self._initialize_azure_services()
    
    def _initialize_azure_services(self):
        """Initialize Azure services and authentication"""
        try:
            # Initialize Azure credential
            if self.config.client_secret:
                self.credential = ClientSecretCredential(
                    tenant_id=self.config.tenant_id,
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret
                )
            else:
                self.credential = DefaultAzureCredential()
            
            # Initialize Blob Service Client
            storage_url = f"https://{self.config.azure_storage_account}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(
                account_url=storage_url,
                credential=self.credential
            )
            
            logger.info("Azure services initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Azure services: {str(e)}")
            raise
    
    def _get_secret_from_keyvault(self, secret_name: str) -> str:
        """Retrieve secret from Azure Key Vault"""
        try:
            secret_client = SecretClient(
                vault_url=self.config.key_vault_url,
                credential=self.credential
            )
            secret = secret_client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
            raise
    
    def _connect_sql_server(self) -> pyodbc.Connection:
        """Establish connection to SQL Server"""
        try:
            # Get connection string from Key Vault or environment
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config.sql_server};"
                f"DATABASE={self.config.sql_database};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout=30;"
                f"Command Timeout=300;"
            )
            
            connection = pyodbc.connect(connection_string)
            connection.autocommit = False
            logger.info("SQL Server connection established")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {str(e)}")
            raise
    
    def _connect_fabric(self) -> pyodbc.Connection:
        """Establish connection to Microsoft Fabric"""
        try:
            # Fabric connection using Azure AD authentication
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config.fabric_endpoint};"
                f"DATABASE={self.config.fabric_database};"
                f"Authentication=ActiveDirectoryIntegrated;"
                f"Connection Timeout=30;"
                f"Command Timeout=300;"
            )
            
            connection = pyodbc.connect(connection_string)
            connection.autocommit = False
            logger.info("Fabric connection established")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to Fabric: {str(e)}")
            raise
    
    def _execute_sql_server_procedure(self, parameters: Dict = None) -> pd.DataFrame:
        """Execute the SQL Server stored procedure"""
        try:
            if not self.sql_connection:
                self.sql_connection = self._connect_sql_server()
            
            # Execute uspSemanticClaimTransactionMeasuresData stored procedure
            cursor = self.sql_connection.cursor()
            
            if parameters:
                param_string = ', '.join([f"@{k}=?" for k in parameters.keys()])
                sql_query = f"EXEC uspSemanticClaimTransactionMeasuresData {param_string}"
                cursor.execute(sql_query, list(parameters.values()))
            else:
                cursor.execute("EXEC uspSemanticClaimTransactionMeasuresData")
            
            # Fetch results
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            
            df = pd.DataFrame.from_records(data, columns=columns)
            logger.info(f"SQL Server procedure executed successfully. Rows returned: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to execute SQL Server procedure: {str(e)}")
            raise
    
    def _upload_to_blob_storage(self, df: pd.DataFrame, blob_name: str) -> str:
        """Upload DataFrame to Azure Blob Storage as Parquet"""
        try:
            # Convert to Parquet format
            parquet_buffer = df.to_parquet(index=False, engine='pyarrow')
            
            # Upload to blob storage
            blob_client = self.blob_service_client.get_blob_client(
                container=self.config.azure_container,
                blob=blob_name
            )
            
            blob_client.upload_blob(parquet_buffer, overwrite=True)
            
            blob_url = f"https://{self.config.azure_storage_account}.blob.core.windows.net/{self.config.azure_container}/{blob_name}"
            logger.info(f"Data uploaded to blob storage: {blob_url}")
            
            return blob_url
            
        except Exception as e:
            logger.error(f"Failed to upload to blob storage: {str(e)}")
            raise
    
    def _create_fabric_external_table(self, blob_url: str, table_name: str, df_schema: pd.DataFrame) -> None:
        """Create external table in Fabric pointing to blob storage"""
        try:
            if not self.fabric_connection:
                self.fabric_connection = self._connect_fabric()
            
            cursor = self.fabric_connection.cursor()
            
            # Drop table if exists
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(drop_sql)
            
            # Generate column definitions based on DataFrame schema
            column_defs = []
            for col, dtype in df_schema.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "FLOAT"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "DATETIME2"
                else:
                    sql_type = "NVARCHAR(MAX)"
                column_defs.append(f"[{col}] {sql_type}")
            
            columns_sql = ",\n    ".join(column_defs)
            
            # Create external table
            create_sql = f"""
            CREATE TABLE {table_name} (
                {columns_sql}
            )
            WITH (
                LOCATION = '{blob_url}',
                DATA_SOURCE = ExternalDataSource,
                FILE_FORMAT = ParquetFileFormat
            )
            """
            
            cursor.execute(create_sql)
            self.fabric_connection.commit()
            
            logger.info(f"External table {table_name} created in Fabric")
            
        except Exception as e:
            logger.error(f"Failed to create Fabric external table: {str(e)}")
            raise
    
    def _execute_fabric_query(self, query: str) -> pd.DataFrame:
        """Execute query in Fabric and return results"""
        try:
            if not self.fabric_connection:
                self.fabric_connection = self._connect_fabric()
            
            df = pd.read_sql(query, self.fabric_connection)
            logger.info(f"Fabric query executed successfully. Rows returned: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to execute Fabric query: {str(e)}")
            raise
    
    def _compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                          key_columns: List[str] = None) -> ReconciliationResult:
        """Compare two DataFrames and return reconciliation results"""
        try:
            start_time = time.time()
            
            # Basic count comparison
            sql_count = len(df1)
            fabric_count = len(df2)
            
            if sql_count == 0 and fabric_count == 0:
                return ReconciliationResult(
                    test_name="Data Comparison",
                    sql_server_count=sql_count,
                    fabric_count=fabric_count,
                    matches=0,
                    mismatches=0,
                    match_percentage=100.0,
                    execution_time=time.time() - start_time,
                    status="PASS"
                )
            
            # Align columns
            common_columns = list(set(df1.columns) & set(df2.columns))
            df1_aligned = df1[common_columns].copy()
            df2_aligned = df2[common_columns].copy()
            
            # Sort both DataFrames for comparison
            if key_columns:
                available_keys = [col for col in key_columns if col in common_columns]
                if available_keys:
                    df1_aligned = df1_aligned.sort_values(available_keys).reset_index(drop=True)
                    df2_aligned = df2_aligned.sort_values(available_keys).reset_index(drop=True)
            
            # Handle data type mismatches
            for col in common_columns:
                if df1_aligned[col].dtype != df2_aligned[col].dtype:
                    try:
                        df1_aligned[col] = df1_aligned[col].astype(str)
                        df2_aligned[col] = df2_aligned[col].astype(str)
                    except:
                        pass
            
            # Compare DataFrames
            if len(df1_aligned) == len(df2_aligned):
                comparison = df1_aligned.equals(df2_aligned)
                if comparison:
                    matches = len(df1_aligned)
                    mismatches = 0
                else:
                    # Row-by-row comparison
                    row_matches = []
                    for i in range(len(df1_aligned)):
                        row_match = df1_aligned.iloc[i].equals(df2_aligned.iloc[i])
                        row_matches.append(row_match)
                    
                    matches = sum(row_matches)
                    mismatches = len(row_matches) - matches
            else:
                matches = 0
                mismatches = max(sql_count, fabric_count)
            
            match_percentage = (matches / max(sql_count, fabric_count, 1)) * 100
            status = "PASS" if match_percentage >= 99.9 else "FAIL"
            
            # Sample mismatches for reporting
            sample_mismatches = []
            if mismatches > 0 and len(df1_aligned) == len(df2_aligned):
                for i in range(min(5, len(df1_aligned))):
                    if not df1_aligned.iloc[i].equals(df2_aligned.iloc[i]):
                        mismatch_detail = {
                            "row_index": i,
                            "sql_server_data": df1_aligned.iloc[i].to_dict(),
                            "fabric_data": df2_aligned.iloc[i].to_dict()
                        }
                        sample_mismatches.append(mismatch_detail)
            
            return ReconciliationResult(
                test_name="Data Comparison",
                sql_server_count=sql_count,
                fabric_count=fabric_count,
                matches=matches,
                mismatches=mismatches,
                match_percentage=match_percentage,
                execution_time=time.time() - start_time,
                status=status,
                sample_mismatches=sample_mismatches[:5]
            )
            
        except Exception as e:
            logger.error(f"Failed to compare DataFrames: {str(e)}")
            return ReconciliationResult(
                test_name="Data Comparison",
                sql_server_count=0,
                fabric_count=0,
                matches=0,
                mismatches=0,
                match_percentage=0.0,
                execution_time=time.time() - start_time,
                status="ERROR",
                error_details=str(e)
            )
    
    def _generate_report(self) -> str:
        """Generate comprehensive reconciliation report"""
        report = []
        report.append("=" * 80)
        report.append("SQL SERVER TO FABRIC RECONCILIATION REPORT")
        report.append("=" * 80)
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Stored Procedure: uspSemanticClaimTransactionMeasuresData")
        report.append("")
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result.status == "PASS")
        failed_tests = sum(1 for result in self.test_results if result.status == "FAIL")
        error_tests = sum(1 for result in self.test_results if result.status == "ERROR")
        
        report.append("SUMMARY:")
        report.append("-" * 40)
        report.append(f"Total Tests: {total_tests}")
        report.append(f"Passed: {passed_tests}")
        report.append(f"Failed: {failed_tests}")
        report.append(f"Errors: {error_tests}")
        report.append(f"Success Rate: {(passed_tests/max(total_tests,1)*100):.2f}%")
        report.append("")
        
        report.append("DETAILED RESULTS:")
        report.append("-" * 40)
        
        for result in self.test_results:
            report.append(f"Test: {result.test_name}")
            report.append(f"  Status: {result.status}")
            report.append(f"  SQL Server Count: {result.sql_server_count:,}")
            report.append(f"  Fabric Count: {result.fabric_count:,}")
            report.append(f"  Matches: {result.matches:,}")
            report.append(f"  Mismatches: {result.mismatches:,}")
            report.append(f"  Match Percentage: {result.match_percentage:.2f}%")
            report.append(f"  Execution Time: {result.execution_time:.2f}s")
            
            if result.error_details:
                report.append(f"  Error: {result.error_details}")
            
            if result.sample_mismatches:
                report.append("  Sample Mismatches:")
                for i, mismatch in enumerate(result.sample_mismatches[:3]):
                    report.append(f"    Row {mismatch['row_index']}:")
                    report.append(f"      SQL Server: {mismatch['sql_server_data']}")
                    report.append(f"      Fabric: {mismatch['fabric_data']}")
            
            report.append("")
        
        return "\n".join(report)
    
    def run_reconciliation_test(self, test_parameters: Dict = None, 
                              key_columns: List[str] = None) -> ReconciliationResult:
        """Run the complete reconciliation test"""
        try:
            logger.info("Starting SQL Server to Fabric reconciliation test")
            
            # Step 1: Execute SQL Server stored procedure
            logger.info("Step 1: Executing SQL Server stored procedure")
            sql_df = self._execute_sql_server_procedure(test_parameters)
            
            # Step 2: Upload data to blob storage
            logger.info("Step 2: Uploading data to Azure Blob Storage")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            blob_name = f"recon_test_data_{timestamp}.parquet"
            blob_url = self._upload_to_blob_storage(sql_df, blob_name)
            
            # Step 3: Create external table in Fabric
            logger.info("Step 3: Creating external table in Fabric")
            table_name = f"ext_recon_test_{timestamp}"
            self._create_fabric_external_table(blob_url, table_name, sql_df)
            
            # Step 4: Query data from Fabric
            logger.info("Step 4: Querying data from Fabric")
            fabric_query = f"SELECT * FROM {table_name}"
            fabric_df = self._execute_fabric_query(fabric_query)
            
            # Step 5: Compare results
            logger.info("Step 5: Comparing results")
            result = self._compare_dataframes(sql_df, fabric_df, key_columns)
            
            # Step 6: Cleanup
            logger.info("Step 6: Cleaning up temporary resources")
            try:
                # Drop temporary table
                cleanup_cursor = self.fabric_connection.cursor()
                cleanup_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.fabric_connection.commit()
                
                # Delete blob
                blob_client = self.blob_service_client.get_blob_client(
                    container=self.config.azure_container,
                    blob=blob_name
                )
                blob_client.delete_blob()
                
            except Exception as cleanup_error:
                logger.warning(f"Cleanup warning: {str(cleanup_error)}")
            
            self.test_results.append(result)
            logger.info(f"Reconciliation test completed with status: {result.status}")
            
            return result
            
        except Exception as e:
            logger.error(f"Reconciliation test failed: {str(e)}")
            error_result = ReconciliationResult(
                test_name="uspSemanticClaimTransactionMeasuresData",
                sql_server_count=0,
                fabric_count=0,
                matches=0,
                mismatches=0,
                match_percentage=0.0,
                execution_time=0.0,
                status="ERROR",
                error_details=str(e)
            )
            self.test_results.append(error_result)
            return error_result
    
    def run_multiple_test_scenarios(self) -> List[ReconciliationResult]:
        """Run multiple test scenarios for comprehensive validation"""
        test_scenarios = [
            {
                "name": "Full Dataset Test",
                "parameters": None,
                "key_columns": ["ClaimId", "TransactionId"]
            },
            {
                "name": "Date Range Test",
                "parameters": {
                    "StartDate": (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                    "EndDate": datetime.now().strftime('%Y-%m-%d')
                },
                "key_columns": ["ClaimId", "TransactionId"]
            },
            {
                "name": "Specific Claim Type Test",
                "parameters": {
                    "ClaimType": "MEDICAL"
                },
                "key_columns": ["ClaimId", "TransactionId"]
            }
        ]
        
        results = []
        for scenario in test_scenarios:
            logger.info(f"Running test scenario: {scenario['name']}")
            result = self.run_reconciliation_test(
                test_parameters=scenario['parameters'],
                key_columns=scenario['key_columns']
            )
            result.test_name = scenario['name']
            results.append(result)
        
        return results
    
    def close_connections(self):
        """Close all database connections"""
        try:
            if self.sql_connection:
                self.sql_connection.close()
                logger.info("SQL Server connection closed")
            
            if self.fabric_connection:
                self.fabric_connection.close()
                logger.info("Fabric connection closed")
                
        except Exception as e:
            logger.warning(f"Error closing connections: {str(e)}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections()

def main():
    """Main execution function"""
    try:
        # Configuration - should be loaded from secure configuration
        config = ConnectionConfig(
            sql_server=os.getenv("SQL_SERVER", "your-sql-server.database.windows.net"),
            sql_database=os.getenv("SQL_DATABASE", "YourDatabase"),
            fabric_endpoint=os.getenv("FABRIC_ENDPOINT", "your-fabric-endpoint.datawarehouse.fabric.microsoft.com"),
            fabric_database=os.getenv("FABRIC_DATABASE", "YourFabricDatabase"),
            azure_storage_account=os.getenv("AZURE_STORAGE_ACCOUNT", "yourstorageaccount"),
            azure_container=os.getenv("AZURE_CONTAINER", "reconciliation-data"),
            key_vault_url=os.getenv("KEY_VAULT_URL", "https://your-keyvault.vault.azure.net/"),
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )
        
        # Run reconciliation test
        with SQLServerToFabricReconTest(config) as recon_test:
            logger.info("Starting comprehensive reconciliation testing")
            
            # Run multiple test scenarios
            results = recon_test.run_multiple_test_scenarios()
            
            # Generate and save report
            report = recon_test._generate_report()
            
            # Save report to file
            report_filename = f"reconciliation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_filename, 'w') as f:
                f.write(report)
            
            logger.info(f"Reconciliation report saved to: {report_filename}")
            print(report)
            
            # Return overall success status
            overall_success = all(result.status == "PASS" for result in results)
            return 0 if overall_success else 1
            
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)