_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server to Fabric migration validation for uspSemanticClaimTransactionMeasuresData
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import os
import sys
import pandas as pd
import pyodbc
import logging
import datetime
import hashlib
import numpy as np
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from typing import Dict, List, Tuple, Any

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/uspSemanticClaimTransactionMeasuresData_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SQLServerToFabricReconTest:
    """Class to validate migration from SQL Server to Fabric for uspSemanticClaimTransactionMeasuresData"""
    
    def __init__(self):
        """Initialize connection parameters and configurations"""
        # SQL Server connection parameters
        self.sql_server = os.getenv('SQL_SERVER')
        self.sql_database = os.getenv('SQL_DATABASE')
        self.sql_username = os.getenv('SQL_USERNAME')
        self.sql_password = os.getenv('SQL_PASSWORD')
        
        # Fabric connection parameters
        self.fabric_server = os.getenv('FABRIC_SERVER')
        self.fabric_database = os.getenv('FABRIC_DATABASE')
        
        # Azure Blob Storage parameters
        self.storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME')
        self.container_name = os.getenv('CONTAINER_NAME')
        
        # Test parameters
        self.proc_name = 'uspSemanticClaimTransactionMeasuresData'
        self.test_params = {
            '@EffectiveDate': '2023-01-01',
            '@ClaimID': 12345
        }
        
        # Comparison thresholds
        self.row_count_threshold = 0.01  # 1% difference allowed
        self.value_match_threshold = 0.001  # 0.1% difference allowed for numeric values
        
        # Results storage
        self.results = {
            'test_name': self.proc_name,
            'timestamp': datetime.datetime.now().isoformat(),
            'status': 'Not Started',
            'sql_server_row_count': 0,
            'fabric_row_count': 0,
            'row_count_match': False,
            'column_matches': {},
            'mismatched_rows': [],
            'execution_time_sql': 0,
            'execution_time_fabric': 0,
            'error': None
        }
    
    def connect_to_sql_server(self):
        """Establish connection to SQL Server"""
        try:
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.sql_server};DATABASE={self.sql_database};UID={self.sql_username};PWD={self.sql_password}'
            return pyodbc.connect(conn_str)
        except Exception as e:
            logger.error(f"Error connecting to SQL Server: {e}")
            self.results['error'] = f"SQL Server connection error: {str(e)}"
            raise
    
    def connect_to_fabric(self):
        """Establish connection to Fabric"""
        try:
            credential = DefaultAzureCredential()
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.fabric_server};DATABASE={self.fabric_database};Authentication=ActiveDirectoryDefault'
            return pyodbc.connect(conn_str)
        except Exception as e:
            logger.error(f"Error connecting to Fabric: {e}")
            self.results['error'] = f"Fabric connection error: {str(e)}"
            raise
    
    def execute_sql_server_proc(self):
        """Execute stored procedure on SQL Server and return results"""
        logger.info(f"Executing {self.proc_name} on SQL Server")
        start_time = datetime.datetime.now()
        
        try:
            conn = self.connect_to_sql_server()
            cursor = conn.cursor()
            
            # Prepare parameter string
            param_str = ', '.join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in self.test_params.items()])
            
            # Execute procedure
            sql = f"EXEC {self.proc_name} {param_str}"
            logger.debug(f"SQL Query: {sql}")
            
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description]
            data = []
            
            for row in cursor.fetchall():
                data.append(dict(zip(columns, row)))
            
            end_time = datetime.datetime.now()
            self.results['execution_time_sql'] = (end_time - start_time).total_seconds()
            
            logger.info(f"SQL Server execution completed in {self.results['execution_time_sql']} seconds")
            self.results['sql_server_row_count'] = len(data)
            
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error executing SQL Server procedure: {e}")
            self.results['error'] = f"SQL Server execution error: {str(e)}"
            raise
        finally:
            if 'conn' in locals() and conn:
                conn.close()
    
    def upload_to_blob_storage(self, df: pd.DataFrame, file_name: str) -> str:
        """Upload dataframe to Azure Blob Storage and return URL"""
        try:
            # Create a connection to the blob storage
            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(
                account_url=f"https://{self.storage_account_name}.blob.core.windows.net",
                credential=credential
            )
            container_client = blob_service_client.get_container_client(self.container_name)
            
            # Convert dataframe to CSV
            csv_data = df.to_csv(index=False)
            
            # Upload to blob storage
            blob_client = container_client.get_blob_client(file_name)
            blob_client.upload_blob(csv_data, overwrite=True)
            
            logger.info(f"Data uploaded to blob storage: {file_name}")
            return f"https://{self.storage_account_name}.blob.core.windows.net/{self.container_name}/{file_name}"
        except Exception as e:
            logger.error(f"Error uploading to blob storage: {e}")
            self.results['error'] = f"Blob storage upload error: {str(e)}"
            raise
    
    def execute_fabric_query(self, blob_url: str):
        """Execute query on Fabric using external table"""
        logger.info(f"Executing {self.proc_name} on Fabric")
        start_time = datetime.datetime.now()
        
        try:
            conn = self.connect_to_fabric()
            cursor = conn.cursor()
            
            # Create external data source if it doesn't exist
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'AzureBlobStorage')
            BEGIN
                CREATE EXTERNAL DATA SOURCE AzureBlobStorage
                WITH (
                    LOCATION = 'abfss://container@account.dfs.core.windows.net'
                );
            END
            """)
            
            # Create external file format if it doesn't exist
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'CSVFormat')
            BEGIN
                CREATE EXTERNAL FILE FORMAT CSVFormat
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2)
                );
            END
            """)
            
            # Create temporary external table
            external_table_name = f"#ext_{self.proc_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
            cursor.execute(f"""
            CREATE EXTERNAL TABLE {external_table_name}
            WITH (
                LOCATION = '{blob_url}',
                DATA_SOURCE = AzureBlobStorage,
                FILE_FORMAT = CSVFormat
            )
            AS
            EXEC {self.proc_name} {', '.join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in self.test_params.items()])}
            """)
            
            # Query the external table
            cursor.execute(f"SELECT * FROM {external_table_name}")
            
            columns = [column[0] for column in cursor.description]
            data = []
            
            for row in cursor.fetchall():
                data.append(dict(zip(columns, row)))
            
            end_time = datetime.datetime.now()
            self.results['execution_time_fabric'] = (end_time - start_time).total_seconds()
            
            logger.info(f"Fabric execution completed in {self.results['execution_time_fabric']} seconds")
            self.results['fabric_row_count'] = len(data)
            
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error executing Fabric query: {e}")
            self.results['error'] = f"Fabric execution error: {str(e)}"
            raise
        finally:
            if 'conn' in locals() and conn:
                conn.close()
    
    def compare_dataframes(self, sql_df: pd.DataFrame, fabric_df: pd.DataFrame):
        """Compare SQL Server and Fabric dataframes"""
        logger.info("Comparing SQL Server and Fabric results")
        
        try:
            # Check row counts
            sql_count = len(sql_df)
            fabric_count = len(fabric_df)
            
            count_diff_pct = abs(sql_count - fabric_count) / max(sql_count, 1) if sql_count > 0 else 0
            self.results['row_count_match'] = count_diff_pct <= self.row_count_threshold
            
            logger.info(f"Row count comparison: SQL={sql_count}, Fabric={fabric_count}, Match={self.results['row_count_match']}")
            
            # Check column presence
            sql_columns = set(sql_df.columns)
            fabric_columns = set(fabric_df.columns)
            
            missing_in_fabric = sql_columns - fabric_columns
            missing_in_sql = fabric_columns - sql_columns
            
            if missing_in_fabric:
                logger.warning(f"Columns missing in Fabric: {missing_in_fabric}")
            
            if missing_in_sql:
                logger.warning(f"Columns missing in SQL Server: {missing_in_sql}")
            
            # Compare common columns
            common_columns = sql_columns.intersection(fabric_columns)
            
            # Sort both dataframes by common columns to align rows
            # This is a simplification - in real scenarios, you'd need a proper key to match rows
            try:
                sql_df = sql_df.sort_values(by=list(common_columns)).reset_index(drop=True)
                fabric_df = fabric_df.sort_values(by=list(common_columns)).reset_index(drop=True)
            except:
                logger.warning("Could not sort dataframes by common columns")
            
            # Compare column values
            for column in common_columns:
                sql_values = sql_df[column].fillna('NULL')
                fabric_values = fabric_df[column].fillna('NULL')
                
                # For numeric columns, allow small differences
                if pd.api.types.is_numeric_dtype(sql_values) and pd.api.types.is_numeric_dtype(fabric_values):
                    # Calculate absolute differences for non-null values
                    sql_numeric = sql_df[column].dropna()
                    fabric_numeric = fabric_df[column].dropna()
                    
                    # If lengths don't match, can't directly compare
                    if len(sql_numeric) == len(fabric_numeric):
                        abs_diff = np.abs(sql_numeric.values - fabric_numeric.values)
                        max_val = np.maximum(np.abs(sql_numeric.values), np.abs(fabric_numeric.values))
                        rel_diff = np.divide(abs_diff, max_val, out=np.zeros_like(abs_diff), where=max_val!=0)
                        match_pct = np.mean(rel_diff <= self.value_match_threshold)
                    else:
                        match_pct = 0
                else:
                    # For non-numeric columns, exact match required
                    match_pct = (sql_values == fabric_values).mean()
                
                self.results['column_matches'][column] = match_pct
                
                if match_pct < 0.99:  # Less than 99% match
                    logger.warning(f"Column {column} has {match_pct*100:.2f}% match rate")
                    
                    # Record some examples of mismatches
                    if len(sql_df) == len(fabric_df):
                        mismatch_indices = np.where(sql_values != fabric_values)[0][:5]  # First 5 mismatches
                        for idx in mismatch_indices:
                            self.results['mismatched_rows'].append({
                                'column': column,
                                'row_index': int(idx),
                                'sql_value': str(sql_values.iloc[idx]),
                                'fabric_value': str(fabric_values.iloc[idx])
                            })
            
            # Set overall status
            all_columns_match = all(match >= 0.99 for match in self.results['column_matches'].values())
            
            if self.results['row_count_match'] and all_columns_match:
                self.results['status'] = 'Success'
            else:
                self.results['status'] = 'Failure'
                
            logger.info(f"Comparison completed with status: {self.results['status']}")
            
        except Exception as e:
            logger.error(f"Error comparing dataframes: {e}")
            self.results['error'] = f"Comparison error: {str(e)}"
            self.results['status'] = 'Error'
    
    def generate_report(self):
        """Generate a detailed report of the test results"""
        report = {
            'test_name': self.results['test_name'],
            'timestamp': self.results['timestamp'],
            'status': self.results['status'],
            'row_counts': {
                'sql_server': self.results['sql_server_row_count'],
                'fabric': self.results['fabric_row_count'],
                'match': self.results['row_count_match']
            },
            'execution_times': {
                'sql_server_seconds': self.results['execution_time_sql'],
                'fabric_seconds': self.results['execution_time_fabric']
            },
            'column_match_percentages': self.results['column_matches'],
            'mismatch_examples': self.results['mismatched_rows'][:10],  # Limit to 10 examples
            'error': self.results['error']
        }
        
        # Calculate overall match percentage
        if self.results['column_matches']:
            report['overall_match_percentage'] = sum(self.results['column_matches'].values()) / len(self.results['column_matches']) * 100
        else:
            report['overall_match_percentage'] = 0
        
        return report
    
    def run_test(self):
        """Run the complete test workflow"""
        try:
            logger.info(f"Starting migration validation test for {self.proc_name}")
            
            # Execute SQL Server procedure and get results
            sql_df = self.execute_sql_server_proc()
            
            # Upload results to blob storage
            timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            blob_file_name = f"{self.proc_name}_{timestamp}.csv"
            blob_url = self.upload_to_blob_storage(sql_df, blob_file_name)
            
            # Execute Fabric query using the blob data
            fabric_df = self.execute_fabric_query(blob_url)
            
            # Compare results
            self.compare_dataframes(sql_df, fabric_df)
            
            # Generate report
            report = self.generate_report()
            
            logger.info(f"Test completed with status: {report['status']}")
            return report
            
        except Exception as e:
            logger.error(f"Test failed with error: {e}")
            self.results['status'] = 'Error'
            self.results['error'] = str(e)
            return self.generate_report()

# Main execution
if __name__ == "__main__":
    try:
        test = SQLServerToFabricReconTest()
        results = test.run_test()
        
        # Output results
        print(f"\nTest Results for {results['test_name']}:")
        print(f"Status: {results['status']}")
        print(f"Row Count Match: {results['row_counts']['match']} (SQL: {results['row_counts']['sql_server']}, Fabric: {results['row_counts']['fabric']})")
        print(f"Overall Match Percentage: {results['overall_match_percentage']:.2f}%")
        print(f"Execution Time - SQL Server: {results['execution_times']['sql_server_seconds']:.2f}s, Fabric: {results['execution_times']['fabric_seconds']:.2f}s")
        
        if results['error']:
            print(f"Error: {results['error']}")
            sys.exit(1)
        
        # Exit with appropriate code
        if results['status'] == 'Success':
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        print(f"Critical error: {e}")
        sys.exit(1)