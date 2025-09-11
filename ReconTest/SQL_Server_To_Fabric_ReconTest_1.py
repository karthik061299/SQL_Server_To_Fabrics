_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server to Fabric migration validation script for uspSemanticClaimTransactionMeasuresData stored procedure
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import pandas as pd
import pyodbc
import sqlalchemy
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

class SQLServerToFabricReconTest:
    """
    Comprehensive validation class for SQL Server to Fabric migration
    for uspSemanticClaimTransactionMeasuresData stored procedure
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the ReconTest with configuration parameters
        
        Args:
            config (Dict): Configuration dictionary containing connection strings and parameters
        """
        self.config = config
        self.setup_logging()
        self.sql_server_conn = None
        self.fabric_conn = None
        self.blob_client = None
        self.results = {
            'sql_server_data': None,
            'fabric_data': None,
            'comparison_results': {},
            'validation_summary': {},
            'errors': []
        }
    
    def setup_logging(self):
        """
        Setup logging configuration for the validation process
        """
        log_level = self.config.get('log_level', 'INFO')
        log_file = self.config.get('log_file', 'recon_test.log')
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("SQLServerToFabricReconTest initialized")
    
    def create_sql_server_connection(self):
        """
        Create connection to SQL Server database
        
        Returns:
            sqlalchemy.engine.Engine: SQL Server connection engine
        """
        try:
            server = self.config['sql_server']['server']
            database = self.config['sql_server']['database']
            username = self.config['sql_server'].get('username')
            password = self.config['sql_server'].get('password')
            
            if username and password:
                connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            else:
                # Use Windows Authentication
                connection_string = f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            
            engine = sqlalchemy.create_engine(connection_string)
            self.sql_server_conn = engine
            self.logger.info("SQL Server connection established successfully")
            return engine
            
        except Exception as e:
            self.logger.error(f"Failed to create SQL Server connection: {str(e)}")
            self.results['errors'].append(f"SQL Server connection error: {str(e)}")
            raise
    
    def create_fabric_connection(self):
        """
        Create connection to Microsoft Fabric
        
        Returns:
            sqlalchemy.engine.Engine: Fabric connection engine
        """
        try:
            server = self.config['fabric']['server']
            database = self.config['fabric']['database']
            
            # Use Azure AD authentication for Fabric
            connection_string = f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&Authentication=ActiveDirectoryIntegrated"
            
            engine = sqlalchemy.create_engine(connection_string)
            self.fabric_conn = engine
            self.logger.info("Fabric connection established successfully")
            return engine
            
        except Exception as e:
            self.logger.error(f"Failed to create Fabric connection: {str(e)}")
            self.results['errors'].append(f"Fabric connection error: {str(e)}")
            raise
    
    def execute_sql_server_procedure(self, procedure_params: Dict) -> pd.DataFrame:
        """
        Execute the uspSemanticClaimTransactionMeasuresData stored procedure on SQL Server
        
        Args:
            procedure_params (Dict): Parameters for the stored procedure
            
        Returns:
            pd.DataFrame: Results from SQL Server stored procedure
        """
        try:
            if not self.sql_server_conn:
                self.create_sql_server_connection()
            
            # Build the stored procedure call
            proc_name = "uspSemanticClaimTransactionMeasuresData"
            
            # Extract parameters
            start_date = procedure_params.get('StartDate', '2023-01-01')
            end_date = procedure_params.get('EndDate', '2023-12-31')
            client_id = procedure_params.get('ClientId', None)
            
            # Build parameter string
            param_string = f"@StartDate='{start_date}', @EndDate='{end_date}'"
            if client_id:
                param_string += f", @ClientId={client_id}"
            
            query = f"EXEC {proc_name} {param_string}"
            
            self.logger.info(f"Executing SQL Server procedure: {query}")
            
            # Execute the stored procedure
            df = pd.read_sql(query, self.sql_server_conn)
            
            self.results['sql_server_data'] = df
            self.logger.info(f"SQL Server procedure executed successfully. Rows returned: {len(df)}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to execute SQL Server procedure: {str(e)}")
            self.results['errors'].append(f"SQL Server procedure execution error: {str(e)}")
            raise
    
    def transform_data_for_fabric(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform SQL Server data for Fabric compatibility
        
        Args:
            df (pd.DataFrame): Original DataFrame from SQL Server
            
        Returns:
            pd.DataFrame: Transformed DataFrame for Fabric
        """
        try:
            transformed_df = df.copy()
            
            # Handle data type conversions
            for col in transformed_df.columns:
                if transformed_df[col].dtype == 'object':
                    # Handle string columns
                    transformed_df[col] = transformed_df[col].astype(str)
                elif 'datetime' in str(transformed_df[col].dtype):
                    # Ensure datetime format compatibility
                    transformed_df[col] = pd.to_datetime(transformed_df[col])
                elif 'float' in str(transformed_df[col].dtype):
                    # Handle float precision
                    transformed_df[col] = transformed_df[col].round(6)
            
            # Handle null values
            transformed_df = transformed_df.fillna('')
            
            self.logger.info("Data transformation for Fabric completed")
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Data transformation failed: {str(e)}")
            self.results['errors'].append(f"Data transformation error: {str(e)}")
            raise
    
    def upload_to_blob_storage(self, df: pd.DataFrame, blob_name: str) -> str:
        """
        Upload DataFrame to Azure Blob Storage
        
        Args:
            df (pd.DataFrame): DataFrame to upload
            blob_name (str): Name of the blob
            
        Returns:
            str: Blob URL
        """
        try:
            # Initialize blob client if not exists
            if not self.blob_client:
                account_name = self.config['azure_storage']['account_name']
                container_name = self.config['azure_storage']['container_name']
                
                credential = DefaultAzureCredential()
                self.blob_client = BlobServiceClient(
                    account_url=f"https://{account_name}.blob.core.windows.net",
                    credential=credential
                )
            
            # Convert DataFrame to CSV
            csv_data = df.to_csv(index=False)
            
            # Upload to blob
            blob_client = self.blob_client.get_blob_client(
                container=self.config['azure_storage']['container_name'],
                blob=blob_name
            )
            
            blob_client.upload_blob(csv_data, overwrite=True)
            
            blob_url = blob_client.url
            self.logger.info(f"Data uploaded to blob storage: {blob_url}")
            
            return blob_url
            
        except Exception as e:
            self.logger.error(f"Blob upload failed: {str(e)}")
            self.results['errors'].append(f"Blob upload error: {str(e)}")
            raise
    
    def create_fabric_external_table(self, blob_url: str, table_name: str, df: pd.DataFrame):
        """
        Create external table in Fabric pointing to blob storage
        
        Args:
            blob_url (str): URL of the blob containing data
            table_name (str): Name of the external table
            df (pd.DataFrame): DataFrame to infer schema from
        """
        try:
            if not self.fabric_conn:
                self.create_fabric_connection()
            
            # Generate CREATE EXTERNAL TABLE statement
            columns_def = []
            for col in df.columns:
                dtype = df[col].dtype
                if 'int' in str(dtype):
                    sql_type = 'INT'
                elif 'float' in str(dtype):
                    sql_type = 'FLOAT'
                elif 'datetime' in str(dtype):
                    sql_type = 'DATETIME2'
                else:
                    sql_type = 'NVARCHAR(MAX)'
                
                columns_def.append(f"[{col}] {sql_type}")
            
            columns_str = ',\n    '.join(columns_def)
            
            # Create external table SQL
            create_table_sql = f"""
            IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                DROP TABLE {table_name};
            
            CREATE TABLE {table_name} (
                {columns_str}
            );
            """
            
            with self.fabric_conn.connect() as conn:
                conn.execute(sqlalchemy.text(create_table_sql))
                conn.commit()
            
            self.logger.info(f"External table {table_name} created successfully in Fabric")
            
        except Exception as e:
            self.logger.error(f"Failed to create external table: {str(e)}")
            self.results['errors'].append(f"External table creation error: {str(e)}")
            raise
    
    def execute_fabric_equivalent_query(self, table_name: str) -> pd.DataFrame:
        """
        Execute equivalent query in Fabric to get the same data
        
        Args:
            table_name (str): Name of the table/view in Fabric
            
        Returns:
            pd.DataFrame: Results from Fabric query
        """
        try:
            if not self.fabric_conn:
                self.create_fabric_connection()
            
            # Simple SELECT query for comparison
            query = f"SELECT * FROM {table_name} ORDER BY 1"
            
            self.logger.info(f"Executing Fabric query: {query}")
            
            df = pd.read_sql(query, self.fabric_conn)
            
            self.results['fabric_data'] = df
            self.logger.info(f"Fabric query executed successfully. Rows returned: {len(df)}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to execute Fabric query: {str(e)}")
            self.results['errors'].append(f"Fabric query execution error: {str(e)}")
            raise
    
    def compare_datasets(self, sql_server_df: pd.DataFrame, fabric_df: pd.DataFrame) -> Dict:
        """
        Compare datasets from SQL Server and Fabric
        
        Args:
            sql_server_df (pd.DataFrame): SQL Server results
            fabric_df (pd.DataFrame): Fabric results
            
        Returns:
            Dict: Comparison results
        """
        try:
            comparison_results = {
                'row_count_match': False,
                'column_count_match': False,
                'schema_match': False,
                'data_match': False,
                'differences': [],
                'statistics': {}
            }
            
            # Row count comparison
            sql_rows = len(sql_server_df)
            fabric_rows = len(fabric_df)
            comparison_results['row_count_match'] = sql_rows == fabric_rows
            comparison_results['statistics']['sql_server_rows'] = sql_rows
            comparison_results['statistics']['fabric_rows'] = fabric_rows
            
            if not comparison_results['row_count_match']:
                comparison_results['differences'].append(
                    f"Row count mismatch: SQL Server={sql_rows}, Fabric={fabric_rows}"
                )
            
            # Column count comparison
            sql_cols = len(sql_server_df.columns)
            fabric_cols = len(fabric_df.columns)
            comparison_results['column_count_match'] = sql_cols == fabric_cols
            comparison_results['statistics']['sql_server_columns'] = sql_cols
            comparison_results['statistics']['fabric_columns'] = fabric_cols
            
            if not comparison_results['column_count_match']:
                comparison_results['differences'].append(
                    f"Column count mismatch: SQL Server={sql_cols}, Fabric={fabric_cols}"
                )
            
            # Schema comparison
            sql_schema = set(sql_server_df.columns)
            fabric_schema = set(fabric_df.columns)
            comparison_results['schema_match'] = sql_schema == fabric_schema
            
            if not comparison_results['schema_match']:
                missing_in_fabric = sql_schema - fabric_schema
                missing_in_sql = fabric_schema - sql_schema
                
                if missing_in_fabric:
                    comparison_results['differences'].append(
                        f"Columns missing in Fabric: {missing_in_fabric}"
                    )
                if missing_in_sql:
                    comparison_results['differences'].append(
                        f"Columns missing in SQL Server: {missing_in_sql}"
                    )
            
            # Data comparison (if schemas match and both have data)
            if (comparison_results['schema_match'] and 
                comparison_results['row_count_match'] and 
                sql_rows > 0):
                
                try:
                    # Sort both dataframes for comparison
                    sql_sorted = sql_server_df.sort_values(by=sql_server_df.columns.tolist()).reset_index(drop=True)
                    fabric_sorted = fabric_df.sort_values(by=fabric_df.columns.tolist()).reset_index(drop=True)
                    
                    # Compare data
                    data_equal = sql_sorted.equals(fabric_sorted)
                    comparison_results['data_match'] = data_equal
                    
                    if not data_equal:
                        # Find specific differences
                        for col in sql_sorted.columns:
                            if not sql_sorted[col].equals(fabric_sorted[col]):
                                diff_count = (sql_sorted[col] != fabric_sorted[col]).sum()
                                comparison_results['differences'].append(
                                    f"Column '{col}' has {diff_count} differing values"
                                )
                    
                except Exception as e:
                    comparison_results['differences'].append(
                        f"Data comparison failed: {str(e)}"
                    )
            
            # Calculate overall match percentage
            total_checks = 4  # row_count, column_count, schema, data
            passed_checks = sum([
                comparison_results['row_count_match'],
                comparison_results['column_count_match'],
                comparison_results['schema_match'],
                comparison_results['data_match']
            ])
            
            comparison_results['match_percentage'] = (passed_checks / total_checks) * 100
            
            self.results['comparison_results'] = comparison_results
            self.logger.info(f"Dataset comparison completed. Match percentage: {comparison_results['match_percentage']:.2f}%")
            
            return comparison_results
            
        except Exception as e:
            self.logger.error(f"Dataset comparison failed: {str(e)}")
            self.results['errors'].append(f"Dataset comparison error: {str(e)}")
            raise
    
    def generate_validation_report(self) -> Dict:
        """
        Generate comprehensive validation report
        
        Returns:
            Dict: Validation report
        """
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'procedure_name': 'uspSemanticClaimTransactionMeasuresData',
                'validation_status': 'PASSED' if self.results['comparison_results'].get('match_percentage', 0) == 100 else 'FAILED',
                'summary': self.results['comparison_results'],
                'errors': self.results['errors'],
                'recommendations': []
            }
            
            # Add recommendations based on results
            if not self.results['comparison_results'].get('row_count_match', False):
                report['recommendations'].append(
                    "Investigate row count differences - check for data filtering or transformation issues"
                )
            
            if not self.results['comparison_results'].get('schema_match', False):
                report['recommendations'].append(
                    "Review schema differences - ensure all columns are properly mapped"
                )
            
            if not self.results['comparison_results'].get('data_match', False):
                report['recommendations'].append(
                    "Analyze data differences - check for data type conversions and precision issues"
                )
            
            if len(self.results['errors']) > 0:
                report['recommendations'].append(
                    "Review and resolve all errors before proceeding with migration"
                )
            
            self.results['validation_summary'] = report
            self.logger.info(f"Validation report generated. Status: {report['validation_status']}")
            
            return report
            
        except Exception as e:
            self.logger.error(f"Failed to generate validation report: {str(e)}")
            raise
    
    def save_results_to_file(self, output_file: str = None):
        """
        Save validation results to JSON file
        
        Args:
            output_file (str): Output file path
        """
        try:
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"recon_test_results_{timestamp}.json"
            
            # Prepare results for JSON serialization
            json_results = {
                'validation_summary': self.results['validation_summary'],
                'comparison_results': self.results['comparison_results'],
                'errors': self.results['errors']
            }
            
            # Convert numpy types to native Python types
            json_results = json.loads(json.dumps(json_results, default=str))
            
            with open(output_file, 'w') as f:
                json.dump(json_results, f, indent=2)
            
            self.logger.info(f"Results saved to {output_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save results: {str(e)}")
            raise
    
    def run_validation(self, procedure_params: Dict, output_file: str = None) -> Dict:
        """
        Run the complete validation process
        
        Args:
            procedure_params (Dict): Parameters for the stored procedure
            output_file (str): Optional output file path
            
        Returns:
            Dict: Validation results
        """
        try:
            self.logger.info("Starting SQL Server to Fabric validation process")
            
            # Step 1: Execute SQL Server stored procedure
            self.logger.info("Step 1: Executing SQL Server stored procedure")
            sql_server_data = self.execute_sql_server_procedure(procedure_params)
            
            # Step 2: Transform data for Fabric
            self.logger.info("Step 2: Transforming data for Fabric")
            transformed_data = self.transform_data_for_fabric(sql_server_data)
            
            # Step 3: Upload to blob storage
            self.logger.info("Step 3: Uploading data to blob storage")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            blob_name = f"recon_test_data_{timestamp}.csv"
            blob_url = self.upload_to_blob_storage(transformed_data, blob_name)
            
            # Step 4: Create external table in Fabric
            self.logger.info("Step 4: Creating external table in Fabric")
            table_name = f"recon_test_table_{timestamp}"
            self.create_fabric_external_table(blob_url, table_name, transformed_data)
            
            # Step 5: Execute equivalent query in Fabric
            self.logger.info("Step 5: Executing equivalent query in Fabric")
            fabric_data = self.execute_fabric_equivalent_query(table_name)
            
            # Step 6: Compare datasets
            self.logger.info("Step 6: Comparing datasets")
            comparison_results = self.compare_datasets(sql_server_data, fabric_data)
            
            # Step 7: Generate validation report
            self.logger.info("Step 7: Generating validation report")
            validation_report = self.generate_validation_report()
            
            # Step 8: Save results
            if output_file:
                self.logger.info("Step 8: Saving results to file")
                self.save_results_to_file(output_file)
            
            self.logger.info("Validation process completed successfully")
            return validation_report
            
        except Exception as e:
            self.logger.error(f"Validation process failed: {str(e)}")
            self.results['errors'].append(f"Validation process error: {str(e)}")
            
            # Generate error report
            error_report = {
                'timestamp': datetime.now().isoformat(),
                'validation_status': 'ERROR',
                'errors': self.results['errors'],
                'partial_results': self.results
            }
            
            if output_file:
                try:
                    with open(output_file, 'w') as f:
                        json.dump(error_report, f, indent=2, default=str)
                except:
                    pass
            
            raise
        
        finally:
            # Cleanup connections
            if self.sql_server_conn:
                self.sql_server_conn.dispose()
            if self.fabric_conn:
                self.fabric_conn.dispose()


def main():
    """
    Main function to demonstrate usage of SQLServerToFabricReconTest
    """
    # Configuration dictionary
    config = {
        'sql_server': {
            'server': 'your-sql-server.database.windows.net',
            'database': 'your-database',
            'username': 'your-username',  # Optional for Windows Auth
            'password': 'your-password'   # Optional for Windows Auth
        },
        'fabric': {
            'server': 'your-fabric-server.database.windows.net',
            'database': 'your-fabric-database'
        },
        'azure_storage': {
            'account_name': 'your-storage-account',
            'container_name': 'your-container'
        },
        'log_level': 'INFO',
        'log_file': 'recon_test.log'
    }
    
    # Stored procedure parameters
    procedure_params = {
        'StartDate': '2023-01-01',
        'EndDate': '2023-12-31',
        'ClientId': None  # Optional parameter
    }
    
    try:
        # Initialize the recon test
        recon_test = SQLServerToFabricReconTest(config)
        
        # Run validation
        results = recon_test.run_validation(
            procedure_params=procedure_params,
            output_file='validation_results.json'
        )
        
        # Print summary
        print("\n" + "="*50)
        print("VALIDATION SUMMARY")
        print("="*50)
        print(f"Status: {results['validation_status']}")
        print(f"Timestamp: {results['timestamp']}")
        print(f"Match Percentage: {results['summary'].get('match_percentage', 0):.2f}%")
        
        if results['errors']:
            print("\nErrors:")
            for error in results['errors']:
                print(f"  - {error}")
        
        if results['recommendations']:
            print("\nRecommendations:")
            for rec in results['recommendations']:
                print(f"  - {rec}")
        
        print("\nDetailed results saved to validation_results.json")
        
    except Exception as e:
        print(f"Validation failed with error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())