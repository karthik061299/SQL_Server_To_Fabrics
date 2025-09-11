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
        Setup logging configuration
        """
        log_level = self.config.get('log_level', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'recontest_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def create_sql_server_connection(self) -> bool:
        """
        Create connection to SQL Server
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            connection_string = (
                f"DRIVER={{{self.config['sql_server']['driver']}}};"
                f"SERVER={self.config['sql_server']['server']};"
                f"DATABASE={self.config['sql_server']['database']};"
                f"UID={self.config['sql_server']['username']};"
                f"PWD={self.config['sql_server']['password']};"
                f"Trusted_Connection={self.config['sql_server'].get('trusted_connection', 'no')};"
            )
            
            self.sql_server_conn = pyodbc.connect(connection_string)
            self.logger.info("SQL Server connection established successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to SQL Server: {str(e)}")
            self.results['errors'].append(f"SQL Server connection error: {str(e)}")
            return False
    
    def create_fabric_connection(self) -> bool:
        """
        Create connection to Microsoft Fabric
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Using Azure SQL Database connection for Fabric SQL Endpoint
            connection_string = (
                f"DRIVER={{{self.config['fabric']['driver']}}};"
                f"SERVER={self.config['fabric']['server']};"
                f"DATABASE={self.config['fabric']['database']};"
                f"Authentication=ActiveDirectoryIntegrated;"
            )
            
            self.fabric_conn = pyodbc.connect(connection_string)
            self.logger.info("Fabric connection established successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Fabric: {str(e)}")
            self.results['errors'].append(f"Fabric connection error: {str(e)}")
            return False
    
    def execute_sql_server_procedure(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Execute the uspSemanticClaimTransactionMeasuresData stored procedure on SQL Server
        
        Args:
            start_date (str): Job start datetime
            end_date (str): Job end datetime
            
        Returns:
            pd.DataFrame: Results from SQL Server execution
        """
        try:
            cursor = self.sql_server_conn.cursor()
            
            # Execute the stored procedure
            exec_query = """
            EXEC [Semantic].[uspSemanticClaimTransactionMeasuresData] 
                @pJobStartDateTime = ?, 
                @pJobEndDateTime = ?
            """
            
            self.logger.info(f"Executing SQL Server procedure with dates: {start_date} to {end_date}")
            cursor.execute(exec_query, start_date, end_date)
            
            # Fetch results
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            
            df = pd.DataFrame.from_records(data, columns=columns)
            
            self.logger.info(f"SQL Server procedure executed successfully. Rows returned: {len(df)}")
            self.results['sql_server_data'] = df
            
            cursor.close()
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing SQL Server procedure: {str(e)}")
            self.results['errors'].append(f"SQL Server execution error: {str(e)}")
            return pd.DataFrame()
    
    def transform_data_for_fabric(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform SQL Server data for Fabric compatibility
        
        Args:
            df (pd.DataFrame): Source dataframe from SQL Server
            
        Returns:
            pd.DataFrame: Transformed dataframe
        """
        try:
            transformed_df = df.copy()
            
            # Handle datetime columns
            datetime_columns = ['SourceClaimTransactionCreateDate', 'TransactionCreateDate', 
                              'TransactionSubmitDate', 'RecordEffectiveDate', 'LoadUpdateDate', 'LoadCreateDate']
            
            for col in datetime_columns:
                if col in transformed_df.columns:
                    transformed_df[col] = pd.to_datetime(transformed_df[col], errors='coerce')
            
            # Handle numeric columns
            numeric_columns = [col for col in transformed_df.columns if 'Amount' in col or 
                             col.startswith(('Net', 'Gross', 'Recovery', 'Reserves'))]
            
            for col in numeric_columns:
                if col in transformed_df.columns:
                    transformed_df[col] = pd.to_numeric(transformed_df[col], errors='coerce')
            
            # Handle NULL values
            transformed_df = transformed_df.fillna({
                col: 0 if transformed_df[col].dtype in ['int64', 'float64'] else ''
                for col in transformed_df.columns
            })
            
            self.logger.info("Data transformation completed successfully")
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Error in data transformation: {str(e)}")
            self.results['errors'].append(f"Data transformation error: {str(e)}")
            return df
    
    def upload_to_blob_storage(self, df: pd.DataFrame, blob_name: str) -> bool:
        """
        Upload dataframe to Azure Blob Storage
        
        Args:
            df (pd.DataFrame): Dataframe to upload
            blob_name (str): Name of the blob
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            # Initialize blob client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{self.config['blob_storage']['account_name']}.blob.core.windows.net",
                credential=DefaultAzureCredential()
            )
            
            container_name = self.config['blob_storage']['container_name']
            
            # Convert dataframe to parquet format for better performance
            parquet_data = df.to_parquet(index=False)
            
            # Upload to blob
            blob_client = blob_service_client.get_blob_client(
                container=container_name, 
                blob=f"{blob_name}.parquet"
            )
            
            blob_client.upload_blob(parquet_data, overwrite=True)
            
            self.logger.info(f"Data uploaded to blob storage: {blob_name}.parquet")
            return True
            
        except Exception as e:
            self.logger.error(f"Error uploading to blob storage: {str(e)}")
            self.results['errors'].append(f"Blob storage upload error: {str(e)}")
            return False
    
    def create_fabric_external_table(self, table_name: str, blob_path: str) -> bool:
        """
        Create external table in Fabric pointing to blob storage
        
        Args:
            table_name (str): Name of the external table
            blob_path (str): Path to the blob storage file
            
        Returns:
            bool: True if table created successfully, False otherwise
        """
        try:
            cursor = self.fabric_conn.cursor()
            
            # Drop table if exists
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(drop_query)
            
            # Create external table
            create_table_query = f"""
            CREATE TABLE {table_name}
            WITH (
                LOCATION = '{blob_path}',
                DATA_SOURCE = ExternalDataSource,
                FILE_FORMAT = ParquetFileFormat
            )
            AS SELECT * FROM OPENROWSET(
                BULK '{blob_path}',
                DATA_SOURCE = 'ExternalDataSource',
                FORMAT = 'PARQUET'
            ) AS [result]
            """
            
            cursor.execute(create_table_query)
            cursor.commit()
            
            self.logger.info(f"External table {table_name} created successfully")
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating external table: {str(e)}")
            self.results['errors'].append(f"External table creation error: {str(e)}")
            return False
    
    def execute_fabric_equivalent_query(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Execute equivalent query in Fabric to match SQL Server procedure logic
        
        Args:
            start_date (str): Job start datetime
            end_date (str): Job end datetime
            
        Returns:
            pd.DataFrame: Results from Fabric execution
        """
        try:
            # Fabric equivalent query (simplified version of the stored procedure logic)
            fabric_query = f"""
            WITH PolicyRiskStateData AS (
                SELECT prs.*,
                       ROW_NUMBER() OVER(
                           PARTITION BY prs.PolicyWCKey, prs.RiskState 
                           ORDER BY prs.RetiredInd, prs.RiskStateEffectiveDate DESC, 
                                   prs.RecordEffectiveDate DESC, prs.LoadUpdateDate DESC, 
                                   prs.PolicyRiskStateWCKey DESC
                       ) AS Rownum
                FROM Semantic.PolicyRiskStateDescriptors prs 
                WHERE prs.retiredind = 0
            ),
            FilteredPolicyRiskState AS (
                SELECT * FROM PolicyRiskStateData WHERE Rownum = 1
            ),
            ClaimTransactionData AS (
                SELECT DISTINCT 
                    FactClaimTransactionLineWC.FactClaimTransactionLineWCKey,
                    COALESCE(FactClaimTransactionLineWC.RevisionNumber, 0) AS RevisionNumber,
                    FactClaimTransactionLineWC.PolicyWCKey,
                    COALESCE(rskState.PolicyRiskStateWCKey, -1) AS PolicyRiskStateWCKey,
                    FactClaimTransactionLineWC.ClaimWCKey,
                    FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey,
                    FactClaimTransactionLineWC.ClaimTransactionWCKey,
                    FactClaimTransactionLineWC.ClaimCheckKey,
                    COALESCE(polAgcy.AgencyKey, -1) AS AgencyKey,
                    FactClaimTransactionLineWC.SourceTransactionLineItemCreateDate AS SourceClaimTransactionCreateDate,
                    FactClaimTransactionLineWC.SourceTransactionLineItemCreateDateKey AS SourceClaimTransactionCreateDateKey,
                    ClaimTransactionDescriptors.SourceTransactionCreateDate AS TransactionCreateDate,
                    ClaimTransactionDescriptors.TransactionSubmitDate,
                    FactClaimTransactionLineWC.SourceSystem,
                    FactClaimTransactionLineWC.RecordEffectiveDate,
                    CONCAT(FactClaimTransactionLineWC.FactClaimTransactionLineWCKey, '~', FactClaimTransactionLineWC.RevisionNumber) AS SourceSystemIdentifier,
                    FactClaimTransactionLineWC.TransactionAmount,
                    FactClaimTransactionLineWC.RetiredInd
                FROM EDSWH.dbo.FactClaimTransactionLineWC
                INNER JOIN edswh.dbo.dimClaimTransactionWC t
                    ON FactClaimTransactionLineWC.ClaimTransactionWCKey = t.ClaimTransactionWCKey
                INNER JOIN Semantic.ClaimTransactionDescriptors AS ClaimTransactionDescriptors
                    ON FactClaimTransactionLineWC.ClaimTransactionLineCategoryKey = ClaimTransactionDescriptors.ClaimTransactionLineCategoryKey
                    AND FactClaimTransactionLineWC.ClaimTransactionWCKey = ClaimTransactionDescriptors.ClaimTransactionWCKey
                    AND FactClaimTransactionLineWC.ClaimWCKey = ClaimTransactionDescriptors.ClaimWCkey
                INNER JOIN Semantic.ClaimDescriptors AS ClaimDescriptors
                    ON FactClaimTransactionLineWC.ClaimWCKey = ClaimDescriptors.ClaimWCKey
                LEFT JOIN Semantic.PolicyDescriptors AS polAgcy
                    ON FactClaimTransactionLineWC.PolicyWCKey = polAgcy.PolicyWCKey
                LEFT JOIN FilteredPolicyRiskState AS rskState
                    ON FactClaimTransactionLineWC.PolicyWCKey = rskState.PolicyWCKey
                    AND COALESCE(ClaimDescriptors.EmploymentLocationState, ClaimDescriptors.JurisdictionState) = rskState.RiskState
                WHERE FactClaimTransactionLineWC.LoadUpdateDate >= '{start_date}'
                   OR t.LoadUpdateDate >= '{start_date}'
            )
            SELECT *,
                   CONVERT(NVARCHAR(128), HASHBYTES('SHA2_256', 
                       CONCAT(FactClaimTransactionLineWCKey, '~', RevisionNumber, '~', PolicyWCKey, '~', 
                             PolicyRiskStateWCKey, '~', ClaimWCKey, '~', TransactionAmount)
                   ), 1) AS HashValue,
                   GETDATE() AS LoadUpdateDate,
                   GETDATE() AS LoadCreateDate
            FROM ClaimTransactionData
            """
            
            df = pd.read_sql(fabric_query, self.fabric_conn)
            
            self.logger.info(f"Fabric query executed successfully. Rows returned: {len(df)}")
            self.results['fabric_data'] = df
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing Fabric query: {str(e)}")
            self.results['errors'].append(f"Fabric execution error: {str(e)}")
            return pd.DataFrame()
    
    def compare_datasets(self, sql_server_df: pd.DataFrame, fabric_df: pd.DataFrame) -> Dict:
        """
        Compare SQL Server and Fabric datasets
        
        Args:
            sql_server_df (pd.DataFrame): SQL Server results
            fabric_df (pd.DataFrame): Fabric results
            
        Returns:
            Dict: Comparison results
        """
        comparison_results = {
            'row_count_match': False,
            'column_count_match': False,
            'schema_match': False,
            'data_match': False,
            'sql_server_rows': len(sql_server_df),
            'fabric_rows': len(fabric_df),
            'sql_server_columns': len(sql_server_df.columns) if not sql_server_df.empty else 0,
            'fabric_columns': len(fabric_df.columns) if not fabric_df.empty else 0,
            'missing_in_fabric': [],
            'missing_in_sql_server': [],
            'data_differences': [],
            'hash_comparison': {}
        }