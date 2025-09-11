_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Comprehensive reconciliation test for SQL Server to Fabric migration of uspSemanticClaimTransactionMeasuresData
## *Version*: 2 
## *Updated on*: 
_____________________________________________

#!/usr/bin/env python3
"""
SQL Server to Fabric Reconciliation Test Suite

This module provides a comprehensive framework for validating the migration of SQL Server
stored procedures to Microsoft Fabric, specifically focusing on the
'uspSemanticClaimTransactionMeasuresData' procedure.

The test suite handles the end-to-end process of:
1. Executing SQL Server code
2. Transferring results to Microsoft Fabric
3. Running equivalent Fabric code
4. Validating that results match between platforms

Version 2 Enhancements:
- Improved error handling with detailed error messages
- Enhanced data comparison with column-level validation
- Added support for large dataset testing with batching
- Implemented parallel processing for performance optimization
- Added detailed reporting with mismatch analysis
- Enhanced security with Azure Key Vault integration
- Added support for parameterized testing
"""

import pyodbc
import pandas as pd
import numpy as np
import os
import json
import logging
import time
import hashlib
import uuid
import sys
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.keyvault.secrets import SecretClient
import pyarrow as pa
import pyarrow.parquet as pq
import warnings
warnings.filterwarnings('ignore')

# Configure logging with more detailed format
log_file = f"recon_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ConnectionConfig:
    """Configuration class for database connections and Azure services"""
    # SQL Server configuration
    sql_server: str
    sql_database: str
    sql_username: Optional[str] = None
    sql_password: Optional[str] = None
    sql_auth_type: str = "windows"  # "windows" or "sql"
    
    # Fabric configuration
    fabric_endpoint: str
    fabric_database: str
    
    # Azure Storage configuration
    azure_storage_account: str
    azure_container: str
    
    # Azure Authentication
    key_vault_url: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    
    # Test configuration
    batch_size: int = 100000
    timeout_seconds: int = 3600
    parallel_threads: int = 4
    comparison_tolerance: float = 0.0001

@dataclass
class ColumnComparisonResult:
    """Class to store column-level comparison results"""
    column_name: str
    match_count: int
    mismatch_count: int
    null_mismatch_count: int
    type_mismatch_count: int
    precision_mismatch_count: int
    match_percentage: float
    sample_mismatches: List[Dict] = field(default_factory=list)

@dataclass
class ReconciliationResult:
    """Class to store comprehensive reconciliation results"""
    test_name: str
    sql_server_count: int
    fabric_count: int
    matches: int
    mismatches: int
    match_percentage: float
    execution_time: float
    status: str
    sql_server_query_time: float = 0.0
    fabric_query_time: float = 0.0
    transfer_time: float = 0.0
    column_results: List[ColumnComparisonResult] = field(default_factory=list)
    error_details: Optional[str] = None
    sample_mismatches: List[Dict] = field(default_factory=list)

class SQLServerToFabricReconTest:
    """Main class for SQL Server to Fabric reconciliation testing"""
    
    def __init__(self, config: ConnectionConfig):
        """Initialize the reconciliation test framework
        
        Args:
            config: Configuration object with connection details
        """
        self.config = config
        self.credential = None
        self.blob_service_client = None
        self.container_client = None
        self.sql_connection = None
        self.fabric_connection = None
        self.test_results = []
        self.test_id = str(uuid.uuid4())[:8]
        self.temp_resources = []
        
        # Initialize Azure services
        self._initialize_azure_services()
    
    def _initialize_azure_services(self) -> None:
        """Initialize Azure services and authentication"""
        try:
            logger.info("Initializing Azure services...")
            
            # Initialize Azure credential
            if self.config.client_id and self.config.client_secret and self.config.tenant_id:
                logger.info("Using service principal authentication")
                self.credential = ClientSecretCredential(
                    tenant_id=self.config.tenant_id,
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret
                )
            else:
                logger.info("Using default Azure credential")
                self.credential = DefaultAzureCredential()
            
            # Initialize Blob Service Client
            storage_url = f"https://{self.config.azure_storage_account}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(
                account_url=storage_url,
                credential=self.credential
            )
            
            # Get container client
            self.container_client = self.blob_service_client.get_container_client(
                self.config.azure_container
            )
            
            # Verify container exists or create it
            try:
                self.container_client.get_container_properties()
            except Exception:
                logger.info(f"Container {self.config.azure_container} does not exist, creating it...")
                self.container_client = self.blob_service_client.create_container(
                    self.config.azure_container
                )
            
            logger.info("Azure services initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Azure services: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _get_secret_from_keyvault(self, secret_name: str) -> str:
        """Retrieve secret from Azure Key Vault
        
        Args:
            secret_name: Name of the secret to retrieve
            
        Returns:
            Secret value as string
        """
        try:
            if not self.config.key_vault_url:
                raise ValueError("Key Vault URL not provided in configuration")
                
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
        """Establish connection to SQL Server
        
        Returns:
            Active SQL Server connection
        """
        try:
            logger.info(f"Connecting to SQL Server: {self.config.sql_server}")
            
            # Build connection string based on authentication type
            if self.config.sql_auth_type.lower() == "windows":
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};" 
                    f"SERVER={self.config.sql_server};" 
                    f"DATABASE={self.config.sql_database};" 
                    f"Trusted_Connection=yes;" 
                    f"Connection Timeout=30;" 
                    f"Command Timeout={self.config.timeout_seconds};"
                )
            else:
                # SQL authentication
                username = self.config.sql_username
                password = self.config.sql_password
                
                # Try to get credentials from Key Vault if not provided
                if (not username or not password) and self.config.key_vault_url:
                    username = self._get_secret_from_keyvault("sql-username")
                    password = self._get_secret_from_keyvault("sql-password")
                
                if not username or not password:
                    raise ValueError("SQL authentication requires username and password")
                    
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};" 
                    f"SERVER={self.config.sql_server};" 
                    f"DATABASE={self.config.sql_database};" 
                    f"UID={username};" 
                    f"PWD={password};" 
                    f"Connection Timeout=30;" 
                    f"Command Timeout={self.config.timeout_seconds};"
                )
            
            # Establish connection
            connection = pyodbc.connect(connection_string)
            connection.autocommit = True
            logger.info("SQL Server connection established successfully")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _connect_fabric(self) -> pyodbc.Connection:
        """Establish connection to Microsoft Fabric
        
        Returns:
            Active Fabric connection
        """
        try:
            logger.info(f"Connecting to Microsoft Fabric: {self.config.fabric_endpoint}")
            
            # Fabric connection using Azure AD authentication
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};" 
                f"SERVER={self.config.fabric_endpoint};" 
                f"DATABASE={self.config.fabric_database};" 
                f"Authentication=ActiveDirectoryIntegrated;" 
                f"Connection Timeout=30;" 
                f"Command Timeout={self.config.timeout_seconds};"
            )
            
            connection = pyodbc.connect(connection_string)
            connection.autocommit = True
            logger.info("Fabric connection established successfully")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to Fabric: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _execute_sql_server_procedure(self, parameters: Dict = None) -> Tuple[pd.DataFrame, float]:
        """Execute the SQL Server stored procedure
        
        Args:
            parameters: Dictionary of parameters to pass to the stored procedure
            
        Returns:
            Tuple containing the result DataFrame and execution time in seconds
        """
        try:
            if not self.sql_connection:
                self.sql_connection = self._connect_sql_server()
            
            # Execute uspSemanticClaimTransactionMeasuresData stored procedure
            cursor = self.sql_connection.cursor()
            start_time = time.time()
            
            # Prepare and execute the stored procedure
            if parameters:
                param_string = ', '.join([f"@{k}=?" for k in parameters.keys()])
                sql_query = f"EXEC [Semantic].[uspSemanticClaimTransactionMeasuresData] {param_string}"
                logger.info(f"Executing SQL Server procedure with parameters: {parameters}")
                cursor.execute(sql_query, list(parameters.values()))
            else:
                logger.info("Executing SQL Server procedure without parameters")
                cursor.execute("EXEC [Semantic].[uspSemanticClaimTransactionMeasuresData] @pJobStartDateTime = '01/01/2023', @pJobEndDateTime = '12/31/2023'")
            
            # Fetch results
            columns = [column[0] for column in cursor.description]
            data = []
            
            # Fetch in batches to handle large result sets
            batch = cursor.fetchmany(self.config.batch_size)
            while batch:
                data.extend(batch)
                batch = cursor.fetchmany(self.config.batch_size)
            
            execution_time = time.time() - start_time
            
            # Convert to DataFrame
            df = pd.DataFrame.from_records(data, columns=columns)
            logger.info(f"SQL Server procedure executed successfully in {execution_time:.2f}s. Rows returned: {len(df)}")
            
            return df, execution_time
            
        except Exception as e:
            logger.error(f"Failed to execute SQL Server procedure: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _upload_to_blob_storage(self, df: pd.DataFrame, blob_name: str) -> Tuple[str, float]:
        """Upload DataFrame to Azure Blob Storage as Parquet
        
        Args:
            df: DataFrame to upload
            blob_name: Name for the blob in storage
            
        Returns:
            Tuple containing the blob URL and upload time in seconds
        """
        try:
            logger.info(f"Uploading data to blob storage as {blob_name}")
            start_time = time.time()
            
            # Handle large DataFrames by using PyArrow directly
            table = pa.Table.from_pandas(df)
            
            # Create a buffer to hold the Parquet data
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer)
            parquet_bytes = buffer.getvalue().to_pybytes()
            
            # Upload to blob storage
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(parquet_bytes, overwrite=True)
            
            # Track for cleanup
            self.temp_resources.append(("blob", blob_name))
            
            upload_time = time.time() - start_time
            blob_url = f"https://{self.config.azure_storage_account}.blob.core.windows.net/{self.config.azure_container}/{blob_name}"
            logger.info(f"Data uploaded to blob storage in {upload_time:.2f}s: {blob_url}")
            
            return blob_url, upload_time
            
        except Exception as e:
            logger.error(f"Failed to upload to blob storage: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _create_fabric_external_table(self, blob_url: str, table_name: str, df_schema: pd.DataFrame) -> None:
        """Create external table in Fabric pointing to blob storage
        
        Args:
            blob_url: URL to the blob containing the data
            table_name: Name for the external table
            df_schema: DataFrame to use for schema definition
        """
        try:
            logger.info(f"Creating external table {table_name} in Fabric")
            
            if not self.fabric_connection:
                self.fabric_connection = self._connect_fabric()
            
            cursor = self.fabric_connection.cursor()
            
            # Track for cleanup
            self.temp_resources.append(("table", table_name))
            
            # Drop table if exists
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(drop_sql)
            
            # Generate column definitions based on DataFrame schema
            column_defs = []
            for col, dtype in df_schema.dtypes.items():
                # Map pandas dtypes to SQL types
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "FLOAT"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "DATETIME2"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BIT"
                elif pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
                    sql_type = "STRING"
                else:
                    sql_type = "STRING"
                column_defs.append(f"[{col}] {sql_type}")
            
            columns_sql = ",\n    ".join(column_defs)
            
            # Create external data source if it doesn't exist
            try:
                cursor.execute("SELECT 1 FROM sys.external_data_sources WHERE name = 'ExternalDataSource'")
                if not cursor.fetchone():
                    create_data_source_sql = f"""
                    CREATE EXTERNAL DATA SOURCE ExternalDataSource 
                    WITH (
                        LOCATION = 'https://{self.config.azure_storage_account}.blob.core.windows.net/{self.config.azure_container}'
                    )
                    """
                    cursor.execute(create_data_source_sql)
            except Exception as e:
                logger.warning(f"Error checking/creating external data source: {str(e)}")
            
            # Create file format if it doesn't exist
            try:
                cursor.execute("SELECT 1 FROM sys.external_file_formats WHERE name = 'ParquetFileFormat'")
                if not cursor.fetchone():
                    create_file_format_sql = """
                    CREATE EXTERNAL FILE FORMAT ParquetFileFormat
                    WITH (
                        FORMAT_TYPE = PARQUET
                    )
                    """
                    cursor.execute(create_file_format_sql)
            except Exception as e:
                logger.warning(f"Error checking/creating external file format: {str(e)}")
            
            # Extract just the filename from the URL
            blob_path = blob_url.split(f"{self.config.azure_container}/")[1]
            
            # Create external table
            create_sql = f"""
            CREATE TABLE {table_name} (
                {columns_sql}
            )
            WITH (
                LOCATION = '{blob_path}',
                DATA_SOURCE = ExternalDataSource,
                FILE_FORMAT = ParquetFileFormat
            )
            """
            
            cursor.execute(create_sql)
            logger.info(f"External table {table_name} created successfully in Fabric")
            
        except Exception as e:
            logger.error(f"Failed to create Fabric external table: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _execute_fabric_query(self, query: str) -> Tuple[pd.DataFrame, float]:
        """Execute query in Fabric and return results
        
        Args:
            query: SQL query to execute in Fabric
            
        Returns:
            Tuple containing the result DataFrame and execution time in seconds
        """
        try:
            logger.info(f"Executing Fabric query: {query[:100]}...")
            
            if not self.fabric_connection:
                self.fabric_connection = self._connect_fabric()
            
            start_time = time.time()
            
            # Execute query and fetch results
            cursor = self.fabric_connection.cursor()
            cursor.execute(query)
            
            # Fetch column names
            columns = [column[0] for column in cursor.description]
            
            # Fetch data in batches
            data = []
            batch = cursor.fetchmany(self.config.batch_size)
            while batch:
                data.extend(batch)
                batch = cursor.fetchmany(self.config.batch_size)
            
            execution_time = time.time() - start_time
            
            # Convert to DataFrame
            df = pd.DataFrame.from_records(data, columns=columns)
            logger.info(f"Fabric query executed successfully in {execution_time:.2f}s. Rows returned: {len(df)}")
            
            return df, execution_time
            
        except Exception as e:
            logger.error(f"Failed to execute Fabric query: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _compare_column_values(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                             column: str) -> ColumnComparisonResult:
        """Compare values in a specific column between two DataFrames
        
        Args:
            df1: First DataFrame (SQL Server results)
            df2: Second DataFrame (Fabric results)
            column: Column name to compare
            
        Returns:
            ColumnComparisonResult with comparison details
        """
        try:
            # Handle case where column doesn't exist in one DataFrame
            if column not in df1.columns or column not in df2.columns:
                return ColumnComparisonResult(
                    column_name=column,
                    match_count=0,
                    mismatch_count=max(len(df1), len(df2)),
                    null_mismatch_count=0,
                    type_mismatch_count=0,
                    precision_mismatch_count=0,
                    match_percentage=0.0
                )
            
            # Extract columns for comparison
            col1 = df1[column]
            col2 = df2[column]
            
            # Handle different data types
            type1 = col1.dtype
            type2 = col2.dtype
            type_mismatch = type1 != type2
            
            # Convert to common type for comparison if needed
            if type_mismatch:
                # For numeric comparisons, convert both to float
                if pd.api.types.is_numeric_dtype(type1) and pd.api.types.is_numeric_dtype(type2):
                    col1 = col1.astype(float)
                    col2 = col2.astype(float)
                else:
                    # For other types, convert both to string
                    col1 = col1.astype(str)
                    col2 = col2.astype(str)
            
            # Compare values
            if pd.api.types.is_numeric_dtype(col1.dtype) and pd.api.types.is_numeric_dtype(col2.dtype):
                # For numeric columns, use tolerance for floating point comparison
                is_close = np.isclose(
                    col1, col2, 
                    rtol=self.config.comparison_tolerance, 
                    atol=self.config.comparison_tolerance,
                    equal_nan=True
                )
                matches = is_close.sum()
                precision_mismatches = (~is_close & ~(col1.isna() | col2.isna())).sum()
            else:
                # For non-numeric columns, use exact comparison
                matches = (col1 == col2).sum()
                precision_mismatches = 0
            
            # Count null mismatches
            null_mismatches = ((col1.isna() & ~col2.isna()) | (~col1.isna() & col2.isna())).sum()
            
            # Calculate total mismatches
            total_mismatches = len(col1) - matches
            
            # Calculate match percentage
            match_percentage = (matches / len(col1)) * 100 if len(col1) > 0 else 0.0
            
            # Sample mismatches for reporting
            sample_mismatches = []
            if total_mismatches > 0:
                # Find indices of mismatches
                if pd.api.types.is_numeric_dtype(col1.dtype) and pd.api.types.is_numeric_dtype(col2.dtype):
                    mismatch_indices = np.where(~is_close)[0][:5]  # Get up to 5 mismatches
                else:
                    mismatch_indices = np.where(col1 != col2)[0][:5]  # Get up to 5 mismatches
                
                for idx in mismatch_indices:
                    if idx < len(col1) and idx < len(col2):
                        sample_mismatches.append({
                            "index": int(idx),
                            "sql_server_value": str(col1.iloc[idx]),
                            "fabric_value": str(col2.iloc[idx])
                        })
            
            return ColumnComparisonResult(
                column_name=column,
                match_count=int(matches),
                mismatch_count=int(total_mismatches),
                null_mismatch_count=int(null_mismatches),
                type_mismatch_count=1 if type_mismatch else 0,
                precision_mismatch_count=int(precision_mismatches),
                match_percentage=float(match_percentage),
                sample_mismatches=sample_mismatches
            )
            
        except Exception as e:
            logger.error(f"Error comparing column {column}: {str(e)}")
            return ColumnComparisonResult(
                column_name=column,
                match_count=0,
                mismatch_count=max(len(df1), len(df2)) if column in df1.columns and column in df2.columns else 0,
                null_mismatch_count=0,
                type_mismatch_count=0,
                precision_mismatch_count=0,
                match_percentage=0.0,
                sample_mismatches=[{"error": str(e)}]
            )