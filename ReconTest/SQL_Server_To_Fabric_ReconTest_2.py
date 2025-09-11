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