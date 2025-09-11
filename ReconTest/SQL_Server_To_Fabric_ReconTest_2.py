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