_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server to Fabric migration validation for uspSemanticClaimTransactionMeasuresData
## *Version*: 2 
## *Updated on*: 
_____________________________________________

#!/usr/bin/env python3
"""
SQL Server to Fabric Migration Validation Tool
Version 2.0 - Comprehensive Reconciliation Testing

This script validates the migration of uspSemanticClaimTransactionMeasuresData
from SQL Server to Microsoft Fabric by:
1. Executing the stored procedure in SQL Server
2. Executing equivalent code in Fabric
3. Comparing results for data integrity
4. Generating detailed validation reports
"""

import pandas as pd
import pyodbc
import logging
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ValidationConfig:
    """Configuration class for validation parameters"""
    sql_server_connection: str
    fabric_connection: str
    stored_procedure_name: str = 'uspSemanticClaimTransactionMeasuresData'
    tolerance_percentage: float = 0.001  # 0.001% tolerance for numeric comparisons
    max_row_differences: int = 100  # Maximum number of row differences to report
    chunk_size: int = 10000  # Chunk size for large dataset processing
    timeout_seconds: int = 3600  # Query timeout in seconds

@dataclass
class ValidationResult:
    """Class to store validation results"""
    test_name: str
    passed: bool
    sql_server_count: int
    fabric_count: int
    differences_found: List[Dict]
    execution_time_sql: float
    execution_time_fabric: float
    error_message: Optional[str] = None
    data_hash_sql: Optional[str] = None
    data_hash_fabric: Optional[str] = None

class DatabaseConnector:
    """Handles database connections and query execution"""
    
    def __init__(self, connection_string: str, db_type: str):
        self.connection_string = connection_string
        self.db_type = db_type
        self.connection = None
    
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            if self.db_type.lower() == 'sqlserver':
                self.connection = pyodbc.connect(self.connection_string, timeout=30)
            elif self.db_type.lower() == 'fabric':
                # Fabric connection using appropriate driver
                self.connection = pyodbc.connect(self.connection_string, timeout=30)
            
            logger.info(f"Successfully connected to {self.db_type}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to {self.db_type}: {str(e)}")
            return False
    
    def execute_query(self, query: str, params: List = None) -> Tuple[pd.DataFrame, float]:
        """Execute query and return results with execution time"""
        start_time = time.time()
        try:
            if params:
                df = pd.read_sql(query, self.connection, params=params)
            else:
                df = pd.read_sql(query, self.connection)
            
            execution_time = time.time() - start_time
            logger.info(f"Query executed successfully in {execution_time:.2f} seconds. Rows returned: {len(df)}")
            return df, execution_time
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query execution failed after {execution_time:.2f} seconds: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info(f"Connection to {self.db_type} closed")