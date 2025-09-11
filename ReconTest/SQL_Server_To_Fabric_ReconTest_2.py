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