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