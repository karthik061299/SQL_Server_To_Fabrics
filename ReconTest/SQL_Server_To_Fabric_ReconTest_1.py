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