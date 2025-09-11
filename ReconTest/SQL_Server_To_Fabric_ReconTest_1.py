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