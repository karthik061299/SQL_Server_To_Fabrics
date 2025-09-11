_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive Python script for SQL Server to Fabric migration validation of uspSemanticClaimTransactionMeasuresData
## *Version*: 1 
## *Updated on*: 
_____________________________________________

"""
SQL Server to Fabric Migration Validation Agent
Purpose: End-to-end validation of uspSemanticClaimTransactionMeasuresData migration
Author: AAVA
Version: 1.0
"""

import os
import sys
import logging
import pandas as pd
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from datetime import datetime