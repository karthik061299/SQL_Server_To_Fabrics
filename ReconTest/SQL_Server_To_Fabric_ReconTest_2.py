_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   SQL Server to Fabric reconciliation test for uspSemanticClaimTransactionMeasuresData stored procedure with enhanced features
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pandas as pd
import pyodbc
import numpy as np
import logging
import json
import time
import hashlib
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import gc
import psutil
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from scipy import stats
import warnings
warnings.filterwarnings('ignore')