_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive SQL Server to Fabric reconciliation test for uspSemanticClaimTransactionMeasuresData with enhanced validation and migration support
## *Version*: 2 
## *Updated on*: 
_____________________________________________

import pyodbc
import pandas as pd
import numpy as np
import os
import json
import logging
import hashlib
import time
import uuid
from datetime import datetime