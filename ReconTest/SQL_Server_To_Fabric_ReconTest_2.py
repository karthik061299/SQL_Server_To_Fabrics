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

@dataclass
class ReconConfig:
    """Configuration class for reconciliation test"""
    chunk_size: int = 10000
    max_workers: int = 4
    retry_attempts: int = 3
    retry_delay: float = 1.0
    memory_threshold: float = 0.8
    tolerance: float = 0.01
    enable_parallel: bool = True
    enable_statistics: bool = True
    log_level: str = "INFO"
    output_dir: str = "recon_reports"
    key_vault_url: str = ""
    
class EnhancedLogger:
    """Enhanced logging with multiple handlers and detailed formatting"""
    
    def __init__(self, name: str, log_level: str = "INFO", output_dir: str = "logs"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)
        
        # Detailed formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler(
            Path(output_dir) / f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Error file handler
        error_handler = logging.FileHandler(
            Path(output_dir) / f"{name}_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        self.logger.addHandler(error_handler)
    
    def info(self, message: str):
        self.logger.info(message)
    
    def error(self, message: str):
        self.logger.error(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def debug(self, message: str):
        self.logger.debug(message)

class AzureKeyVaultManager:
    """Secure credential management using Azure Key Vault"""
    
    def __init__(self, vault_url: str):
        self.vault_url = vault_url
        self.credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=vault_url, credential=self.credential)
        
    def get_secret(self, secret_name: str) -> str:
        """Retrieve secret from Azure Key Vault"""
        try:
            secret = self.client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            raise Exception(f"Failed to retrieve secret '{secret_name}': {str(e)}")

class PerformanceMonitor:
    """Performance monitoring and metrics collection"""
    
    def __init__(self, logger):
        self.logger = logger
        self.metrics = {}
        self.start_time = None
        
    def start_monitoring(self, operation: str):
        """Start monitoring an operation"""
        self.start_time = time.time()
        self.metrics[operation] = {
            'start_time': self.start_time,
            'start_memory': psutil.virtual_memory().percent,
            'start_cpu': psutil.cpu_percent()
        }
        
    def end_monitoring(self, operation: str):
        """End monitoring and record metrics"""
        if operation in self.metrics:
            end_time = time.time()
            self.metrics[operation].update({
                'end_time': end_time,
                'duration': end_time - self.metrics[operation]['start_time'],
                'end_memory': psutil.virtual_memory().percent,
                'end_cpu': psutil.cpu_percent(),
                'memory_delta': psutil.virtual_memory().percent - self.metrics[operation]['start_memory']
            })
            
            self.logger.info(f"Operation '{operation}' completed in {self.metrics[operation]['duration']:.2f}s")
            self.logger.info(f"Memory usage: {self.metrics[operation]['memory_delta']:+.2f}%")
    
    def get_metrics(self) -> Dict:
        """Get all collected metrics"""
        return self.metrics