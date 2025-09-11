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

class DataQualityValidator:
    """Comprehensive data quality validation"""
    
    def __init__(self, logger):
        self.logger = logger
        self.quality_issues = []
    
    def validate_dataframe(self, df: pd.DataFrame, source: str) -> Dict[str, Any]:
        """Perform comprehensive data quality checks"""
        self.logger.info(f"Starting data quality validation for {source}")
        
        validation_results = {
            'source': source,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'null_counts': {},
            'duplicate_rows': 0,
            'data_types': {},
            'outliers': {},
            'business_rules': {}
        }
        
        # Null value analysis
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_percentage = (null_count / len(df)) * 100
            validation_results['null_counts'][col] = {
                'count': int(null_count),
                'percentage': round(null_percentage, 2)
            }
            
            if null_percentage > 50:
                self.quality_issues.append(f"{source}: Column '{col}' has {null_percentage:.2f}% null values")
        
        # Duplicate analysis
        duplicate_count = df.duplicated().sum()
        validation_results['duplicate_rows'] = int(duplicate_count)
        
        if duplicate_count > 0:
            self.quality_issues.append(f"{source}: Found {duplicate_count} duplicate rows")
        
        # Data type analysis
        for col in df.columns:
            validation_results['data_types'][col] = str(df[col].dtype)
        
        # Outlier detection for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if not df[col].empty:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                
                validation_results['outliers'][col] = {
                    'count': len(outliers),
                    'percentage': round((len(outliers) / len(df)) * 100, 2)
                }
        
        # Business rule validation
        validation_results['business_rules'] = self._validate_business_rules(df, source)
        
        self.logger.info(f"Data quality validation completed for {source}")
        return validation_results
    
    def _validate_business_rules(self, df: pd.DataFrame, source: str) -> Dict[str, Any]:
        """Validate business-specific rules"""
        rules_results = {}
        
        # Example business rules - customize based on your data
        if 'Amount' in df.columns:
            negative_amounts = (df['Amount'] < 0).sum()
            rules_results['negative_amounts'] = {
                'count': int(negative_amounts),
                'percentage': round((negative_amounts / len(df)) * 100, 2)
            }
        
        if 'Date' in df.columns:
            try:
                df['Date'] = pd.to_datetime(df['Date'])
                future_dates = (df['Date'] > datetime.now()).sum()
                rules_results['future_dates'] = {
                    'count': int(future_dates),
                    'percentage': round((future_dates / len(df)) * 100, 2)
                }
            except:
                rules_results['date_conversion_error'] = True
        
        return rules_results
    
    def get_quality_issues(self) -> List[str]:
        """Get all identified quality issues"""
        return self.quality_issues

class StatisticalAnalyzer:
    """Advanced statistical analysis for data comparison"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def compare_distributions(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                            numeric_columns: List[str]) -> Dict[str, Any]:
        """Compare statistical distributions between datasets"""
        self.logger.info("Starting statistical distribution comparison")
        
        comparison_results = {}
        
        for col in numeric_columns:
            if col in df1.columns and col in df2.columns:
                try:
                    # Remove null values
                    data1 = df1[col].dropna()
                    data2 = df2[col].dropna()
                    
                    if len(data1) > 0 and len(data2) > 0:
                        # Kolmogorov-Smirnov test
                        ks_stat, ks_pvalue = stats.ks_2samp(data1, data2)
                        
                        # Mann-Whitney U test
                        mw_stat, mw_pvalue = stats.mannwhitneyu(data1, data2, alternative='two-sided')
                        
                        # Descriptive statistics
                        desc_stats = {
                            'source1': {
                                'mean': float(data1.mean()),
                                'median': float(data1.median()),
                                'std': float(data1.std()),
                                'min': float(data1.min()),
                                'max': float(data1.max())
                            },
                            'source2': {
                                'mean': float(data2.mean()),
                                'median': float(data2.median()),
                                'std': float(data2.std()),
                                'min': float(data2.min()),
                                'max': float(data2.max())
                            }
                        }
                        
                        comparison_results[col] = {
                            'ks_test': {'statistic': float(ks_stat), 'p_value': float(ks_pvalue)},
                            'mannwhitney_test': {'statistic': float(mw_stat), 'p_value': float(mw_pvalue)},
                            'descriptive_stats': desc_stats,
                            'distributions_similar': ks_pvalue > 0.05 and mw_pvalue > 0.05
                        }
                        
                except Exception as e:
                    self.logger.error(f"Statistical comparison failed for column {col}: {str(e)}")
                    comparison_results[col] = {'error': str(e)}
        
        return comparison_results