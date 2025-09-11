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

class DataValidator:
    """Handles data validation and comparison logic"""
    
    def __init__(self, config: ValidationConfig):
        self.config = config
        self.sql_connector = DatabaseConnector(config.sql_server_connection, 'sqlserver')
        self.fabric_connector = DatabaseConnector(config.fabric_connection, 'fabric')
    
    def calculate_data_hash(self, df: pd.DataFrame) -> str:
        """Calculate hash of dataframe for quick comparison"""
        # Sort dataframe to ensure consistent hashing
        df_sorted = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        # Convert to string and hash
        data_string = df_sorted.to_string()
        return hashlib.md5(data_string.encode()).hexdigest()
    
    def compare_dataframes(self, df_sql: pd.DataFrame, df_fabric: pd.DataFrame) -> List[Dict]:
        """Compare two dataframes and return differences"""
        differences = []
        
        # Check row counts
        if len(df_sql) != len(df_fabric):
            differences.append({
                'type': 'row_count_mismatch',
                'sql_count': len(df_sql),
                'fabric_count': len(df_fabric),
                'description': f"Row count mismatch: SQL Server has {len(df_sql)} rows, Fabric has {len(df_fabric)} rows"
            })
        
        # Check column names
        sql_columns = set(df_sql.columns)
        fabric_columns = set(df_fabric.columns)
        
        if sql_columns != fabric_columns:
            differences.append({
                'type': 'column_mismatch',
                'sql_columns': list(sql_columns),
                'fabric_columns': list(fabric_columns),
                'missing_in_fabric': list(sql_columns - fabric_columns),
                'extra_in_fabric': list(fabric_columns - sql_columns),
                'description': 'Column structure mismatch between SQL Server and Fabric'
            })
            return differences
        
        # Align dataframes for comparison
        common_columns = list(sql_columns.intersection(fabric_columns))
        df_sql_aligned = df_sql[common_columns].sort_values(by=common_columns).reset_index(drop=True)
        df_fabric_aligned = df_fabric[common_columns].sort_values(by=common_columns).reset_index(drop=True)
        
        # Compare data row by row (for smaller datasets)
        if len(df_sql_aligned) <= self.config.chunk_size:
            row_differences = self._compare_rows(df_sql_aligned, df_fabric_aligned)
            differences.extend(row_differences)
        else:
            # For large datasets, use chunked comparison
            chunk_differences = self._compare_chunks(df_sql_aligned, df_fabric_aligned)
            differences.extend(chunk_differences)
        
        return differences
    
    def _compare_rows(self, df_sql: pd.DataFrame, df_fabric: pd.DataFrame) -> List[Dict]:
        """Compare dataframes row by row"""
        differences = []
        max_rows = min(len(df_sql), len(df_fabric))
        
        for idx in range(max_rows):
            if len(differences) >= self.config.max_row_differences:
                differences.append({
                    'type': 'max_differences_reached',
                    'description': f'Maximum number of differences ({self.config.max_row_differences}) reached. Stopping comparison.'
                })
                break
            
            sql_row = df_sql.iloc[idx]
            fabric_row = df_fabric.iloc[idx]
            
            row_diff = self._compare_single_row(sql_row, fabric_row, idx)
            if row_diff:
                differences.append(row_diff)
        
        return differences
    
    def _compare_single_row(self, sql_row: pd.Series, fabric_row: pd.Series, row_index: int) -> Optional[Dict]:
        """Compare two rows and return differences if any"""
        column_differences = []
        
        for col in sql_row.index:
            sql_val = sql_row[col]
            fabric_val = fabric_row[col]
            
            # Handle null values
            if pd.isna(sql_val) and pd.isna(fabric_val):
                continue
            elif pd.isna(sql_val) or pd.isna(fabric_val):
                column_differences.append({
                    'column': col,
                    'sql_value': sql_val,
                    'fabric_value': fabric_val,
                    'difference_type': 'null_mismatch'
                })
                continue
            
            # Handle numeric values with tolerance
            if isinstance(sql_val, (int, float)) and isinstance(fabric_val, (int, float)):
                if abs(sql_val - fabric_val) > abs(sql_val * self.config.tolerance_percentage / 100):
                    column_differences.append({
                        'column': col,
                        'sql_value': sql_val,
                        'fabric_value': fabric_val,
                        'difference': abs(sql_val - fabric_val),
                        'difference_type': 'numeric_tolerance_exceeded'
                    })
            # Handle string and other types
            elif str(sql_val) != str(fabric_val):
                column_differences.append({
                    'column': col,
                    'sql_value': sql_val,
                    'fabric_value': fabric_val,
                    'difference_type': 'value_mismatch'
                })
        
        if column_differences:
            return {
                'type': 'row_data_mismatch',
                'row_index': row_index,
                'column_differences': column_differences,
                'description': f'Data mismatch found in row {row_index}'
            }
        
        return None
    
    def _compare_chunks(self, df_sql: pd.DataFrame, df_fabric: pd.DataFrame) -> List[Dict]:
        """Compare large dataframes in chunks"""
        differences = []
        
        # Calculate chunk-level statistics for comparison
        sql_stats = self._calculate_chunk_statistics(df_sql)
        fabric_stats = self._calculate_chunk_statistics(df_fabric)
        
        # Compare statistics
        for col in sql_stats:
            if col in fabric_stats:
                stat_diff = self._compare_statistics(sql_stats[col], fabric_stats[col], col)
                if stat_diff:
                    differences.append(stat_diff)
        
        return differences
    
    def _calculate_chunk_statistics(self, df: pd.DataFrame) -> Dict:
        """Calculate statistical summary of dataframe"""
        stats = {}
        
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                stats[col] = {
                    'count': df[col].count(),
                    'sum': df[col].sum(),
                    'mean': df[col].mean(),
                    'std': df[col].std(),
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'null_count': df[col].isnull().sum()
                }
            else:
                stats[col] = {
                    'count': df[col].count(),
                    'unique_count': df[col].nunique(),
                    'null_count': df[col].isnull().sum(),
                    'most_frequent': df[col].mode().iloc[0] if not df[col].mode().empty else None
                }
        
        return stats
    
    def _compare_statistics(self, sql_stats: Dict, fabric_stats: Dict, column_name: str) -> Optional[Dict]:
        """Compare statistical summaries"""
        differences = []
        
        for stat_name in sql_stats:
            if stat_name in fabric_stats:
                sql_val = sql_stats[stat_name]
                fabric_val = fabric_stats[stat_name]
                
                if pd.isna(sql_val) and pd.isna(fabric_val):
                    continue
                elif pd.isna(sql_val) or pd.isna(fabric_val):
                    differences.append(f"{stat_name}: SQL={sql_val}, Fabric={fabric_val}")
                elif isinstance(sql_val, (int, float)) and isinstance(fabric_val, (int, float)):
                    if abs(sql_val - fabric_val) > abs(sql_val * self.config.tolerance_percentage / 100):
                        differences.append(f"{stat_name}: SQL={sql_val}, Fabric={fabric_val}, Diff={abs(sql_val - fabric_val)}")
                elif sql_val != fabric_val:
                    differences.append(f"{stat_name}: SQL={sql_val}, Fabric={fabric_val}")
        
        if differences:
            return {
                'type': 'statistical_mismatch',
                'column': column_name,
                'differences': differences,
                'description': f'Statistical differences found in column {column_name}'
            }
        
        return None