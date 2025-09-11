_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Comprehensive conversion tests for uspSemanticClaimTransactionMeasuresData SQL Server to Fabric migration
## *Version*: 3 
## *Updated on*: 
_____________________________________________

import pytest
import pandas as pd
import pyodbc
import time
import logging
import json
import hashlib
import os
import datetime
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"conversion_test_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ConversionTest")

@dataclass
class TestConfig:
    """Configuration for test execution"""
    sql_server_conn_string: str
    fabric_conn_string: str
    test_cases_path: str
    output_report_path: str
    parallel_execution: bool = False
    max_workers: int = 4
    timeout_seconds: int = 600  # 10 minutes timeout per test
    compare_performance: bool = True
    compare_results: bool = True
    detailed_logging: bool = True

@dataclass
class TestCase:
    """Individual test case definition"""
    id: str
    name: str
    description: str
    sql_server_query: str
    fabric_query: str
    parameters: Dict[str, Any] = None
    expected_row_count: Optional[int] = None
    expected_column_count: Optional[int] = None
    expected_hash: Optional[str] = None
    tags: List[str] = None

@dataclass
class TestResult:
    """Results of a test execution"""
    test_case: TestCase
    passed: bool
    execution_time_sql_server: float
    execution_time_fabric: float
    performance_diff_percent: float
    row_count_sql_server: int
    row_count_fabric: int
    column_count_sql_server: int
    column_count_fabric: int
    data_hash_sql_server: str
    data_hash_fabric: str
    data_matches: bool
    error_message: str = None
    warnings: List[str] = None

class ConversionTestHarness:
    """Test harness for SQL Server to Fabric conversion validation"""
    
    def __init__(self, config: TestConfig):
        self.config = config
        self.test_cases = []
        self.test_results = []
        self.sql_server_conn = None
        self.fabric_conn = None
        self.start_time = None
        self.end_time = None
        
    def load_test_cases(self):
        """Load test cases from configuration file"""
        try:
            with open(self.config.test_cases_path, 'r') as f:
                test_cases_data = json.load(f)
                
            for tc_data in test_cases_data:
                test_case = TestCase(
                    id=tc_data.get('id'),
                    name=tc_data.get('name'),
                    description=tc_data.get('description'),
                    sql_server_query=tc_data.get('sql_server_query'),
                    fabric_query=tc_data.get('fabric_query'),
                    parameters=tc_data.get('parameters'),
                    expected_row_count=tc_data.get('expected_row_count'),
                    expected_column_count=tc_data.get('expected_column_count'),
                    expected_hash=tc_data.get('expected_hash'),
                    tags=tc_data.get('tags', [])
                )
                self.test_cases.append(test_case)
                
            logger.info(f"Loaded {len(self.test_cases)} test cases")
        except Exception as e:
            logger.error(f"Failed to load test cases: {str(e)}")
            raise
    
    def connect_databases(self):
        """Establish connections to SQL Server and Fabric"""
        try:
            logger.info("Connecting to SQL Server...")
            self.sql_server_conn = pyodbc.connect(self.config.sql_server_conn_string)
            logger.info("SQL Server connection established")
            
            logger.info("Connecting to Fabric...")
            self.fabric_conn = pyodbc.connect(self.config.fabric_conn_string)
            logger.info("Fabric connection established")
        except Exception as e:
            logger.error(f"Failed to connect to databases: {str(e)}")
            raise
    
    def close_connections(self):
        """Close database connections"""
        if self.sql_server_conn:
            self.sql_server_conn.close()
            logger.info("SQL Server connection closed")
        
        if self.fabric_conn:
            self.fabric_conn.close()
            logger.info("Fabric connection closed")
    
    def calculate_data_hash(self, df: pd.DataFrame) -> str:
        """Calculate a hash of the dataframe for comparison"""
        # Convert dataframe to a consistent string representation and hash it
        df_str = df.to_csv(index=False)
        return hashlib.md5(df_str.encode()).hexdigest()
    
    def execute_query(self, connection, query: str, parameters: Dict[str, Any] = None) -> Tuple[pd.DataFrame, float]:
        """Execute a query and return results with execution time"""
        start_time = time.time()
        
        try:
            if parameters:
                # Format the query with parameters
                formatted_query = query
                for param_name, param_value in parameters.items():
                    placeholder = f"@{param_name}"
                    if isinstance(param_value, str):
                        formatted_query = formatted_query.replace(placeholder, f"'{param_value}'")
                    else:
                        formatted_query = formatted_query.replace(placeholder, str(param_value))
                
                df = pd.read_sql(formatted_query, connection)
            else:
                df = pd.read_sql(query, connection)
                
            execution_time = time.time() - start_time
            return df, execution_time
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def run_test_case(self, test_case: TestCase) -> TestResult:
        """Run a single test case and return results"""
        logger.info(f"Running test case: {test_case.id} - {test_case.name}")
        warnings = []
        
        try:
            # Execute SQL Server query
            logger.info(f"Executing SQL Server query for test case {test_case.id}")
            sql_server_df, sql_server_time = self.execute_query(
                self.sql_server_conn, test_case.sql_server_query, test_case.parameters
            )
            
            # Execute Fabric query
            logger.info(f"Executing Fabric query for test case {test_case.id}")
            fabric_df, fabric_time = self.execute_query(
                self.fabric_conn, test_case.fabric_query, test_case.parameters
            )
            
            # Calculate performance difference
            if sql_server_time > 0:
                perf_diff_percent = ((fabric_time - sql_server_time) / sql_server_time) * 100
            else:
                perf_diff_percent = 0
                warnings.append("SQL Server execution time was 0, could not calculate performance difference")
            
            # Compare result sets
            row_count_sql_server = len(sql_server_df)
            row_count_fabric = len(fabric_df)
            column_count_sql_server = len(sql_server_df.columns)
            column_count_fabric = len(fabric_df.columns)
            
            # Check if column counts match
            if column_count_sql_server != column_count_fabric:
                warnings.append(f"Column count mismatch: SQL Server={column_count_sql_server}, Fabric={column_count_fabric}")
                
                # Log the column differences
                sql_server_cols = set(sql_server_df.columns)
                fabric_cols = set(fabric_df.columns)
                missing_in_fabric = sql_server_cols - fabric_cols
                extra_in_fabric = fabric_cols - sql_server_cols
                
                if missing_in_fabric:
                    warnings.append(f"Columns missing in Fabric: {', '.join(missing_in_fabric)}")
                if extra_in_fabric:
                    warnings.append(f"Extra columns in Fabric: {', '.join(extra_in_fabric)}")
            
            # Check if row counts match
            if row_count_sql_server != row_count_fabric:
                warnings.append(f"Row count mismatch: SQL Server={row_count_sql_server}, Fabric={row_count_fabric}")
            
            # Calculate data hashes for comparison
            # First ensure we're comparing the same columns if they exist in both datasets
            common_columns = list(set(sql_server_df.columns).intersection(set(fabric_df.columns)))
            
            if common_columns:
                sql_server_hash = self.calculate_data_hash(sql_server_df[common_columns])
                fabric_hash = self.calculate_data_hash(fabric_df[common_columns])
                data_matches = sql_server_hash == fabric_hash
                
                if not data_matches:
                    warnings.append("Data content differs between SQL Server and Fabric results")
                    
                    # Sample some differences for logging
                    if self.config.detailed_logging and len(sql_server_df) > 0 and len(fabric_df) > 0:
                        try:
                            # Sort both dataframes if possible to align rows
                            if common_columns:
                                sql_server_df_sorted = sql_server_df[common_columns].sort_values(by=common_columns[0]).reset_index(drop=True)
                                fabric_df_sorted = fabric_df[common_columns].sort_values(by=common_columns[0]).reset_index(drop=True)
                                
                                # Compare the first few rows
                                sample_size = min(5, len(sql_server_df_sorted), len(fabric_df_sorted))
                                for i in range(sample_size):
                                    if not sql_server_df_sorted.iloc[i].equals(fabric_df_sorted.iloc[i]):
                                        diff = {col: (sql_server_df_sorted.iloc[i][col], fabric_df_sorted.iloc[i][col]) 
                                                for col in common_columns 
                                                if sql_server_df_sorted.iloc[i][col] != fabric_df_sorted.iloc[i][col]}
                                        warnings.append(f"Row {i} differences: {diff}")
                        except Exception as e:
                            warnings.append(f"Failed to generate detailed difference report: {str(e)}")
            else:
                sql_server_hash = self.calculate_data_hash(sql_server_df)
                fabric_hash = self.calculate_data_hash(fabric_df)
                data_matches = False
                warnings.append("No common columns found between SQL Server and Fabric results")
            
            # Check against expected values if provided
            if test_case.expected_row_count is not None and row_count_fabric != test_case.expected_row_count:
                warnings.append(f"Expected row count {test_case.expected_row_count}, but got {row_count_fabric}")
                
            if test_case.expected_column_count is not None and column_count_fabric != test_case.expected_column_count:
                warnings.append(f"Expected column count {test_case.expected_column_count}, but got {column_count_fabric}")
                
            if test_case.expected_hash is not None and fabric_hash != test_case.expected_hash:
                warnings.append(f"Expected data hash {test_case.expected_hash}, but got {fabric_hash}")
            
            # Determine if test passed
            passed = data_matches and not warnings
            
            return TestResult(
                test_case=test_case,
                passed=passed,
                execution_time_sql_server=sql_server_time,
                execution_time_fabric=fabric_time,
                performance_diff_percent=perf_diff_percent,
                row_count_sql_server=row_count_sql_server,
                row_count_fabric=row_count_fabric,
                column_count_sql_server=column_count_sql_server,
                column_count_fabric=column_count_fabric,
                data_hash_sql_server=sql_server_hash,
                data_hash_fabric=fabric_hash,
                data_matches=data_matches,
                warnings=warnings
            )
            
        except Exception as e:
            logger.error(f"Test case {test_case.id} failed with error: {str(e)}")
            return TestResult(
                test_case=test_case,
                passed=False,
                execution_time_sql_server=0,
                execution_time_fabric=0,
                performance_diff_percent=0,
                row_count_sql_server=0,
                row_count_fabric=0,
                column_count_sql_server=0,
                column_count_fabric=0,
                data_hash_sql_server="",
                data_hash_fabric="",
                data_matches=False,
                error_message=str(e),
                warnings=[]
            )