# -*- coding: utf-8 -*-
"""
Metadata:
    Author: AAVA
    Created on: 
    Description: Comprehensive reconciliation test for uspSemanticClaimTransactionMeasuresData between SQL Server and Fabric
    Version: 1
    Updated on: 
"""

import pandas as pd
import numpy as np
import pyodbc
import time
import hashlib
import logging
import datetime
from typing import Dict, List, Tuple, Any, Optional
from configparser import ConfigParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("recon_test_uspSemanticClaimTransactionMeasuresData.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ReconTest")

class SQLServerToFabricReconTest:
    """
    A class to perform reconciliation testing between SQL Server and Fabric
    for the uspSemanticClaimTransactionMeasuresData stored procedure.
    """
    
    def __init__(self, config_file: str = 'config.ini'):
        """
        Initialize the reconciliation test with configuration settings.
        
        Args:
            config_file: Path to the configuration file
        """
        self.config = ConfigParser()
        self.config.read(config_file)
        
        # Connection strings
        self.sql_server_conn_str = self._get_sql_server_conn_str()
        self.fabric_conn_str = self._get_fabric_conn_str()
        
        # Test parameters
        self.proc_name = "uspSemanticClaimTransactionMeasuresData"
        self.test_cases = self._define_test_cases()
        
        logger.info(f"Initialized reconciliation test for {self.proc_name}")
    
    def _get_sql_server_conn_str(self) -> str:
        """
        Build SQL Server connection string from config.
        
        Returns:
            SQL Server connection string
        """
        try:
            server = self.config.get('SQLServer', 'server')
            database = self.config.get('SQLServer', 'database')
            username = self.config.get('SQLServer', 'username')
            password = self.config.get('SQLServer', 'password')
            
            return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        except Exception as e:
            logger.error(f"Error building SQL Server connection string: {str(e)}")
            raise
    
    def _get_fabric_conn_str(self) -> str:
        """
        Build Fabric connection string from config.
        
        Returns:
            Fabric connection string
        """
        try:
            server = self.config.get('Fabric', 'server')
            database = self.config.get('Fabric', 'database')
            username = self.config.get('Fabric', 'username')
            password = self.config.get('Fabric', 'password')
            
            return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        except Exception as e:
            logger.error(f"Error building Fabric connection string: {str(e)}")
            raise
    
    def _define_test_cases(self) -> List[Dict[str, Any]]:
        """
        Define test cases for the stored procedure.
        
        Returns:
            List of test case dictionaries
        """
        return [
            {
                "name": "Special Date Conversion Test",
                "params": {
                    "@pJobStartDateTime": "'01/01/1900'",
                    "@pJobEndDateTime": "'" + datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S') + "'"
                },
                "description": "Test special case where @pJobStartDateTime = '01/01/1900' is converted to '01/01/1700'"
            },
            {
                "name": "Standard Execution Test",
                "params": {
                    "@pJobStartDateTime": f"'{datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')}'",
                    "@pJobEndDateTime": "'" + (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%m/%d/%Y %H:%M:%S') + "'"
                },
                "description": "Test standard execution with current date"
            },
            {
                "name": "Historical Data Test",
                "params": {
                    "@pJobStartDateTime": "'01/01/2023'",
                    "@pJobEndDateTime": "'" + datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S') + "'"
                },
                "description": "Test with historical date to validate change tracking"
            }
        ]
    
    def execute_stored_procedure(self, connection_string: str, proc_name: str, params: Dict[str, str]) -> None:
        """
        Execute the stored procedure with the given parameters.
        
        Args:
            connection_string: Database connection string
            proc_name: Name of the stored procedure
            params: Dictionary of parameter names and values
        """
        param_string = ", ".join([f"{k}={v}" for k, v in params.items()])
        exec_statement = f"EXEC {proc_name} {param_string}"
        
        logger.info(f"Executing: {exec_statement}")
        
        try:
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(exec_statement)
                conn.commit()
            logger.info("Stored procedure executed successfully")
        except Exception as e:
            logger.error(f"Error executing stored procedure: {str(e)}")
            raise
    
    def validate_special_date_conversion(self, sql_server_conn: pyodbc.Connection, fabric_conn: pyodbc.Connection) -> bool:
        """
        Validate that '01/01/1900' is correctly converted to '01/01/1700' in Fabric.
        
        Args:
            sql_server_conn: SQL Server connection
            fabric_conn: Fabric connection
            
        Returns:
            True if validation passes, False otherwise
        """
        query = """
        SELECT TOP 10 * 
        FROM YourAuditTable 
        WHERE CONVERT(VARCHAR, JobStartDateTime, 101) = '01/01/1700'
        """
        
        try:
            # Check SQL Server
            sql_cursor = sql_server_conn.cursor()
            sql_cursor.execute(query.replace('01/01/1700', '01/01/1900'))
            sql_results = sql_cursor.fetchall()
            
            # Check Fabric
            fabric_cursor = fabric_conn.cursor()
            fabric_cursor.execute(query)
            fabric_results = fabric_cursor.fetchall()
            
            # Compare row counts
            if len(sql_results) != len(fabric_results):
                logger.warning(f"Date conversion validation failed: SQL Server has {len(sql_results)} rows, Fabric has {len(fabric_results)} rows")
                return False
                
            logger.info("Special date conversion validation passed")
            return True
        except Exception as e:
            logger.error(f"Error during special date conversion validation: {str(e)}")
            return False
    
    def validate_temp_tables(self, sql_server_conn: pyodbc.Connection, fabric_conn: pyodbc.Connection) -> bool:
        """
        Validate that temporary tables are handled correctly in both systems.
        
        Args:
            sql_server_conn: SQL Server connection
            fabric_conn: Fabric connection
            
        Returns:
            True if validation passes, False otherwise
        """
        # This is a simplified approach - in reality, you would need to check specific temp tables
        # created by the stored procedure, which might require modifying the proc to output this info
        
        try:
            # Check if the final tables have the same structure
            validation_query = """
            SELECT COUNT(*) as row_count 
            FROM Semantic.ClaimTransactionMeasures
            """
            
            # SQL Server count
            sql_cursor = sql_server_conn.cursor()
            sql_cursor.execute(validation_query)
            sql_count = sql_cursor.fetchone()[0]
            
            # Fabric count
            fabric_cursor = fabric_conn.cursor()
            fabric_cursor.execute(validation_query)
            fabric_count = fabric_cursor.fetchone()[0]
            
            if sql_count != fabric_count:
                logger.warning(f"Temp table validation failed: SQL Server has {sql_count} rows, Fabric has {fabric_count} rows")
                return False
                
            logger.info("Temporary tables validation passed")
            return True
        except Exception as e:
            logger.error(f"Error during temp tables validation: {str(e)}")
            return False
    
    def validate_hash_values(self, sql_server_conn: pyodbc.Connection, fabric_conn: pyodbc.Connection) -> bool:
        """
        Validate that hash values for change tracking are calculated correctly.
        
        Args:
            sql_server_conn: SQL Server connection
            fabric_conn: Fabric connection
            
        Returns:
            True if validation passes, False otherwise
        """
        hash_validation_query = """
        SELECT TOP 100 
            FactClaimTransactionLineWCKey, 
            HashValue
        FROM Semantic.ClaimTransactionMeasures
        ORDER BY FactClaimTransactionLineWCKey
        """
        
        try:
            # Get SQL Server hash values
            sql_cursor = sql_server_conn.cursor()
            sql_cursor.execute(hash_validation_query)
            sql_hashes = {row.FactClaimTransactionLineWCKey: row.HashValue for row in sql_cursor.fetchall()}
            
            # Get Fabric hash values
            fabric_cursor = fabric_conn.cursor()
            fabric_cursor.execute(hash_validation_query)
            fabric_hashes = {row.FactClaimTransactionLineWCKey: row.HashValue for row in fabric_cursor.fetchall()}
            
            # Compare hash values
            mismatches = 0
            for key in sql_hashes:
                if key in fabric_hashes:
                    if sql_hashes[key] != fabric_hashes[key]:
                        mismatches += 1
                        logger.warning(f"Hash mismatch for FactClaimTransactionLineWCKey {key}: SQL={sql_hashes[key]}, Fabric={fabric_hashes[key]}")
            
            if mismatches > 0:
                logger.warning(f"Hash validation failed: {mismatches} mismatches found")
                return False
                
            logger.info("Hash values validation passed")
            return True
        except Exception as e:
            logger.error(f"Error during hash values validation: {str(e)}")
            return False
    
    def validate_insert_update_logic(self, sql_server_conn: pyodbc.Connection, fabric_conn: pyodbc.Connection) -> bool:
        """
        Validate that records are correctly identified as inserted vs updated.
        
        Args:
            sql_server_conn: SQL Server connection
            fabric_conn: Fabric connection
            
        Returns:
            True if validation passes, False otherwise
        """
        # Query to check inserted vs updated counts
        validation_query = """
        SELECT 
            COUNT(CASE WHEN InsertUpdates = 1 THEN 1 END) as inserted_count,
            COUNT(CASE WHEN InsertUpdates = 0 THEN 1 END) as updated_count
        FROM Semantic.ClaimTransactionMeasures
        """
        
        try:
            # SQL Server counts
            sql_cursor = sql_server_conn.cursor()
            sql_cursor.execute(validation_query)
            sql_result = sql_cursor.fetchone()
            sql_inserted = sql_result[0]
            sql_updated = sql_result[1]
            
            # Fabric counts
            fabric_cursor = fabric_conn.cursor()
            fabric_cursor.execute(validation_query)
            fabric_result = fabric_cursor.fetchone()
            fabric_inserted = fabric_result[0]
            fabric_updated = fabric_result[1]
            
            # Compare counts
            if sql_inserted != fabric_inserted or sql_updated != fabric_updated:
                logger.warning(f"Insert/Update validation failed: SQL Server has {sql_inserted} inserts and {sql_updated} updates, "
                              f"Fabric has {fabric_inserted} inserts and {fabric_updated} updates")
                return False
                
            logger.info("Insert/Update logic validation passed")
            return True
        except Exception as e:
            logger.error(f"Error during insert/update logic validation: {str(e)}")
            return False
    
    def validate_aggregation(self, sql_server_conn: pyodbc.Connection, fabric_conn: pyodbc.Connection) -> bool:
        """
        Validate that measures are properly aggregated from the Rules.SemanticLayerMetaData table.
        
        Args:
            sql_server_conn: SQL Server connection
            fabric_conn: Fabric connection
            
        Returns:
            True if validation passes, False otherwise
        """
        # Query to check aggregated measures
        validation_query = """
        SELECT 
            'NetPaid' as MeasureType,
            SUM(NetPaidIndemnity + NetPaidMedical + NetPaidExpense) as TotalValue,
            COUNT(*) as MeasureCount
        FROM Semantic.ClaimTransactionMeasures
        UNION ALL
        SELECT 
            'NetIncurred' as MeasureType,
            SUM(NetIncurredIndemnity + NetIncurredMedical + NetIncurredExpense) as TotalValue,
            COUNT(*) as MeasureCount
        FROM Semantic.ClaimTransactionMeasures
        UNION ALL
        SELECT 
            'Reserves' as MeasureType,
            SUM(ReservesIndemnity + ReservesMedical + ReservesExpense) as TotalValue,
            COUNT(*) as MeasureCount
        FROM Semantic.ClaimTransactionMeasures
        """
        
        try:
            # SQL Server aggregations
            sql_cursor = sql_server_conn.cursor()
            sql_cursor.execute(validation_query)
            sql_results = {}
            for row in sql_cursor.fetchall():
                sql_results[row.MeasureType] = (row.TotalValue, row.MeasureCount)
            
            # Fabric aggregations
            fabric_cursor = fabric_conn.cursor()
            fabric_cursor.execute(validation_query)
            fabric_results = {}
            for row in fabric_cursor.fetchall():
                fabric_results[row.MeasureType] = (row.TotalValue, row.MeasureCount)
            
            # Compare aggregations
            mismatches = 0
            for measure_type in sql_results:
                if measure_type in fabric_results:
                    sql_total, sql_count = sql_results[measure_type]
                    fabric_total, fabric_count = fabric_results[measure_type]
                    
                    # Allow for small floating point differences
                    if sql_count != fabric_count or abs(sql_total - fabric_total) > 0.001:
                        mismatches += 1
                        logger.warning(f"Aggregation mismatch for MeasureType {measure_type}: "
                                     f"SQL={sql_total} ({sql_count} records), "
                                     f"Fabric={fabric_total} ({fabric_count} records)")
                else:
                    mismatches += 1
                    logger.warning(f"MeasureType {measure_type} exists in SQL Server but not in Fabric")
            
            # Check for measure types in Fabric but not in SQL Server
            for measure_type in fabric_results:
                if measure_type not in sql_results:
                    mismatches += 1
                    logger.warning(f"MeasureType {measure_type} exists in Fabric but not in SQL Server")
            
            if mismatches > 0:
                logger.warning(f"Aggregation validation failed: {mismatches} mismatches found")
                return False
                
            logger.info("Aggregation validation passed")
            return True
        except Exception as e:
            logger.error(f"Error during aggregation validation: {str(e)}")
            return False
    
    def validate_joins(self, sql_server_conn: pyodbc.Connection, fabric_conn: pyodbc.Connection) -> bool:
        """
        Validate that data is correctly joined from multiple tables.
        
        Args:
            sql_server_conn: SQL Server connection
            fabric_conn: Fabric connection
            
        Returns:
            True if validation passes, False otherwise
        """
        # Query to check join integrity
        validation_query = """
        SELECT 
            ct.FactClaimTransactionLineWCKey,
            ct.ClaimWCKey,
            ct.PolicyWCKey,
            COUNT(*) as RelatedMeasureCount
        FROM Semantic.ClaimTransactionMeasures ct
        JOIN Semantic.ClaimDescriptors c ON ct.ClaimWCKey = c.ClaimWCKey
        JOIN Semantic.PolicyDescriptors p ON ct.PolicyWCKey = p.PolicyWCKey
        GROUP BY ct.FactClaimTransactionLineWCKey, ct.ClaimWCKey, ct.PolicyWCKey
        ORDER BY ct.FactClaimTransactionLineWCKey
        OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY
        """
        
        try:
            # SQL Server join results
            sql_cursor = sql_server_conn.cursor()
            sql_cursor.execute(validation_query)
            sql_results = []
            for row in sql_cursor.fetchall():
                sql_results.append((row.FactClaimTransactionLineWCKey, row.ClaimWCKey, row.PolicyWCKey, row.RelatedMeasureCount))
            
            # Fabric join results
            fabric_cursor = fabric_conn.cursor()
            fabric_cursor.execute(validation_query)
            fabric_results = []
            for row in fabric_cursor.fetchall():
                fabric_results.append((row.FactClaimTransactionLineWCKey, row.ClaimWCKey, row.PolicyWCKey, row.RelatedMeasureCount))
            
            # Compare results
            if len(sql_results) != len(fabric_results):
                logger.warning(f"Join validation failed: SQL Server has {len(sql_results)} rows, Fabric has {len(fabric_results)} rows")
                return False
            
            mismatches = 0
            for i in range(len(sql_results)):
                if sql_results[i] != fabric_results[i]:
                    mismatches += 1
                    logger.warning(f"Join mismatch at row {i}: SQL={sql_results[i]}, Fabric={fabric_results[i]}")
                    
                    # Limit the number of reported mismatches
                    if mismatches >= 10:
                        logger.warning("Too many mismatches, stopping comparison")
                        break
            
            if mismatches > 0:
                logger.warning(f"Join validation failed: {mismatches} mismatches found")
                return False
                
            logger.info("Join validation passed")
            return True
        except Exception as e:
            logger.error(f"Error during join validation: {str(e)}")
            return False
    
    def run_test_case(self, test_case: Dict[str, Any]) -> Dict[str, bool]:
        """
        Run a single test case and validate results.
        
        Args:
            test_case: Test case dictionary
            
        Returns:
            Dictionary of validation results
        """
        logger.info(f"Running test case: {test_case['name']}")
        logger.info(f"Description: {test_case['description']}")
        
        try:
            # Execute stored procedure on SQL Server
            self.execute_stored_procedure(self.sql_server_conn_str, self.proc_name, test_case['params'])
            
            # Execute stored procedure on Fabric
            self.execute_stored_procedure(self.fabric_conn_str, self.proc_name, test_case['params'])
            
            # Wait for any asynchronous processes to complete
            time.sleep(5)
            
            # Run validations
            with pyodbc.connect(self.sql_server_conn_str) as sql_conn, \
                 pyodbc.connect(self.fabric_conn_str) as fabric_conn:
                
                validation_results = {
                    "special_date_conversion": self.validate_special_date_conversion(sql_conn, fabric_conn),
                    "temp_tables": self.validate_temp_tables(sql_conn, fabric_conn),
                    "hash_values": self.validate_hash_values(sql_conn, fabric_conn),
                    "insert_update_logic": self.validate_insert_update_logic(sql_conn, fabric_conn),
                    "aggregation": self.validate_aggregation(sql_conn, fabric_conn),
                    "joins": self.validate_joins(sql_conn, fabric_conn)
                }
                
                return validation_results
        except Exception as e:
            logger.error(f"Error running test case {test_case['name']}: {str(e)}")
            return {
                "special_date_conversion": False,
                "temp_tables": False,
                "hash_values": False,
                "insert_update_logic": False,
                "aggregation": False,
                "joins": False
            }
    
    def run_all_tests(self) -> Dict[str, Dict[str, bool]]:
        """
        Run all test cases.
        
        Returns:
            Dictionary of test case names and their validation results
        """
        logger.info(f"Starting reconciliation tests for {self.proc_name}")
        
        all_results = {}
        for test_case in self.test_cases:
            all_results[test_case['name']] = self.run_test_case(test_case)
            
        return all_results
    
    def generate_report(self, results: Dict[str, Dict[str, bool]]) -> str:
        """
        Generate a report of test results.
        
        Args:
            results: Dictionary of test case names and their validation results
            
        Returns:
            Report string
        """
        report = ["\n===== RECONCILIATION TEST REPORT ====="]
        report.append(f"Stored Procedure: {self.proc_name}")
        report.append(f"Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("\n")
        
        all_passed = True
        
        for test_name, validations in results.items():
            test_passed = all(validations.values())
            all_passed = all_passed and test_passed
            
            status = "PASSED" if test_passed else "FAILED"
            report.append(f"Test Case: {test_name} - {status}")
            
            for validation_name, passed in validations.items():
                status = "PASSED" if passed else "FAILED"
                report.append(f"  - {validation_name}: {status}")
            
            report.append("\n")
        
        overall_status = "PASSED" if all_passed else "FAILED"
        report.append(f"Overall Status: {overall_status}")
        
        return "\n".join(report)


def main():
    """
    Main function to run the reconciliation test.
    """
    try:
        # Initialize the test
        recon_test = SQLServerToFabricReconTest()
        
        # Run all tests
        results = recon_test.run_all_tests()
        
        # Generate and print report
        report = recon_test.generate_report(results)
        print(report)
        
        # Log the report
        logger.info(report)
        
        # Write report to file
        with open("uspSemanticClaimTransactionMeasuresData_recon_report.txt", "w") as f:
            f.write(report)
        
        # Return success code
        return 0
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)