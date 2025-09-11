_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Comprehensive test cases and Pytest script to validate SQL Server-to-Fabric SQL conversion
## *Version*: 8 
## *Updated on*: 
_____________________________________________

# ==================== TEST EXECUTION REPORT ====================

def generate_test_report(test_results):
    """Generate a test execution report"""
    report = {
        "test_summary": {
            "total_tests": len(test_results),
            "passed": sum(1 for r in test_results if r.status == "PASSED"),
            "failed": sum(1 for r in test_results if r.status == "FAILED"),
            "total_execution_time": sum(r.execution_time for r in test_results)
        },
        "test_details": [
            {
                "test_name": r.test_name,
                "status": r.status,
                "execution_time": r.execution_time,
                "error_message": r.error_message,
                "performance_metrics": r.performance_metrics
            }
            for r in test_results
        ],
        "performance_summary": {
            "average_execution_time": sum(r.execution_time for r in test_results) / len(test_results) if test_results else 0,
            "performance_improvements": [
                {
                    "test_name": r.test_name,
                    "improvement_percentage": r.performance_metrics.get("improvement_percentage", 0) if r.performance_metrics else 0
                }
                for r in test_results if r.performance_metrics and "improvement_percentage" in r.performance_metrics
            ]
        }
    }
    
    return report

def save_test_report(report, filename="test_execution_report.json"):
    """Save test report to a file"""
    with open(filename, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"Test report saved to {filename}")

# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    """Main execution for running tests directly"""
    # Setup test framework
    framework = SQLConversionTestFramework()
    framework.setup_connections()
    
    # Run tests
    pytest.main(["-v"])
    
    # Generate and save test report
    report = generate_test_report(framework.test_results)
    save_test_report(report)
    
    # Cleanup
    framework.teardown_connections()