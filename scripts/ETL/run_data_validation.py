import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pathlib import Path
from datetime import datetime

def run_data_quality_validation():
    """Run data quality validation on all tables"""
    
    print("RUNNING DATA QUALITY VALIDATION")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get context
    context = gx.get_context()
    
    # Validate schools
    print("\nValidating schools table...")
    
    schools_batch = RuntimeBatchRequest(
        datasource_name="uk_education_postgres",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="schools_validation",
        runtime_parameters={"query": "SELECT * FROM schools"},
        batch_identifiers={"default_identifier_name": f"schools_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
    )
    
    schools_validator = context.get_validator(
        batch_request=schools_batch,
        expectation_suite_name="schools_quality"
    )
    
    schools_results = schools_validator.validate()
    
    print(f"  Status: {'PASSED' if schools_results.success else 'FAILED'}")
    print(f"  Success Rate: {schools_results.statistics['success_percent']:.1f}%")
    print(f"  Expectations: {schools_results.statistics['successful_expectations']}/{schools_results.statistics['evaluated_expectations']}")
    
    # Validate performance
    print("\nValidating school_performance table...")
    
    perf_batch = RuntimeBatchRequest(
        datasource_name="uk_education_postgres",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="performance_validation",
        runtime_parameters={"query": "SELECT * FROM school_performance"},
        batch_identifiers={"default_identifier_name": f"performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
    )
    
    perf_validator = context.get_validator(
        batch_request=perf_batch,
        expectation_suite_name="performance_quality"
    )
    
    perf_results = perf_validator.validate()
    
    print(f"  Status: {'PASSED' if perf_results.success else 'FAILED'}")
    print(f"  Success Rate: {perf_results.statistics['success_percent']:.1f}%")
    print(f"  Expectations: {perf_results.statistics['successful_expectations']}/{perf_results.statistics['evaluated_expectations']}")
    
    # Build updated data docs
    print("\nUpdating data documentation...")
    context.build_data_docs()
    
    docs_path = Path("great_expectations/uncommitted/data_docs/local_site/index.html")
    
    # Summary
    print("\n" + "=" * 50)
    print("VALIDATION COMPLETE")
    print("=" * 50)
    
    all_passed = schools_results.success and perf_results.success
    
    if all_passed:
        print("STATUS: ALL CHECKS PASSED")
    else:
        print("STATUS: SOME CHECKS FAILED")
        print("\nFailed Expectations:")
        
        for result in schools_results.results:
            if not result.success:
                print(f"  Schools - {result.expectation_config.expectation_type}")
        
        for result in perf_results.results:
            if not result.success:
                print(f"  Performance - {result.expectation_config.expectation_type}")
    
    if docs_path.exists():
        print(f"\nView detailed report: file:///{docs_path.absolute()}")
    
    return all_passed

if __name__ == "__main__":
    try:
        success = run_data_quality_validation()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\nValidation failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)