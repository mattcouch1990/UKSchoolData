import great_expectations as gx
from great_expectations.datasource.fluent import PostgresDatasource
from pathlib import Path
import shutil

def setup_ge_018():
    """Set up Great Expectations 0.18 with modern fluent API"""
    
    print("SETTING UP GREAT EXPECTATIONS 0.18")
    print("=" * 50)
    print(f"GE Version: {gx.__version__}")
    
    # Clean slate
    for dir_name in ["great_expectations", "gx"]:
        if Path(dir_name).exists():
            print(f"Removing {dir_name}...")
            shutil.rmtree(dir_name)
    
    # Create file data context (persistent)
    print("\nInitializing file-backed data context...")
    
    context = gx.get_context(mode="file")
    
    print(f"Context type: {type(context).__name__}")
    print(f"Context root: {context.root_directory}")
    
    if context.root_directory is None:
        print("ERROR: Context has no root directory!")
        return False
    
    # Add PostgreSQL datasource using fluent API
    print("\nAdding PostgreSQL datasource...")
    
    try:
        datasource = context.sources.add_postgres(
            name="uk_education_db",
            connection_string="postgresql://uk_edu_user:TopShark_1990!@localhost:5432/uk_education_analytics"
        )
        print(f"Datasource added: {datasource.name}")
    except Exception as e:
        print(f"Datasource creation failed: {e}")
        return False
    
    # Add table assets
    print("\nAdding table assets...")
    
    schools_asset = datasource.add_table_asset(
        name="schools",
        table_name="schools"
    )
    print("Schools asset added")
    
    performance_asset = datasource.add_table_asset(
        name="school_performance",
        table_name="school_performance"
    )
    print("Performance asset added")
    
    # Create expectation suites
    print("\nCreating expectation suites...")
    
    # Schools suite
    schools_suite = context.add_expectation_suite("schools_quality")
    
    schools_batch_request = schools_asset.build_batch_request()
    schools_validator = context.get_validator(
        batch_request=schools_batch_request,
        expectation_suite_name="schools_quality"
    )
    
    schools_validator.expect_column_to_exist("urn")
    schools_validator.expect_column_values_to_not_be_null("urn")
    schools_validator.expect_column_values_to_be_unique("urn")
    schools_validator.expect_column_values_to_be_between("urn", min_value=100000, max_value=9999999)
    schools_validator.expect_column_to_exist("school_name")
    schools_validator.expect_column_values_to_not_be_null("school_name")
    
    schools_validator.save_expectation_suite()
    print("Schools suite created and saved")
    
    # Performance suite
    perf_suite = context.add_expectation_suite("performance_quality")
    
    perf_batch_request = performance_asset.build_batch_request()
    perf_validator = context.get_validator(
        batch_request=perf_batch_request,
        expectation_suite_name="performance_quality"
    )
    
    perf_validator.expect_column_to_exist("urn")
    perf_validator.expect_column_values_to_not_be_null("urn")
    perf_validator.expect_column_to_exist("academic_year")
    perf_validator.expect_column_values_to_be_in_set("academic_year", ["2023-24", "2022-23", "2021-22"])
    perf_validator.expect_column_values_to_be_between("attainment_8_score", min_value=0, max_value=90, mostly=0.90)
    perf_validator.expect_column_values_to_be_between("progress_8_score", min_value=-3, max_value=3, mostly=0.90)
    
    perf_validator.save_expectation_suite()
    print("Performance suite created and saved")
    
    # Run validations
    print("\nRunning validations...")
    
    schools_results = schools_validator.validate()
    print(f"Schools: {'PASSED' if schools_results.success else 'FAILED'} ({schools_results.statistics['success_percent']:.1f}%)")
    
    perf_results = perf_validator.validate()
    print(f"Performance: {'PASSED' if perf_results.success else 'FAILED'} ({perf_results.statistics['success_percent']:.1f}%)")
    
    # Build data docs
    print("\nBuilding data docs...")
    context.build_data_docs()
    
    docs_path = Path(context.root_directory) / "uncommitted" / "data_docs" / "local_site" / "index.html"
    
    # Verify persistence
    print("\nVerifying file persistence...")
    
    exp_dir = Path(context.root_directory) / "expectations"
    exp_files = list(exp_dir.glob("*.json"))
    
    print(f"Expectation files on disk: {len(exp_files)}")
    for f in exp_files:
        print(f"  {f.name}")
    
    suites = context.list_expectation_suite_names()
    print(f"Suites accessible: {suites}")
    
    print("\n" + "=" * 50)
    print("SETUP COMPLETE")
    print("=" * 50)
    print(f"GE Version: {gx.__version__}")
    print(f"Root directory: {context.root_directory}")
    print(f"Suites: {len(suites)}")
    
    if docs_path.exists():
        print(f"Data docs: file:///{docs_path.absolute()}")
    
    print("\nTest persistence by running:")
    print("  python test_ge_persistence.py")
    
    return True

if __name__ == "__main__":
    try:
        setup_ge_018()
    except Exception as e:
        print(f"\nSetup failed: {e}")
        import traceback
        traceback.print_exc()