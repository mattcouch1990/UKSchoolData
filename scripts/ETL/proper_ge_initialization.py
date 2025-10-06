import subprocess
import sys
from pathlib import Path
import shutil

def initialize_ge_project_properly():
    """Properly initialize a GE project with file-backed storage"""
    
    print("PROPERLY INITIALIZING GREAT EXPECTATIONS PROJECT")
    print("=" * 50)
    
    # Clean slate
    for dir_name in ["great_expectations", "gx"]:
        if Path(dir_name).exists():
            print(f"Removing {dir_name}...")
            shutil.rmtree(dir_name)
    
    print("\nInitializing GE project using CLI...")
    
    # Use the GE CLI to properly initialize the project
    # This creates the correct directory structure
    result = subprocess.run(
        ["great_expectations", "init"],
        input="n\n",  # Say "no" to viewing docs
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"CLI initialization output:\n{result.stdout}")
        print(f"CLI initialization errors:\n{result.stderr}")
        
        # Fallback: manually create the directory structure
        print("\nCLI failed, creating directory structure manually...")
        
        ge_dir = Path("great_expectations")
        ge_dir.mkdir(exist_ok=True)
        
        # Create required subdirectories
        (ge_dir / "expectations").mkdir(exist_ok=True)
        (ge_dir / "checkpoints").mkdir(exist_ok=True)
        (ge_dir / "plugins").mkdir(exist_ok=True)
        (ge_dir / "uncommitted").mkdir(exist_ok=True)
        (ge_dir / "uncommitted" / "validations").mkdir(exist_ok=True)
        (ge_dir / "uncommitted" / "data_docs").mkdir(exist_ok=True)
        (ge_dir / "uncommitted" / "data_docs" / "local_site").mkdir(exist_ok=True)
        
        # Create minimal config file
        config_content = """# Welcome to Great Expectations!
config_version: 3.0
datasources: {}
stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/
  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/
  evaluation_parameter_store:
    class_name: EvaluationParameterStore
  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: checkpoints/
expectations_store_name: expectations_store
validations_store_name: validations_store
evaluation_parameter_store_name: evaluation_parameter_store
checkpoint_store_name: checkpoint_store
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: true
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
anonymous_usage_statistics:
  enabled: true
  data_context_id: 00000000-0000-0000-0000-000000000000
config_variables_file_path: uncommitted/config_variables.yml
plugins_directory: plugins/
"""
        
        config_file = ge_dir / "great_expectations.yml"
        with open(config_file, 'w') as f:
            f.write(config_content)
        
        # Create empty config_variables file
        config_vars = ge_dir / "uncommitted" / "config_variables.yml"
        config_vars.touch()
        
        print("Directory structure created manually")
    else:
        print("GE project initialized via CLI")
    
    # Verify the project exists
    import great_expectations as gx
    
    print("\nVerifying project initialization...")
    context = gx.get_context()
    
    print(f"Context root directory: {context.root_directory}")
    
    if context.root_directory is None:
        print("ERROR: Context still has no root directory!")
        return False
    
    print(f"Root directory exists: {Path(context.root_directory).exists()}")
    
    # Now add datasource and suites
    print("\nAdding datasource...")
    
    datasource_config = {
        "name": "uk_education_postgres",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": "postgresql://uk_edu_user:TopShark_1990!@localhost:5432/uk_education_analytics"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"]
            }
        }
    }
    
    context.add_datasource(**datasource_config)
    print("Datasource added")
    
    # Create expectation suites
    from great_expectations.core.batch import RuntimeBatchRequest
    
    print("\nCreating schools expectation suite...")
    
    schools_suite = context.create_expectation_suite("schools_quality", overwrite_existing=True)
    
    schools_batch = RuntimeBatchRequest(
        datasource_name="uk_education_postgres",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="schools",
        runtime_parameters={"query": "SELECT * FROM schools LIMIT 100"},
        batch_identifiers={"default_identifier_name": "schools_sample"}
    )
    
    schools_validator = context.get_validator(
        batch_request=schools_batch,
        expectation_suite_name="schools_quality"
    )
    
    schools_validator.expect_column_to_exist("urn")
    schools_validator.expect_column_values_to_not_be_null("urn")
    schools_validator.expect_column_values_to_be_unique("urn")
    schools_validator.expect_column_values_to_be_between("urn", min_value=100000, max_value=9999999)
    schools_validator.expect_column_to_exist("school_name")
    schools_validator.expect_column_values_to_not_be_null("school_name")
    
    schools_validator.save_expectation_suite(discard_failed_expectations=False)
    print("Schools suite saved")
    
    print("\nCreating performance expectation suite...")
    
    perf_suite = context.create_expectation_suite("performance_quality", overwrite_existing=True)
    
    perf_batch = RuntimeBatchRequest(
        datasource_name="uk_education_postgres",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="school_performance",
        runtime_parameters={"query": "SELECT * FROM school_performance LIMIT 100"},
        batch_identifiers={"default_identifier_name": "performance_sample"}
    )
    
    perf_validator = context.get_validator(
        batch_request=perf_batch,
        expectation_suite_name="performance_quality"
    )
    
    perf_validator.expect_column_to_exist("urn")
    perf_validator.expect_column_values_to_not_be_null("urn")
    perf_validator.expect_column_to_exist("academic_year")
    perf_validator.expect_column_values_to_be_in_set("academic_year", ["2023-24", "2022-23", "2021-22"])
    perf_validator.expect_column_values_to_be_between("attainment_8_score", min_value=0, max_value=90, mostly=0.90)
    perf_validator.expect_column_values_to_be_between("progress_8_score", min_value=-3, max_value=3, mostly=0.90)
    
    perf_validator.save_expectation_suite(discard_failed_expectations=False)
    print("Performance suite saved")
    
    # Verify files exist
    print("\nVerifying expectation suite files...")
    
    exp_dir = Path(context.root_directory) / "expectations"
    exp_files = list(exp_dir.glob("*.json"))
    
    print(f"Found {len(exp_files)} expectation suite files:")
    for f in exp_files:
        print(f"  {f.name} ({f.stat().st_size} bytes)")
    
    # Verify suites can be loaded
    suites = context.list_expectation_suite_names()
    print(f"\nExpectation suites accessible: {suites}")
    
    print("\n" + "=" * 50)
    print("PROJECT INITIALIZATION COMPLETE")
    print("=" * 50)
    print(f"Root directory: {context.root_directory}")
    print(f"Suites created: {len(suites)}")
    print("\nYou can now run: python run_data_validation.py")
    
    return True

if __name__ == "__main__":
    try:
        initialize_ge_project_properly()
    except Exception as e:
        print(f"\nInitialization failed: {e}")
        import traceback
        traceback.print_exc()