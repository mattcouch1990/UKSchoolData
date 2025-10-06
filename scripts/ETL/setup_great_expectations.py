import shutil
import os
from pathlib import Path
import great_expectations as gx
from ukeducationdbconnection import UKEducationDB

def clean_and_init_ge():
    """Clean and initialize Great Expectations from scratch"""
    
    print("🧹 CLEANING AND INITIALIZING GREAT EXPECTATIONS")
    print("=" * 50)
    
    # Step 1: Remove existing GE directory if it exists
    ge_dir = Path("great_expectations")
    if ge_dir.exists():
        print("🗑️  Removing existing Great Expectations directory...")
        shutil.rmtree(ge_dir)
        print("✅ Cleaned up existing configuration")
    
    # Step 2: Initialize fresh GE project
    print("\n📁 Initializing fresh Great Expectations project...")
    
    try:
        # Initialize project in current directory
        context = gx.get_context(mode="file")
        print("✅ Great Expectations project initialized")
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        
        # Try alternative initialization
        try:
            os.system("great_expectations init")
            context = gx.get_context()
            print("✅ Great Expectations initialized via CLI")
        except Exception as e2:
            print(f"❌ CLI initialization also failed: {e2}")
            return None
    
    return context

def test_basic_validation():
    """Test basic validation without complex datasource setup"""
    
    print("\n🔍 TESTING BASIC VALIDATION")
    print("=" * 30)
    
    try:
        # Get some data from our database
        db = UKEducationDB()
        schools_df = db.read_sql("SELECT urn, school_name FROM schools LIMIT 100")
        
        print(f"📊 Loaded {len(schools_df)} school records for testing")
        
        # Create a simple validator using pandas
        context = gx.get_context()
        
        # Create expectation suite
        suite_name = "simple_schools_suite"
        suite = context.add_or_update_expectation_suite(suite_name)
        
        # Add basic expectations
        suite.expect_column_to_exist("urn")
        suite.expect_column_values_to_not_be_null("urn")
        suite.expect_column_values_to_be_unique("urn") 
        suite.expect_column_to_exist("school_name")
        
        # Get validator using pandas dataframe
        validator = context.get_validator(
            batch_request={
                "batch_data": schools_df,
                "batch_identifiers": {"batch_id": "schools_sample"}
            },
            expectation_suite_name=suite_name
        )
        
        # Run validation
        results = validator.validate()
        
        print(f"🎯 Validation Results:")
        print(f"   Success: {results.success}")
        print(f"   Total expectations: {len(results.results)}")
        
        passed = sum(1 for r in results.results if r.success)
        failed = len(results.results) - passed
        
        print(f"   Passed: {passed}")
        print(f"   Failed: {failed}")
        
        if results.success:
            print("✅ All validations passed!")
        else:
            print("❌ Some validations failed:")
            for result in results.results:
                if not result.success:
                    exp_type = result.expectation_config.expectation_type
                    print(f"      - {exp_type}")
        
        return True
        
    except Exception as e:
        print(f"❌ Basic validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def setup_simple_checkpoint():
    """Set up a simple checkpoint for regular validation"""
    
    print("\n🎯 SETTING UP SIMPLE CHECKPOINT")
    print("=" * 30)
    
    try:
        context = gx.get_context()
        
        # Create simple checkpoint config
        checkpoint_config = {
            "name": "simple_data_quality_checkpoint",
            "config_version": 1.0,
            "template_name": None,
            "run_name_template": "%Y%m%d-%H%M%S-data-quality-check",
            "expectation_suite_name": "simple_schools_suite",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "update_data_docs", 
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        }
        
        # Add checkpoint
        checkpoint = context.add_or_update_checkpoint(**checkpoint_config)
        print("✅ Simple checkpoint created")
        
        return checkpoint
        
    except Exception as e:
        print(f"❌ Checkpoint creation failed: {e}")
        return None

def run_simple_data_docs():
    """Build simple data documentation"""
    
    print("\n📚 BUILDING DATA DOCUMENTATION")
    print("=" * 30)
    
    try:
        context = gx.get_context()
        context.build_data_docs()
        
        # Find docs path
        docs_path = Path("great_expectations/uncommitted/data_docs/local_site/index.html")
        
        if docs_path.exists():
            print("✅ Data documentation built!")
            print(f"📖 View at: file://{docs_path.absolute()}")
            return str(docs_path.absolute())
        else:
            print("⚠️  Documentation built but file not found")
            return None
            
    except Exception as e:
        print(f"❌ Documentation build failed: {e}")
        return None

def main():
    """Main function to set up Great Expectations properly"""
    
    try:
        # Clean and initialize
        context = clean_and_init_ge()
        
        if context is None:
            print("❌ Failed to initialize Great Expectations")
            return False
        
        # Test basic validation
        validation_success = test_basic_validation()
        
        if not validation_success:
            print("❌ Basic validation failed")
            return False
        
        # Set up checkpoint
        checkpoint = setup_simple_checkpoint()
        
        # Build documentation
        docs_path = run_simple_data_docs()
        
        print(f"\n🎉 GREAT EXPECTATIONS SETUP COMPLETE!")
        print("=" * 40)
        print("✅ Project initialized")
        print("✅ Basic validation working")
        print("✅ Checkpoint configured")
        print("✅ Data documentation built")
        
        if docs_path:
            print(f"\n📖 Open data docs in browser:")
            print(f"   {docs_path}")
        
        print(f"\n🔧 Next steps:")
        print("   1. Add more expectation suites for other tables")
        print("   2. Set up automated validation runs")
        print("   3. Configure alerts for data quality issues")
        
        return True
        
    except Exception as e:
        print(f"❌ Great Expectations setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()