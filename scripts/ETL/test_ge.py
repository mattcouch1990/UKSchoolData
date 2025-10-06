from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
from ukeducationdbconnection import UKEducationDB
from pathlib import Path
import json

def test_ge_setup():
    """Test that Great Expectations is properly configured and working"""
    
    print("🧪 TESTING GREAT EXPECTATIONS SETUP")
    print("=" * 60)
    
    # Step 1: Check GE version and context
    print(f"\n1️⃣ Great Expectations Version: {gx.__version__}")
    
    try:
        context = gx.get_context()
        print("✅ Great Expectations context loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load context: {e}")
        return False
    
    # Step 2: Check datasources
    print("\n2️⃣ Checking configured datasources:")
    datasources = context.list_datasources()
    if datasources:
        for ds in datasources:
            # Handle different possible datasource structures
            name = ds.get('name', ds.get('datasource_name', 'Unknown'))
            ds_type = ds.get('class_name', ds.get('type', 'Unknown'))
            print(f"   ✅ {name} ({ds_type})")
            # Debug: show actual structure
            # print(f"      Structure: {list(ds.keys())}")
    else:
        print("   ⚠️  No datasources configured")
    
    # Step 3: Check expectation suites
    print("\n3️⃣ Checking expectation suites:")
    try:
        suites = context.list_expectation_suite_names()
        if suites:
            for suite in suites:
                print(f"   ✅ {suite}")
        else:
            print("   ⚠️  No expectation suites found")
    except Exception as e:
        print(f"   ❌ Error listing suites: {e}")
    
    # Step 4: Test database connection
    print("\n4️⃣ Testing database connection:")
    try:
        db = UKEducationDB()
        
        # Test connection using the class's method
        conn_info = db.test_connection()
        print(f"   ✅ Connected to database: {conn_info['database']}")
        print(f"   📊 PostgreSQL version: {conn_info['version']}")
        print(f"   📚 Tables in database: {conn_info['table_count']}")
        print(f"   🏫 School types: {conn_info['school_types']}")
        print(f"   ♿ SEN categories: {conn_info['sen_categories']}")
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        return False
    
    # Step 5: Run a simple validation
    print("\n5️⃣ Running validation on schools data:")
    try:
        # Use the pre-configured 'schools' data asset
        datasource_name = "uk_education_db"
        data_asset_name = "schools"
        
        print(f"   📊 Using data asset: {data_asset_name}")
        
        # Get the datasource
        datasource = context.get_datasource(datasource_name)
        print(f"   ✅ Datasource loaded: {datasource_name}")
        
        # Get the data asset
        data_asset = datasource.get_asset(data_asset_name)
        print(f"   ✅ Data asset loaded: {data_asset_name}")
        
        # Create a batch request for the schools table
        batch_request = data_asset.build_batch_request()
        
        # Get or create validator
        suite_name = "schools_quality"  # Use existing suite
        if suite_name in suites:
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            print(f"   ✅ Loaded existing suite: {suite_name}")
            
            # Show what expectations are in this suite
            expectations = validator.get_expectation_suite().expectations
            print(f"   📋 Suite contains {len(expectations)} expectations:")
            for exp in expectations[:5]:  # Show first 5
                exp_type = exp.expectation_type
                print(f"      - {exp_type}")
            if len(expectations) > 5:
                print(f"      ... and {len(expectations) - 5} more")
        else:
            print(f"   ⚠️  Suite {suite_name} not found, creating basic one")
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            # Add some basic expectations
            validator.expect_table_row_count_to_be_between(min_value=1, max_value=10000)
            validator.expect_column_values_to_not_be_null(column="urn")
            validator.expect_column_values_to_not_be_null(column="school_name")
            validator.save_expectation_suite(discard_failed_expectations=False)
            print(f"   ✅ Created new suite: {suite_name}")
        
        # Run validation
        results = validator.validate()
        
        # Display results
        print(f"\n   📊 Validation Results:")
        print(f"   {'='*50}")
        print(f"   Success: {results['success']}")
        print(f"   Expectations evaluated: {results['statistics']['evaluated_expectations']}")
        print(f"   Successful expectations: {results['statistics']['successful_expectations']}")
        print(f"   Failed expectations: {results['statistics']['unsuccessful_expectations']}")
        print(f"   Success percentage: {results['statistics']['success_percent']:.1f}%")
        
        if not results['success']:
            print(f"\n   ⚠️  Some expectations failed:")
            for result in results['results']:
                if not result['success']:
                    print(f"      - {result['expectation_config']['expectation_type']}")
        else:
            print(f"   ✅ All expectations passed!")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "="*60)
    print("✅ Great Expectations setup is working!")
    return True

def show_ge_structure():
    """Display the Great Expectations project structure"""
    print("\n📁 Great Expectations Project Structure:")
    print("="*60)
    
    # Look in multiple locations
    possible_locations = [
        Path("great_expectations"),
        Path("../great_expectations"),
        Path("../../great_expectations"),
        Path.home() / ".great_expectations"
    ]
    
    ge_dir = None
    for location in possible_locations:
        if location.exists():
            ge_dir = location
            print(f"✅ Found at: {ge_dir.absolute()}\n")
            break
    
    if not ge_dir:
        print("⚠️  great_expectations directory not found in common locations!")
        print("   Checked:")
        for loc in possible_locations:
            print(f"   - {loc.absolute()}")
        return
    
    def print_tree(directory, prefix="", max_depth=3, current_depth=0):
        if current_depth >= max_depth:
            return
        
        try:
            contents = sorted(directory.iterdir(), key=lambda x: (not x.is_dir(), x.name))
            for i, path in enumerate(contents):
                is_last = i == len(contents) - 1
                current_prefix = "└── " if is_last else "├── "
                print(f"{prefix}{current_prefix}{path.name}")
                
                if path.is_dir() and not path.name.startswith('.'):
                    extension_prefix = "    " if is_last else "│   "
                    print_tree(path, prefix + extension_prefix, max_depth, current_depth + 1)
        except PermissionError:
            pass
    
    print_tree(ge_dir)

if __name__ == "__main__":
    # Run tests
    success = test_ge_setup()
    
    if success:
        print("\n🎉 Great Expectations is ready to use!")
        print("\n📚 Next steps:")
        print("   1. Load Ofsted inspection data")
        print("   2. Create expectations for Ofsted data quality")
        print("   3. Set up data validation checkpoints")
        print("   4. Generate data documentation")
    else:
        print("\n⚠️  Great Expectations needs some configuration")
        print("   Let's fix the setup before continuing")
    
    # Show project structure
    show_ge_structure()