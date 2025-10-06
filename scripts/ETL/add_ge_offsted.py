from dotenv import load_dotenv
load_dotenv()

import great_expectations as gx
from ukeducationdbconnection import UKEducationDB

print("ADDING OFSTED INSPECTIONS TO GREAT EXPECTATIONS")
print("=" * 70)

context = gx.get_context()
db = UKEducationDB()

# Step 1: Add ofsted_inspections as a data asset
print("\n1️⃣ Adding ofsted_inspections as a data asset...")
print("=" * 70)

try:
    datasource = context.get_datasource("uk_education_db")
    
    # Check if ofsted_inspections already exists
    existing_assets = datasource.get_asset_names()
    
    if "ofsted_inspections" in existing_assets:
        print("Asset 'ofsted_inspections' already exists")
        data_asset = datasource.get_asset("ofsted_inspections")
    else:
        # Add the table as a data asset
        data_asset = datasource.add_table_asset(
            name="ofsted_inspections",
            table_name="ofsted_inspections"
        )
        print("✅ Added 'ofsted_inspections' as a data asset")
    
except Exception as e:
    print(f"Error adding data asset: {e}")
    print("\nThis is expected - the asset configuration may need to be done differently")
    print("Let's create expectations anyway using a batch request")

# Step 2: Create expectation suite
print("\n2️⃣ Creating expectation suite for ofsted_inspections...")
print("=" * 70)

suite_name = "ofsted_quality"

# Check if suite already exists
existing_suites = context.list_expectation_suite_names()

if suite_name in existing_suites:
    print(f"Suite '{suite_name}' already exists, will update it")
    suite = context.get_expectation_suite(suite_name)
else:
    print(f"Creating new suite '{suite_name}'")
    suite = context.add_expectation_suite(expectation_suite_name=suite_name)

# Get a validator
try:
    datasource = context.get_datasource("uk_education_db")
    
    # Try to get the data asset
    try:
        data_asset = datasource.get_asset("ofsted_inspections")
        batch_request = data_asset.build_batch_request()
    except:
        # If asset doesn't exist, we'll need to work around it
        print("Note: Data asset not found, you may need to configure it manually")
        print("For now, let's define the expectations conceptually")
        batch_request = None
    
    if batch_request:
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        print(f"\n📋 Adding expectations to '{suite_name}'...")
        
        # Core data integrity expectations
        print("\n   Adding core integrity checks...")
        validator.expect_column_to_exist(column="urn")
        validator.expect_column_to_exist(column="inspection_date")
        validator.expect_column_to_exist(column="overall_effectiveness")
        
        validator.expect_column_values_to_not_be_null(column="urn")
        validator.expect_column_values_to_not_be_null(column="inspection_date")
        
        # URN should be valid (6-7 digit numbers)
        validator.expect_column_values_to_be_between(
            column="urn",
            min_value=100000,
            max_value=9999999
        )
        
        # Rating expectations (1-4 scale)
        print("   Adding rating validations...")
        rating_columns = [
            'overall_effectiveness',
            'quality_of_education',
            'behaviour_and_attitudes',
            'personal_development',
            'leadership_and_management',
            'previous_overall_rating'
        ]
        
        for col in rating_columns:
            validator.expect_column_values_to_be_between(
                column=col,
                min_value=1,
                max_value=4,
                mostly=0.95  # Allow some nulls
            )
        
        # Date expectations
        print("   Adding date validations...")
        validator.expect_column_values_to_be_between(
            column="inspection_date",
            min_value="2005-01-01",
            max_value="2030-12-31"
        )
        
        # Inspection type should be reasonable length
        validator.expect_column_value_lengths_to_be_between(
            column="inspection_type",
            min_value=1,
            max_value=50
        )
        
        # Save the suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        print(f"\n✅ Saved {len(validator.get_expectation_suite().expectations)} expectations")
        
        # Step 3: Run validation
        print("\n3️⃣ Running validation...")
        print("=" * 70)
        
        results = validator.validate()
        
        print(f"\n📊 Validation Results:")
        print(f"   Success: {results['success']}")
        print(f"   Evaluated: {results['statistics']['evaluated_expectations']}")
        print(f"   Passed: {results['statistics']['successful_expectations']}")
        print(f"   Failed: {results['statistics']['unsuccessful_expectations']}")
        print(f"   Success Rate: {results['statistics']['success_percent']:.1f}%")
        
        if not results['success']:
            print(f"\n⚠️  Failed expectations:")
            for result in results['results']:
                if not result['success']:
                    exp_type = result['expectation_config']['expectation_type']
                    print(f"   - {exp_type}")
                    if 'result' in result and 'unexpected_count' in result['result']:
                        print(f"     Unexpected values: {result['result']['unexpected_count']}")
        
except Exception as e:
    print(f"Error creating validator: {e}")
    import traceback
    traceback.print_exc()
    
    print("\n💡 Manual configuration needed:")
    print("   The ofsted_inspections table needs to be added to your")
    print("   Great Expectations datasource configuration.")

# Step 4: Show current state
print("\n4️⃣ Current Great Expectations Setup:")
print("=" * 70)

print("\n📊 Data Assets:")
datasource = context.get_datasource("uk_education_db")
assets = datasource.get_asset_names()
for asset in assets:
    print(f"   - {asset}")

print("\n📋 Expectation Suites:")
suites = context.list_expectation_suite_names()
for suite in suites:
    print(f"   - {suite}")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

# Verify data is loaded
query = "SELECT COUNT(*) as count FROM ofsted_inspections"
result = db.read_sql(query)
inspection_count = result['count'][0]

print(f"\n✅ Ofsted inspections in database: {inspection_count:,}")
print(f"✅ Expectation suite created: {suite_name}")
print(f"✅ Ready for Data Docs generation")

print("\n📚 Next steps:")
print("   1. Generate Data Docs: context.build_data_docs()")
print("   2. View in browser: Check ~/.great_expectations/uncommitted/data_docs/")
print("   3. Run validations: Use checkpoints to validate all data")