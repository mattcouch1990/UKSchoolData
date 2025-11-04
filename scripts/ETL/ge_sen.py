from dotenv import load_dotenv
load_dotenv()

import great_expectations as gx
from ukeducationdbconnection import UKEducationDB
import pandas as pd

print("SETTING UP GREAT EXPECTATIONS FOR SEN PUPILS")
print("=" * 70)

db = UKEducationDB()
context = gx.get_context()

# Step 1: Inspect the sen_pupils table
print("\n1️⃣ Inspecting sen_pupils table structure...")
print("=" * 70)

schema_query = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'sen_pupils'
ORDER BY ordinal_position;
"""

schema_df = db.read_sql(schema_query)

print(f"\nColumns ({len(schema_df)}):")
for _, row in schema_df.iterrows():
    nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
    print(f"   {row['column_name']:<30} {row['data_type']:<20} {nullable}")

# Get sample data
print(f"\n📊 Sample data (first 3 rows):")
sample_query = "SELECT * FROM sen_pupils LIMIT 3"
sample_df = db.read_sql(sample_query)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
print(sample_df)

# Get row count
count_query = "SELECT COUNT(*) as count FROM sen_pupils"
count_df = db.read_sql(count_query)
total_rows = count_df['count'][0]
print(f"\n📈 Total records: {total_rows:,}")

# Step 2: Add sen_pupils as a data asset
print("\n2️⃣ Adding sen_pupils as a data asset...")
print("=" * 70)

try:
    datasource = context.get_datasource("uk_education_db")
    
    # Check if already exists
    existing_assets = datasource.get_asset_names()
    
    if "sen_pupils" in existing_assets:
        print("Asset 'sen_pupils' already exists")
        data_asset = datasource.get_asset("sen_pupils")
    else:
        # Add the table as a data asset
        data_asset = datasource.add_table_asset(
            name="sen_pupils",
            table_name="sen_pupils"
        )
        print("✅ Added 'sen_pupils' as a data asset")
    
except Exception as e:
    print(f"Note: {e}")
    print("Continuing with expectation suite creation...")

# Step 3: Create expectation suite
print("\n3️⃣ Creating expectation suite for sen_pupils...")
print("=" * 70)

suite_name = "sen_pupils_quality"

# Check if suite already exists
existing_suites = context.list_expectation_suite_names()

if suite_name in existing_suites:
    print(f"Suite '{suite_name}' already exists, will update it")
else:
    print(f"Creating new suite '{suite_name}'")
    context.add_expectation_suite(expectation_suite_name=suite_name)

# Get validator
try:
    data_asset = datasource.get_asset("sen_pupils")
    batch_request = data_asset.build_batch_request()
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print(f"\n📋 Adding expectations to '{suite_name}'...")
    
    # Core integrity expectations
    print("\n   Adding core integrity checks...")
    
    # URN should exist and not be null (links to schools)
    validator.expect_column_to_exist(column="urn")
    validator.expect_column_values_to_not_be_null(column="urn")
    
    # URN should be valid range
    validator.expect_column_values_to_be_between(
        column="urn",
        min_value=100000,
        max_value=9999999
    )
    
    # Check for columns that are typically in SEN data
    # (These will vary based on actual schema)
    
    # If there's an academic year column
    if 'academic_year' in sample_df.columns or 'year' in sample_df.columns:
        year_col = 'academic_year' if 'academic_year' in sample_df.columns else 'year'
        validator.expect_column_to_exist(column=year_col)
        print(f"   Added validation for {year_col}")
    
    # If there's a SEN category/type column
    sen_cols = [col for col in sample_df.columns if 'sen' in col.lower() or 'category' in col.lower()]
    if sen_cols:
        for col in sen_cols[:3]:  # First 3 SEN-related columns
            validator.expect_column_to_exist(column=col)
            print(f"   Added validation for {col}")
    
    # If there are count/number columns, they should be non-negative
    count_cols = [col for col in sample_df.columns if 'count' in col.lower() or 'number' in col.lower() or 'total' in col.lower()]
    for col in count_cols[:3]:  # First 3 count columns
        validator.expect_column_values_to_be_between(
            column=col,
            min_value=0,
            mostly=0.95  # Allow some nulls
        )
        print(f"   Added non-negative validation for {col}")
    
    # Table size expectation
    validator.expect_table_row_count_to_be_between(
        min_value=100000,
        max_value=200000
    )
    
    # Save the suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"\n✅ Saved {len(validator.get_expectation_suite().expectations)} expectations")
    
    # Step 4: Run validation
    print("\n4️⃣ Running validation...")
    print("=" * 70)
    
    results = validator.validate()
    
    print(f"\n📊 VALIDATION RESULTS")
    print("=" * 70)
    print(f"\nOverall Success: {'✅ PASS' if results['success'] else '❌ FAIL'}")
    print(f"Success Rate: {results['statistics']['success_percent']:.1f}%")
    print(f"\nExpectations Evaluated: {results['statistics']['evaluated_expectations']}")
    print(f"Successful: {results['statistics']['successful_expectations']}")
    print(f"Failed: {results['statistics']['unsuccessful_expectations']}")
    
    if not results['success']:
        print(f"\n⚠️  Failed expectations:")
        for result in results['results']:
            if not result['success']:
                exp_type = result['expectation_config']['expectation_type']
                print(f"   - {exp_type}")
                kwargs = result['expectation_config'].get('kwargs', {})
                if 'column' in kwargs:
                    print(f"     Column: {kwargs['column']}")
    
    # Step 5: Rebuild Data Docs
    print("\n5️⃣ Rebuilding Data Docs...")
    print("=" * 70)
    
    context.build_data_docs()
    print("✅ Data Docs rebuilt with SEN pupils validation results")
    
    # Open docs
    print("\n6️⃣ Opening Data Docs...")
    from pathlib import Path
    import webbrowser
    
    paths = [
        Path.home() / ".great_expectations" / "uncommitted" / "data_docs" / "local_site" / "index.html",
        Path("gx") / "uncommitted" / "data_docs" / "local_site" / "index.html",
    ]
    
    for path in paths:
        if path.exists():
            print(f"Opening: {path}")
            webbrowser.open(f"file:///{path.absolute()}")
            break
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

print(f"\n✅ SEN Pupils Dataset:")
print(f"   Records: {total_rows:,}")
print(f"   Columns: {len(schema_df)}")
print(f"   Expectation Suite: {suite_name}")

print(f"\n📊 All Data Assets in Great Expectations:")
datasource = context.get_datasource("uk_education_db")
assets = datasource.get_asset_names()
for asset in assets:
    print(f"   - {asset}")

print(f"\n📋 All Expectation Suites:")
suites = context.list_expectation_suite_names()
for suite in suites:
    print(f"   - {suite}")

print("\n🎉 SEN pupils validation is now set up!")
print("\n💡 To re-run validation:")
print("   validator = context.get_validator(...)")
print("   results = validator.validate()")
print("   context.build_data_docs()")