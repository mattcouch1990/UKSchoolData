from dotenv import load_dotenv
load_dotenv()

import great_expectations as gx
import json

print("🔍 EXPLORING YOUR GREAT EXPECTATIONS SETUP")
print("=" * 70)

context = gx.get_context()

# 1. Show all expectation suites in detail
print("\n📚 EXPECTATION SUITES DETAILS")
print("=" * 70)

suite_names = context.list_expectation_suite_names()

for suite_name in suite_names:
    print(f"\n{'='*70}")
    print(f"📋 Suite: {suite_name}")
    print(f"{'='*70}")
    
    suite = context.get_expectation_suite(suite_name)
    expectations = suite.expectations
    
    print(f"Total expectations: {len(expectations)}\n")
    
    for i, exp in enumerate(expectations, 1):
        print(f"{i}. {exp.expectation_type}")
        
        # Show key parameters
        kwargs = exp.kwargs
        if 'column' in kwargs:
            print(f"   Column: {kwargs['column']}")
        
        # Show specific parameters based on expectation type
        if 'min_value' in kwargs or 'max_value' in kwargs:
            min_val = kwargs.get('min_value', 'N/A')
            max_val = kwargs.get('max_value', 'N/A')
            print(f"   Range: {min_val} to {max_val}")
        
        if 'value_set' in kwargs:
            values = kwargs['value_set']
            if len(values) <= 5:
                print(f"   Allowed values: {values}")
            else:
                print(f"   Allowed values: {values[:5]} ... ({len(values)} total)")
        
        if 'regex' in kwargs:
            print(f"   Pattern: {kwargs['regex']}")
        
        print()

# 2. Show available data assets
print("\n" + "=" * 70)
print("📊 AVAILABLE DATA ASSETS")
print("=" * 70)

datasources = context.list_datasources()
for ds in datasources:
    ds_name = ds.get('name', 'Unknown')
    print(f"\n🔗 Datasource: {ds_name}")
    
    try:
        datasource = context.get_datasource(ds_name)
        assets = datasource.get_asset_names()
        
        print(f"   Available assets:")
        for asset_name in assets:
            print(f"   - {asset_name}")
            
            # Try to get asset info
            try:
                asset = datasource.get_asset(asset_name)
                print(f"     Type: {type(asset).__name__}")
            except:
                pass
    except Exception as e:
        print(f"   ⚠️  Could not retrieve assets: {e}")

# 3. Show what tables exist in the database
print("\n" + "=" * 70)
print("📋 DATABASE TABLES")
print("=" * 70)

from ukeducationdbconnection import UKEducationDB
db = UKEducationDB()

tables_query = """
SELECT table_name, 
       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
ORDER BY table_name;
"""

tables_df = db.read_sql(tables_query)
print(f"\nTotal tables: {len(tables_df)}\n")

for _, row in tables_df.iterrows():
    table_name = row['table_name']
    col_count = row['column_count']
    
    # Get row count
    count_query = f"SELECT COUNT(*) as count FROM {table_name}"
    try:
        count_df = db.read_sql(count_query)
        row_count = count_df['count'][0]
        print(f"✅ {table_name:<30} {row_count:>8,} rows  {col_count:>2} columns")
    except:
        print(f"⚠️  {table_name:<30} (could not count rows)")

# 4. Suggest next steps
print("\n" + "=" * 70)
print("🎯 SUGGESTED NEXT STEPS")
print("=" * 70)

print("""
1. 🔍 Explore existing expectations in detail
   - View the full schools_quality suite
   - View the performance_quality suite
   - Run validations and generate data docs

2. 📊 Load Ofsted inspection data
   - Add Ofsted inspection ratings
   - Add Ofsted outcomes data
   - Create quality expectations for Ofsted data

3. 🔗 Create relationships between data
   - Join schools with Ofsted data
   - Validate referential integrity
   - Create cross-table expectations

4. 📈 Advanced GE features
   - Set up checkpoints for automated validation
   - Generate and view Data Docs
   - Create custom expectations
   - Add profiling for new datasets

5. 🎨 Explore the other 9 tables
   - What's in school_performance?
   - What other data do you have?
   - Create expectations for all tables

Which would you like to do next?
""")

print("=" * 70)