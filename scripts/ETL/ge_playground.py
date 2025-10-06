from dotenv import load_dotenv
load_dotenv()

import great_expectations as gx
from ukeducationdbconnection import UKEducationDB
import json

print("🎮 GREAT EXPECTATIONS PLAYGROUND")
print("=" * 70)
print("\nWhat would you like to do?\n")
print("1. 📊 Validate school_performance data and see detailed results")
print("2. 🔍 Explore sen_pupils data (150k rows!) and create expectations")
print("3. 📈 Generate Data Docs (visual HTML reports)")
print("4. 🎯 Create a checkpoint for automated validation")
print("5. 🧪 Profile a table to auto-generate expectations")
print("6. ✨ Create custom expectations for a specific use case")
print("7. 🔗 Validate referential integrity between tables")
print("8. 📋 Run all existing validations and get a report")

choice = input("\nEnter your choice (1-8): ").strip()

context = gx.get_context()
db = UKEducationDB()

if choice == '1':
    print("\n" + "=" * 70)
    print("📊 VALIDATING SCHOOL PERFORMANCE DATA")
    print("=" * 70)
    
    # Get the data asset and validator
    datasource = context.get_datasource("uk_education_db")
    data_asset = datasource.get_asset("school_performance")
    batch_request = data_asset.build_batch_request()
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="performance_quality"
    )
    
    print("\n🔄 Running validation...")
    results = validator.validate()
    
    print(f"\n{'='*70}")
    print(f"📊 VALIDATION RESULTS")
    print(f"{'='*70}")
    print(f"✅ Success: {results['success']}")
    print(f"📈 Success Rate: {results['statistics']['success_percent']:.1f}%")
    print(f"📋 Total Expectations: {results['statistics']['evaluated_expectations']}")
    print(f"✅ Passed: {results['statistics']['successful_expectations']}")
    print(f"❌ Failed: {results['statistics']['unsuccessful_expectations']}")
    
    print(f"\n{'='*70}")
    print("DETAILED RESULTS")
    print(f"{'='*70}")
    
    for result in results['results']:
        exp_type = result['expectation_config']['expectation_type']
        success = result['success']
        status = "✅" if success else "❌"
        
        print(f"\n{status} {exp_type}")
        
        kwargs = result['expectation_config']['kwargs']
        if 'column' in kwargs:
            print(f"   Column: {kwargs['column']}")
        
        if not success:
            print(f"   ⚠️  FAILED")
            if 'result' in result:
                print(f"   Details: {result['result']}")

elif choice == '2':
    print("\n" + "=" * 70)
    print("🔍 EXPLORING SEN_PUPILS DATA")
    print("=" * 70)
    
    # Get schema
    schema_query = """
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'sen_pupils'
    ORDER BY ordinal_position
    """
    schema_df = db.read_sql(schema_query)
    
    print(f"\n📋 Columns ({len(schema_df)}):")
    for _, row in schema_df.iterrows():
        print(f"   - {row['column_name']:<30} ({row['data_type']})")
    
    # Get sample data
    sample_query = "SELECT * FROM sen_pupils LIMIT 5"
    sample_df = db.read_sql(sample_query)
    
    print(f"\n📊 Sample Data:")
    print(sample_df.to_string())
    
    print(f"\n💡 Would you like to create expectations for this table?")
    create = input("Create sen_pupils_quality suite? (y/n): ").strip().lower()
    
    if create == 'y':
        print("\n🔨 Creating expectation suite...")
        
        # Create new suite
        suite_name = "sen_pupils_quality"
        context.add_expectation_suite(expectation_suite_name=suite_name)
        
        # Get validator
        datasource = context.get_datasource("uk_education_db")
        
        # We need to add sen_pupils as a data asset first
        print("⚠️  sen_pupils is not configured as a data asset yet.")
        print("We would need to add it to the datasource configuration first.")
        print("\nWould you like me to show you how to do that?")

elif choice == '3':
    print("\n" + "=" * 70)
    print("📈 GENERATING DATA DOCS")
    print("=" * 70)
    
    print("\n🔄 Building Data Docs...")
    
    try:
        context.build_data_docs()
        print("\n✅ Data Docs generated successfully!")
        print("\n📂 Data Docs location:")
        print("   ~/.gx/uncommitted/data_docs/local_site/index.html")
        print("\n💡 To view:")
        print("   1. Navigate to the file in Windows Explorer")
        print("   2. Double-click index.html to open in browser")
        print("   3. Or run: great_expectations docs build")
        
        # Try to open automatically
        import webbrowser
        import os
        from pathlib import Path
        
        docs_path = Path.home() / ".gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
        if docs_path.exists():
            print("\n🌐 Opening in browser...")
            webbrowser.open(f"file://{docs_path}")
        
    except Exception as e:
        print(f"\n❌ Error generating docs: {e}")
        print("This might be because the GE directory structure needs to be set up.")

elif choice == '4':
    print("\n" + "=" * 70)
    print("🎯 CREATING VALIDATION CHECKPOINT")
    print("=" * 70)
    
    checkpoint_name = "school_data_quality_checkpoint"
    
    print(f"\nCreating checkpoint: {checkpoint_name}")
    print("This will validate both schools and school_performance tables.")
    
    try:
        checkpoint = context.add_or_update_checkpoint(
            name=checkpoint_name,
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "uk_education_db",
                        "data_asset_name": "schools"
                    },
                    "expectation_suite_name": "schools_quality"
                },
                {
                    "batch_request": {
                        "datasource_name": "uk_education_db",
                        "data_asset_name": "school_performance"
                    },
                    "expectation_suite_name": "performance_quality"
                }
            ]
        )
        
        print(f"\n✅ Checkpoint created!")
        print(f"\n🔄 Running checkpoint...")
        
        results = checkpoint.run()
        
        print(f"\n📊 CHECKPOINT RESULTS")
        print(f"{'='*70}")
        print(f"Success: {results['success']}")
        
        for validation_result in results['run_results'].values():
            suite_name = validation_result['validation_result']['meta']['expectation_suite_name']
            success = validation_result['validation_result']['success']
            stats = validation_result['validation_result']['statistics']
            
            status = "✅" if success else "❌"
            print(f"\n{status} {suite_name}")
            print(f"   Success Rate: {stats['success_percent']:.1f}%")
            print(f"   Passed: {stats['successful_expectations']}/{stats['evaluated_expectations']}")
        
        print(f"\n💡 You can now run this checkpoint anytime with:")
        print(f"   checkpoint = context.get_checkpoint('{checkpoint_name}')")
        print(f"   results = checkpoint.run()")
        
    except Exception as e:
        print(f"\n❌ Error creating checkpoint: {e}")
        import traceback
        traceback.print_exc()

elif choice == '5':
    print("\n" + "=" * 70)
    print("🧪 PROFILING A TABLE")
    print("=" * 70)
    
    print("\nAvailable tables:")
    print("1. schools")
    print("2. school_performance")
    print("3. sen_pupils")
    
    table_choice = input("\nWhich table to profile? (1-3): ").strip()
    
    table_map = {'1': 'schools', '2': 'school_performance', '3': 'sen_pupils'}
    table_name = table_map.get(table_choice)
    
    if table_name:
        print(f"\n🔍 Profiling {table_name}...")
        print("⚠️  Note: Auto-profiling works best with pre-configured data assets.")
        print("This feature generates expectations automatically based on data patterns.")
        print("\n💡 Would you like me to show you how to set up profiling?")
    else:
        print("Invalid choice!")

elif choice == '6':
    print("\n" + "=" * 70)
    print("✨ CUSTOM EXPECTATIONS")
    print("=" * 70)
    
    print("\nLet's create some interesting custom expectations!")
    print("\n1. Validate that secondary schools have pupils aged 11+")
    print("2. Check that Progress 8 scores cluster around 0 (national average)")
    print("3. Ensure active schools have valid opened_date")
    print("4. Validate that SEN pupils are linked to valid schools")
    
    custom_choice = input("\nChoose a custom validation (1-4): ").strip()
    
    if custom_choice == '1':
        print("\n🔨 Creating custom expectation for secondary schools...")
        print("Checking that secondary schools have statutory_high_age >= 16")
        
        # This would require custom SQL or pandas validation
        query = """
        SELECT COUNT(*) as count
        FROM schools
        WHERE phase = 'Secondary' 
        AND (statutory_high_age IS NULL OR statutory_high_age < 16)
        """
        
        result_df = db.read_sql(query)
        violations = result_df['count'][0]
        
        if violations == 0:
            print(f"✅ All secondary schools have valid age ranges!")
        else:
            print(f"⚠️  Found {violations} secondary schools with invalid age ranges")

elif choice == '7':
    print("\n" + "=" * 70)
    print("🔗 REFERENTIAL INTEGRITY VALIDATION")
    print("=" * 70)
    
    print("\nChecking referential integrity between tables...")
    
    # Check if all schools in school_performance exist in schools
    query1 = """
    SELECT COUNT(*) as orphaned_records
    FROM school_performance sp
    LEFT JOIN schools s ON sp.urn = s.urn
    WHERE s.urn IS NULL
    """
    
    result1 = db.read_sql(query1)
    orphaned = result1['orphaned_records'][0]
    
    if orphaned == 0:
        print("✅ All performance records have valid school URNs")
    else:
        print(f"⚠️  Found {orphaned} performance records without matching schools")
    
    # Check sen_pupils
    query2 = """
    SELECT COUNT(*) as orphaned_sen
    FROM sen_pupils sp
    LEFT JOIN schools s ON sp.urn = s.urn
    WHERE s.urn IS NULL
    """
    
    result2 = db.read_sql(query2)
    orphaned_sen = result2['orphaned_sen'][0]
    
    if orphaned_sen == 0:
        print("✅ All SEN records have valid school URNs")
    else:
        print(f"⚠️  Found {orphaned_sen} SEN records without matching schools")

elif choice == '8':
    print("\n" + "=" * 70)
    print("📋 RUNNING ALL VALIDATIONS")
    print("=" * 70)
    
    suites = context.list_expectation_suite_names()
    
    for suite_name in suites:
        print(f"\n{'='*70}")
        print(f"Validating: {suite_name}")
        print(f"{'='*70}")
        
        # Determine which data asset to use
        if 'schools' in suite_name and 'performance' not in suite_name:
            asset_name = "schools"
        elif 'performance' in suite_name:
            asset_name = "school_performance"
        else:
            print(f"⚠️  Don't know which asset to use for {suite_name}")
            continue
        
        try:
            datasource = context.get_datasource("uk_education_db")
            data_asset = datasource.get_asset(asset_name)
            batch_request = data_asset.build_batch_request()
            
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            
            results = validator.validate()
            
            status = "✅" if results['success'] else "❌"
            print(f"{status} Success: {results['success']}")
            print(f"   Success Rate: {results['statistics']['success_percent']:.1f}%")
            print(f"   Passed: {results['statistics']['successful_expectations']}/{results['statistics']['evaluated_expectations']}")
            
        except Exception as e:
            print(f"❌ Error: {e}")

else:
    print("\n❌ Invalid choice!")

print("\n" + "=" * 70)
print("Thanks for playing with Great Expectations! 🎉")
print("=" * 70)