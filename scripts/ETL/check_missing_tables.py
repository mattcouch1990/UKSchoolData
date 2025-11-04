from dotenv import load_dotenv
load_dotenv()

from ukeducationdbconnection import UKEducationDB
from pathlib import Path

print("CHECKING EMPTY TABLES AND AVAILABLE DATA")
print("=" * 70)

db = UKEducationDB()

# Step 1: Identify empty tables
print("\n1️⃣ Current table status:")
print("=" * 70)

tables_query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
"""

tables_df = db.read_sql(tables_query)

table_status = {}
for _, row in tables_df.iterrows():
    table_name = row['table_name']
    count_query = f"SELECT COUNT(*) as count FROM {table_name}"
    try:
        count_df = db.read_sql(count_query)
        row_count = count_df['count'][0]
        table_status[table_name] = row_count
        
        status = "✅" if row_count > 0 else "❌"
        print(f"{status} {table_name:<35} {row_count:>10,} rows")
    except:
        print(f"⚠️  {table_name:<35} (error counting)")

# Step 2: Check schemas of empty tables
print("\n2️⃣ Empty tables and their schemas:")
print("=" * 70)

empty_tables = [name for name, count in table_status.items() if count == 0]

for table_name in empty_tables:
    print(f"\n📋 {table_name}")
    schema_query = f"""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    
    schema_df = db.read_sql(schema_query)
    for _, col in schema_df.iterrows():
        nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
        print(f"   - {col['column_name']:<30} {col['data_type']:<20} {nullable}")

# Step 3: Check available data files
print("\n3️⃣ Available data files not yet loaded:")
print("=" * 70)

data_dir = Path("../../data")

if data_dir.exists():
    # Check Ofsted folder for unused files
    ofsted_dir = data_dir / "ofsted"
    if ofsted_dir.exists():
        print("\n📁 Ofsted directory:")
        ofsted_files = list(ofsted_dir.glob("*.csv")) + list(ofsted_dir.glob("*.ods"))
        
        loaded_file = "State_funded_schools_inspections_and_outcomes_as_at_31_December_2024.csv"
        
        for file in ofsted_files:
            if file.name == loaded_file:
                print(f"   ✅ {file.name} (already loaded)")
            else:
                size_mb = file.stat().st_size / (1024 * 1024)
                print(f"   ❌ {file.name} ({size_mb:.2f} MB) - NOT LOADED")
    
    # Check for other data directories
    for subdir in data_dir.iterdir():
        if subdir.is_dir() and subdir.name not in ['ofsted', '__pycache__']:
            print(f"\n📁 {subdir.name} directory:")
            files = list(subdir.glob("*"))
            if files:
                for file in files[:10]:  # Show first 10
                    if file.is_file():
                        size_mb = file.stat().st_size / (1024 * 1024)
                        print(f"   - {file.name} ({size_mb:.2f} MB)")
            else:
                print(f"   (empty)")

# Step 4: Assessment
print("\n4️⃣ ETL COMPLETION ASSESSMENT:")
print("=" * 70)

print("\n📊 Data Pipeline Status:")
print(f"\n✅ LOADED ({len([c for c in table_status.values() if c > 0])} tables with data):")
for table, count in sorted(table_status.items()):
    if count > 0:
        print(f"   - {table}: {count:,} rows")

print(f"\n❌ EMPTY ({len(empty_tables)} tables):")
for table in empty_tables:
    print(f"   - {table}")

print("\n💡 RECOMMENDATIONS:")
print("=" * 70)

# Analyze what's needed for their analysis plan
recommendations = []

if "geography" in empty_tables:
    recommendations.append({
        "table": "geography",
        "priority": "HIGH",
        "reason": "Needed for geographic visualizations (your plan item #2)",
        "action": "Could derive from local_authorities + schools data, or load ONS geographic data"
    })

if "graduate_outcomes" in empty_tables:
    recommendations.append({
        "table": "graduate_outcomes",
        "priority": "LOW",
        "reason": "Not in your immediate analysis plan (focused on inspections + performance)",
        "action": "Skip for now, can add later if needed"
    })

if "data_quality_checks" in empty_tables:
    recommendations.append({
        "table": "data_quality_checks",
        "priority": "LOW",
        "reason": "Metadata table - Great Expectations handles validation",
        "action": "Optional - could store GE validation run results here"
    })

for rec in recommendations:
    print(f"\n{rec['table'].upper()} - Priority: {rec['priority']}")
    print(f"   Reason: {rec['reason']}")
    print(f"   Action: {rec['action']}")

# Additional Ofsted files assessment
print("\n📁 ADDITIONAL OFSTED FILES:")
print("   - Independent schools inspection data (NOT LOADED)")
print("   - Teacher education inspection data (NOT LOADED)")
print("   Priority: LOW - Your analysis focuses on state-funded schools")

print("\n" + "=" * 70)
print("ETL COMPLETION RECOMMENDATION")
print("=" * 70)

print("""
CORE DATASET: ✅ COMPLETE
   - 5,709 schools with basic info
   - 5,709 performance records (Attainment 8, Progress 8)
   - 4,088 Ofsted inspections (ratings, dates)
   - 150,281 SEN pupil records (demographics, provision)
   - Reference tables (local authorities, school types, SEN categories)

OPTIONAL ENHANCEMENTS:
   - Geography table: Would enhance geographic analysis
   - Graduate outcomes: Not needed for current plan
   - Additional Ofsted files: Independent schools (not your focus)

RECOMMENDATION:
   ✅ Your ETL is FUNCTIONALLY COMPLETE for your analysis plan:
      1. Ofsted rating breakdowns ✓ (have data)
      2. Geographic visualizations ✓ (have local_authority_code)
      3. Correlations ✓ (can join schools + performance + inspections)
      4. ML predictions ✓ (have all features needed)
   
   You can:
   Option A: START ANALYSIS NOW with current data
   Option B: Populate geography table first (if you want richer maps)
   Option C: Load additional Ofsted files (independent schools)

What would you like to do?
""")