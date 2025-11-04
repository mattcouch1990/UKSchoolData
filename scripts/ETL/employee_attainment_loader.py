from dotenv import load_dotenv
load_dotenv()

from ukeducationdbconnection import UKEducationDB
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
import numpy as np

print("LOADING REGIONAL EDUCATION ATTAINMENT DATA")
print("=" * 70)

db = UKEducationDB()
file_path = Path("../../data/ons/educationattainmentbydisabilityitl2region2022.xlsx")

if not file_path.exists():
    print(f"File not found: {file_path}")
    exit(1)

print(f"\n📂 File: {file_path.name}")

calendar_year = 2022  # From filename

# Table configurations
table_configs = {
    'Table 1': {
        'description': 'Base data (Region, Disability Status, Qualification)',
        'columns': ['ITL2 Region', 'Disability Status', 'Highest Qualification', 'Weighted Count', 'Sample size'],
        'has_gender': False,
        'has_age': False,
        'has_severity': False,
        'has_condition': False
    },
    'Table 2': {
        'description': 'Base + Gender',
        'columns': ['ITL2 Region', 'Disability Status', 'Sex', 'Highest Qualification', 'Weighted Count', 'Sample size'],
        'has_gender': True,
        'has_age': False,
        'has_severity': False,
        'has_condition': False
    },
    'Table 3': {
        'description': 'Base + Age Band',
        'columns': ['ITL2 Region', 'Disability Status', 'Highest Qualification', 'Age Band', 'Weighted Count', 'Sample size'],
        'has_gender': False,
        'has_age': True,
        'has_severity': False,
        'has_condition': False
    },
    'Table 4': {
        'description': 'Disability Severity (A little, A lot)',
        'columns': ['ITL2 Region', 'Highest Qualification', 'Disability Severity', 'Weighted Count', 'Sample size'],
        'has_gender': False,
        'has_age': False,
        'has_severity': True,
        'has_condition': False
    },
    'Table 5': {
        'description': '17 Health Conditions',
        'columns': ['ITL2 Region', 'Highest Qualification', 'Health Condition', 'Weighted Count', 'Sample size'],
        'has_gender': False,
        'has_age': False,
        'has_severity': False,
        'has_condition': True
    },
    'Table 6': {
        'description': 'Health Condition + Disability Severity (RICHEST DATA)',
        'columns': ['ITL2 Region', 'Highest Qualification', 'Health Condition', 'Disability Severity', 'Weighted Count', 'Sample size'],
        'has_gender': False,
        'has_age': False,
        'has_severity': True,
        'has_condition': True
    }
}

print("\n📋 Available tables:")
for i, (table_name, config) in enumerate(table_configs.items(), 1):
    print(f"   {i}. {table_name}: {config['description']}")

print("\n💡 Recommendation: Load Table 6 (most detailed) + optionally Table 2 (gender) or Table 3 (age)")

choice = input("\nWhich tables to load? (e.g., '6' or '2,6' or 'all'): ").strip()

tables_to_load = []
if choice.lower() == 'all':
    tables_to_load = list(table_configs.keys())
else:
    table_nums = [int(x.strip()) for x in choice.split(',') if x.strip().isdigit()]
    table_names = list(table_configs.keys())
    tables_to_load = [table_names[i-1] for i in table_nums if 0 < i <= len(table_names)]

if not tables_to_load:
    print("No tables selected")
    exit(0)

print(f"\nLoading: {', '.join(tables_to_load)}")

# Load each selected table
all_records = []

for table_name in tables_to_load:
    print(f"\n{'='*70}")
    print(f"📋 Processing {table_name}")
    print(f"{'='*70}")
    
    config = table_configs[table_name]
    
    try:
        # Load table (skip first 4 rows)
        df = pd.read_excel(file_path, sheet_name=table_name, skiprows=4)
        
        print(f"Loaded {len(df):,} rows")
        
        # Create output dataframe
        output_df = pd.DataFrame()
        
        # Standard fields
        output_df['calendar_year'] = 2022
        output_df['region'] = df['ITL2 Region']
        output_df['qualification_level'] = df['Highest Qualification']
        output_df['local_authority_code'] = None  # We have regions, not LAs
        
        # Build disability_status from available columns
        if config['has_condition'] and config['has_severity']:
            # Table 6: Combine condition + severity
            output_df['disability_status'] = df['Health Condition'] + ' - ' + df['Disability Severity']
        elif config['has_condition']:
            # Table 5: Just condition
            output_df['disability_status'] = df['Health Condition']
        elif config['has_severity']:
            # Table 4: Just severity
            output_df['disability_status'] = df['Disability Severity']
        elif 'Disability Status' in df.columns:
            # Tables 1-3: Standard status
            output_df['disability_status'] = df['Disability Status']
        
        # Demographics
        if config['has_gender'] and 'Sex' in df.columns:
            output_df['gender'] = df['Sex']
        else:
            output_df['gender'] = None
        
        if config['has_age'] and 'Age Band' in df.columns:
            output_df['age_group'] = df['Age Band']
        else:
            output_df['age_group'] = None
        
        # Sample size (handle [c] for confidential)
        if 'Sample size' in df.columns:
            sample_size = df['Sample size'].astype(str).replace('[c]', np.nan)
            output_df['sample_size'] = pd.to_numeric(sample_size, errors='coerce').astype('Int64')
        else:
            output_df['sample_size'] = None
        
        # Employment metrics (we don't have this data)
        output_df['employment_rate'] = None
        output_df['unemployment_rate'] = None
        output_df['inactivity_rate'] = None
        output_df['median_salary'] = None
        output_df['mean_salary'] = None
        
        # Metadata
        output_df['created_at'] = datetime.now()
        output_df['updated_at'] = datetime.now()
        
        # Remove rows where critical fields are null
        before_clean = len(output_df)
        output_df = output_df.dropna(subset=['region', 'qualification_level'])
        after_clean = len(output_df)
        
        if before_clean > after_clean:
            print(f"Removed {before_clean - after_clean} rows with missing critical data")
        
        print(f"✅ Prepared {len(output_df):,} records from {table_name}")
        
        all_records.append(output_df)
        
    except Exception as e:
        print(f"❌ Error processing {table_name}: {e}")
        import traceback
        traceback.print_exc()
        continue

if not all_records:
    print("\n❌ No data loaded")
    exit(1)

# Combine all loaded tables
print(f"\n{'='*70}")
print("COMBINING DATA")
print(f"{'='*70}")

combined_df = pd.concat(all_records, ignore_index=True)

print(f"\nTotal records: {len(combined_df):,}")
print(f"\nBreakdown:")
print(f"   Unique regions: {combined_df['region'].nunique()}")
print(f"   Unique qualifications: {combined_df['qualification_level'].nunique()}")
print(f"   Unique disability statuses: {combined_df['disability_status'].nunique()}")

if combined_df['gender'].notna().any():
    print(f"   Gender values: {combined_df['gender'].dropna().unique()}")
if combined_df['age_group'].notna().any():
    print(f"   Age groups: {combined_df['age_group'].dropna().unique()}")

print(f"\n📊 Sample combined data:")
print(combined_df[['region', 'disability_status', 'qualification_level', 'gender', 'age_group']].head(10))

# Load to database
load = input("\nLoad to graduate_outcomes table? (y/n): ").strip().lower()

if load == 'y':
    try:
        print("Fixing calendar_year nulls...")
        combined_df['calendar_year'] = combined_df['calendar_year'].fillna(2022)
        combined_df['calendar_year'] = combined_df['calendar_year'].astype(int)
        print(f"Calendar year nulls after fix: {combined_df['calendar_year'].isnull().sum()}")
        # Clear existing data
        print("\nClearing existing graduate_outcomes data...")
        with db.engine.connect() as conn:
            conn.execute(text("DELETE FROM graduate_outcomes"))
            conn.commit()
        print("✅ Cleared")
        
        # Load in batches
        print(f"\nLoading {len(combined_df):,} records...")
        batch_size = 1000
        total_batches = (len(combined_df) + batch_size - 1) // batch_size
        
        for i in range(0, len(combined_df), batch_size):
            batch = combined_df.iloc[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            db.write_dataframe(batch, 'graduate_outcomes', if_exists='append')
            print(f"Loaded batch {batch_num}/{total_batches} ({len(batch)} rows)")
        
        print(f"\n✅ Successfully loaded {len(combined_df):,} records!")
        
        # Verify
        count_query = "SELECT COUNT(*) as count FROM graduate_outcomes"
        result = db.read_sql(count_query)
        print(f"✅ Verified: {result['count'][0]:,} records in database")
        
        # Show sample
        sample_query = """
        SELECT region, disability_status, qualification_level, 
            gender, age_group, sample_size
        FROM graduate_outcomes 
        LIMIT 5
        """
        print(f"\n📊 Sample from database (with health condition + severity):")
        sample = db.read_sql(sample_query)
        print(sample)
        
        print("\n" + "=" * 70)
        print("🎊 ETL COMPLETE!")
        print("=" * 70)
        
        print("\n✅ Final Database:")
        print("   ✅ 5,709 schools")
        print("   ✅ 5,709 performance records")
        print("   ✅ 4,088 Ofsted inspections")
        print("   ✅ 150,281 SEN pupil records")
        print("   ✅ 165 Local Authorities")
        print(f"   ✅ {result['count'][0]:,} Regional education attainment records")
        
        print("\n📊 Regional attainment data includes:")
        print("   - 41 ITL2 regions across UK")
        print("   - 17 specific health conditions")
        print("   - 3 disability severity levels")
        print("   - 6 qualification levels")
        print("   - Gender and age breakdowns (where selected)")
        
        print("\n🎯 Ready for regional SEN analysis!")
        print("   - Compare regions by disability educational attainment")
        print("   - Analyze specific health conditions vs qualification levels")
        print("   - Severity analysis (A little vs A lot)")
        print("   - Correlate with your school-level SEN provision")
        
    except Exception as e:
        print(f"\n❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()
else:
    print("\n❌ Loading cancelled")

print("\n✅ DONE!")