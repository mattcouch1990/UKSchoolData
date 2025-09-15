import pandas as pd
import numpy as np
from ukeducationdbconnection import UKEducationDB

def load_sen_data():
    """Load SEN data with step-by-step verification"""
    
    file_path = r"C:\Users\matth\Desktop\UKSchoolData\UKSchoolData\data\dfe\special-educational-needs-in-england_2024-25\data\sen_ncyear_new_.csv"
      
    # Load data
    df = pd.read_csv(file_path)
    print(f"Data Shape: {df.shape}")
    
    # Filter to Local Authority level and remove totals
    df_clean = df[
        (df['geographic_level'] == 'Local authority') &
        (df['sen_status'] != 'Total') & 
        (df['sen_primary_need'] != 'Total')
    ].copy()
    print(f"Data successfully filtered")
    
    # Create mapping
    sen_status_mapping = {
        'No identified SEN': 'No SEN',
        'SEN support / SEN without an EHC plan': 'SEN Support',
        'Education, Health and Care plans': 'EHC Plan'
    }
    
    need_mapping = {
        'Autistic Spectrum Disorder': 'ASD',
        'Hearing Impairment': 'HI',
        'Moderate Learning Difficulty': 'MLD',
        'Other difficulty or disability': 'OTH',
        'Physical Disability': 'PD',
        'Profound & Multiple Learning Difficulty': 'PMLD',
        'Social, Emotional and Mental Health': 'SEMH',
        'Speech, Language and Communications needs': 'SLCN',
        'Visual Impairment': 'VI',
        'Multi-sensory impairment': 'MSI',
        'Severe Learning Difficulty': 'SLD',
        'Specific Learning Difficulty': 'SPLD',
        'SEN support but no specialist assessment of type of need': 'NSA',
        'Downs Syndrome': 'OTH',
        'Missing': None
    }
    
    df_clean['sen_provision'] = df_clean['sen_status'].map(sen_status_mapping)
    df_clean['primary_need_code'] = df_clean['sen_primary_need'].map(need_mapping)
    
    print(f"SEN mappings applied")
    
    #Load Local Authorities 
    print(f"\n Loading Local Authorities...")
    
    db = UKEducationDB()
    
    # drop duplicates
    la_data = df_clean[['new_la_code', 'la_name']].drop_duplicates()
    la_data = la_data.dropna(subset=['new_la_code', 'la_name'])
    
    # Check which LAs already exist
    try:
        existing_las = db.read_sql("SELECT la_code FROM local_authorities")
        existing_codes = set(existing_las['la_code'].tolist())
    except:
        existing_codes = set()
    
    # Prepare new LAs for insertion
    new_las = []
    for _, row in la_data.iterrows():
        if row['new_la_code'] not in existing_codes:
            new_las.append({
                'la_code': row['new_la_code'],
                'la_name': row['la_name'],
                'region': 'England' """TODO: UPDATE WITH REAL REGIONS"""
            })
    #write new authorities
    if new_las:
        la_df = pd.DataFrame(new_las)
        db.write_dataframe(la_df, 'local_authorities', if_exists='append')
        print(f"✅ Added {len(new_las)} new local authorities")
    else:
        print(f"ℹ️  All local authorities already exist")
    
    # Create SEN pupil records 
    print(f"\n👥 Creating SEN pupil records...")
    
    sen_records = []
    
    for idx, row in df_clean.iterrows():
        # Skip records with missing key data, must be acknowledged
        if pd.isna(row['sen_provision']) or pd.isna(row['new_la_code']):
            continue
            
        # Create one record per data row (represents aggregated pupil counts)
        sen_record = {
            'urn': None,  # No school-level data in this dataset
            'academic_year': '2024-25',
            'sen_provision': row['sen_provision'],
            'primary_need_code': row['primary_need_code'],
            'year_group': None,  #
            'gender': None,
            'ethnicity': None, 
            'first_language': None,
            'fsm_eligible': None,
            'progress_8_score': None,
            'attainment_8_score': None,
            'pupil_count': int(row['number_of_pupils']) if pd.notna(row['number_of_pupils']) else 0
        }
        
        # Only add records with actual pupil counts
        if sen_record['pupil_count'] > 0:
            sen_records.append(sen_record)
            
        # Progress indicator
        if (idx + 1) % 10000 == 0:
            print(f"   Processed {idx + 1:,} records...")
    
    print(f" Created {len(sen_records)} SEN pupil records")
    
    # Load SEN data to database
    if sen_records:
        sen_df = pd.DataFrame(sen_records)
        
        # Remove records where primary_need_code is None 
        sen_df_clean = sen_df[sen_df['primary_need_code'].notna()]
        print(f"   Records with valid need codes: {len(sen_df_clean)}")
        
        if len(sen_df_clean) > 0:
            db.write_dataframe(sen_df_clean, 'sen_pupils', if_exists='append')
            
            # Show summary
            total_pupils = sen_df_clean['pupil_count'].sum()
            print(f"Total pupils: {total_pupils:,}")
            
            provision_summary = sen_df_clean.groupby('sen_provision')['pupil_count'].sum()
            print(f"By SEN provision:")
            for prov, count in provision_summary.items():
                print(f"   {prov}: {count:,}")
        else:
            print(" No records with valid need codes to load")
    else:
        print("No SEN records created")
    
    # Record the data source
    load_record = pd.DataFrame([{
        'source_name': 'DfE Special Educational Needs Statistics',
        'source_url': 'https://explore-education-statistics.service.gov.uk/find-statistics/special-educational-needs-in-england/2024-25',
        'file_name': 'sen_ncyear_new_.ods',
        'download_date': pd.Timestamp.now().date(),
        'academic_year': '2024-25',
        'records_loaded': len(sen_df_clean) if 'sen_df_clean' in locals() else 0,
        'load_status': 'success'
    }])
    
    db.write_dataframe(load_record, 'data_sources', if_exists='append')
    print(f"✅ Data source recorded")
    
    return True

if __name__ == "__main__":
    success = load_sen_data()
    
    if success:
        print(f"\n SEN Data Loading Complete!")
        
        # Show final table counts
        db = UKEducationDB()
        counts = db.get_table_counts()
        print(f"\n Final Table Counts:")
        for table, count in counts.items():
            print(f"   {table}: {count}")
    else:
        print(f"\n Loading failed")