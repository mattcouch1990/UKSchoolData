import pandas as pd
import numpy as np
from scripts.ETL.ukeducationdbconnection import UKEducationDB

def load_schools_and_performance():
    """Load schools first, then performance data"""
    
    performance_file = r"C:\Users\matth\Desktop\UKSchoolData\UKSchoolData\data\dfe\key-stage-4-performance_2023-24\data\202324_performance_tables_schools_final.csv"
    
    print(" LOADING SCHOOLS AND PERFORMANCE DATA")
    print("=" * 50)
    
    try:
        # Load KS4 data
        print(" Loading KS4 data...")
        df = pd.read_csv(performance_file, dtype=str)
        
        # Filter to school level
        df_schools = df[df['geographic_level'] == 'School'].copy()
        print(f" School-level records: {len(df_schools)}")
        
        # Get unique schools with essential info
        school_info = df_schools.groupby('school_urn').first().reset_index()
        print(f" Unique schools: {len(school_info)}")
        
        # Step 1: Load Schools
        print(f"\n STEP 1: Loading Schools")
        print("-" * 30)
        
        schools_to_load = []
        for _, row in school_info.iterrows():
            try:
                school_record = {
                    'urn': int(row['school_urn']),
                    'school_name': row['school_name'][:200] if pd.notna(row['school_name']) else 'Unknown School',
                    'local_authority_code': row['new_la_code'] if pd.notna(row['new_la_code']) else None,
                    'school_type_code': None,  # We'll map this later
                    'phase': 'Secondary',  # KS4 is secondary phase
                    'is_active': True
                }
                schools_to_load.append(school_record)
            except (ValueError, TypeError) as e:
                print(f"  Skipping invalid school URN: {row['school_urn']}")
                continue
        
        print(f" Prepared {len(schools_to_load)} school records")
        
        if schools_to_load:
            db = UKEducationDB()
            schools_df = pd.DataFrame(schools_to_load)
            
            # Remove duplicates just in case
            schools_df = schools_df.drop_duplicates(subset=['urn'])
            print(f" Loading {len(schools_df)} unique schools...")
            
            db.write_dataframe(schools_df, 'schools', if_exists='append')
            print(f" Schools loaded successfully!")
            
            # Verify schools were loaded
            school_count = db.read_sql("SELECT COUNT(*) as count FROM schools")
            print(f" Total schools in database: {school_count.iloc[0]['count']}")
        
        # Step 2: Load Performance Data
        print(f"\n STEP 2: Loading Performance Data")
        print("-" * 30)
        
        # Get one record per school for performance
        perf_data = df_schools.groupby('school_urn').first().reset_index()
        
        performance_records = []
        for _, row in perf_data.iterrows():
            try:
                # Clean numeric values
                def clean_numeric(val):
                    if pd.isna(val) or val in ['z', 'c', 'x', '.']:
                        return None
                    try:
                        return float(val)
                    except:
                        return None
                
                perf_record = {
                    'urn': int(row['school_urn']),
                    'academic_year': '2023-24',
                    'key_stage': 'KS4',
                    'attainment_8_score': clean_numeric(row.get('avg_att8')),
                    'progress_8_score': clean_numeric(row.get('avg_p8score')),
                    'ebacc_average_point_score': clean_numeric(row.get('avg_ebaccaps')),
                    'total_pupils': clean_numeric(row.get('t_pupils'))
                }
                performance_records.append(perf_record)
                
            except (ValueError, TypeError):
                continue
        
        print(f" Prepared {len(performance_records)} performance records")
        
        if performance_records:
            perf_df = pd.DataFrame(performance_records)
            db.write_dataframe(perf_df, 'school_performance', if_exists='append')
            print(f" Performance data loaded successfully!")
            
            # Show summary stats
            print(f"\n PERFORMANCE SUMMARY:")
            if 'attainment_8_score' in perf_df.columns:
                att8_stats = perf_df['attainment_8_score'].describe()
                print(f"   Attainment 8: Mean {att8_stats['mean']:.1f}, Range {att8_stats['min']:.1f}-{att8_stats['max']:.1f}")
            
            if 'progress_8_score' in perf_df.columns:
                p8_stats = perf_df['progress_8_score'].describe()
                print(f"   Progress 8: Mean {p8_stats['mean']:.2f}, Range {p8_stats['min']:.2f}-{p8_stats['max']:.2f}")
        
        # Record data source
        load_record = pd.DataFrame([{
            'source_name': 'DfE Key Stage 4 Performance + Schools',
            'source_url': 'https://explore-education-statistics.service.gov.uk/find-statistics/key-stage-4-performance/2023-24',
            'file_name': '202324_performance_tables_schools_final.csv',
            'download_date': pd.Timestamp.now().date(),
            'academic_year': '2023-24',
            'records_loaded': len(performance_records) if performance_records else 0,
            'load_status': 'success'
        }])
        
        db.write_dataframe(load_record, 'data_sources', if_exists='append')
        print(f" Data source recorded")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = load_schools_and_performance()
    
    if success:
        print(f"\n Successful Completion")
        
        # Final verification
        db = UKEducationDB()
        counts = db.get_table_counts()
        print(f"\n📊 FINAL TABLE COUNTS:")
        for table, count in counts.items():
            if count > 0:
                print(f"   {table}: {count}")
    else:
        print(f"\n Loading failed")