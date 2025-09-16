import pandas as pd
import os

def examine_ks4_performance():
    """Examine the main KS4 performance file in detail"""
    
    performance_file = r"C:\Users\matth\Desktop\UKSchoolData\UKSchoolData\data\dfe\key-stage-4-performance_2023-24\data\202324_performance_tables_schools_final.csv"
    
    print("📊 EXAMINING KS4 PERFORMANCE TABLES (SCHOOLS)")
    print("=" * 60)
    
    try:
        # Read sample to understand structure
        df_sample = pd.read_csv(performance_file, nrows=10)
        
        print(f" File loaded successfully")
        print(f" Sample shape: {df_sample.shape}")
        print(f" Total file size: {os.path.getsize(performance_file) / (1024*1024):.1f} MB")
        
        print(f"\n ALL COLUMNS ({len(df_sample.columns)}):")
        print("-" * 40)
        
        # Group columns by likely purpose for better understanding
        id_cols = [col for col in df_sample.columns if any(x in col.lower() for x in ['urn', 'school', 'la_', 'estab'])]
        performance_cols = [col for col in df_sample.columns if any(x in col.lower() for x in ['att8', 'p8', 'progress', 'attainment', 'ebacc'])]
        pupil_cols = [col for col in df_sample.columns if any(x in col.lower() for x in ['pupil', 't_', 'pt_'])]
        other_cols = [col for col in df_sample.columns if col not in id_cols + performance_cols + pupil_cols]
        
        print(f" IDENTIFIER COLUMNS ({len(id_cols)}):")
        for col in id_cols:
            print(f"   {col}")
            
        print(f"\n PERFORMANCE METRICS ({len(performance_cols)}):")
        for col in performance_cols:
            print(f"   {col}")
            
        print(f"\n PUPIL DATA ({len(pupil_cols)}):")
        for col in pupil_cols[:10]:  # First 10 to avoid spam
            print(f"   {col}")
        if len(pupil_cols) > 10:
            print(f"   ... and {len(pupil_cols)-10} more")
            
        print(f"\n OTHER COLUMNS ({len(other_cols)}):")
        for col in other_cols[:10]:  # First 10 to avoid spam
            print(f"   {col}")
        if len(other_cols) > 10:
            print(f"   ... and {len(other_cols)-10} more")
        
        # Show sample data for key columns
        print(f"\n SAMPLE DATA:")
        print("-" * 30)
        
        key_cols_to_show = []
        if 'school_urn' in df_sample.columns:
            key_cols_to_show.append('school_urn')
        if 'school_name' in df_sample.columns:
            key_cols_to_show.append('school_name')
        
        # Add performance columns
        for col in ['att8scr', 'p8mea', 'att8scr_pp', 'p8mea_pp']:
            if col in df_sample.columns:
                key_cols_to_show.append(col)
                
        if key_cols_to_show:
            print(df_sample[key_cols_to_show].head().to_string(index=False))
        
        # Check data types and missing values
        print(f"\n DATA QUALITY PREVIEW:")
        print("-" * 30)
        
        if 'school_urn' in df_sample.columns:
            print(f"School URNs: {df_sample['school_urn'].nunique()} unique values")
            print(f"Missing URNs: {df_sample['school_urn'].isna().sum()}")
            
        for col in performance_cols[:5]:  # Check first 5 performance columns
            if col in df_sample.columns:
                non_na_count = df_sample[col].notna().sum()
                print(f"{col}: {non_na_count}/{len(df_sample)} non-null values")
        
        print(f"\n Ready to load this data!")
        return True
        
    except Exception as e:
        print(f" Error examining performance file: {e}")
        return False

if __name__ == "__main__":
    examine_ks4_performance()