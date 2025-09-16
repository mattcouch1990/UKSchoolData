import os
import pandas as pd

def explore_ks4_files():
    """Undertand struncture of KS4 dataset"""
    
    data_path = r"C:\Users\matth\Desktop\UKSchoolData\UKSchoolData\data\dfe\key-stage-4-performance_2023-24\data"
    
    try:
        # List all CSV files
        csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
        print(f"📋 Found {len(csv_files)} CSV files:")
        
        for i, file in enumerate(sorted(csv_files), 1):
            file_path = os.path.join(data_path, file)
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"   {i:2d}. {file} ({file_size_mb:.1f} MB)")
        
        print("\n" + "="*50)
        
        # Look for key files 
        priority_keywords = ['school', 'institution', 'performance', 'attainment', 'progress']
        
        print("Priority Files (containing keywords")
        priority_files = []
        for file in csv_files:
            file_lower = file.lower()
            if any(keyword in file_lower for keyword in priority_keywords):
                priority_files.append(file)
                print(f"{file}")
        
        if not priority_files:
            print("   No obvious priority files found.")
            priority_files = sorted(csv_files)[:5]
            for file in priority_files:
                print(f"{file}")
        
        # Examine the structure of priority files
        print(f"\n EXAMINING FILE STRUCTURES:")
        print("="*50)
        
        for i, file in enumerate(priority_files):  
            print(f"\n{i+1}. {file}")
            print("-" * (len(file) + 8))
            
            try:
                file_path = os.path.join(data_path, file)
                
                # Read first rows to show structure
                df_sample = pd.read_csv(file_path, nrows=5)
                
                print(f"   Shape: {df_sample.shape}")
                print(f"   Columns ({len(df_sample.columns)}):")
                
                # Show columns in groups for readability
                cols = df_sample.columns.tolist()
                for j in range(0, len(cols), 4):
                    col_group = cols[j:j+4]
                    print(f"     {', '.join(col_group)}")
                
                # Look for key identifiers
                key_cols = []
                for col in df_sample.columns:
                    col_lower = col.lower()
                    if any(key in col_lower for key in ['urn', 'school', 'institution', 'id']):
                        key_cols.append(col)
                
                if key_cols:
                    print(f"Key identifier columns: {key_cols}")
                
                # Show a sample row
                print(f"Sample data:")
                for col in df_sample.columns[:6]:  # First 6 columns
                    if not df_sample[col].isna().all():
                        sample_val = df_sample[col].dropna().iloc[0] if not df_sample[col].dropna().empty else 'N/A'
                        print(f"     {col}: {sample_val}")
                
            except Exception as e:
                print(f"Error reading {file}: {e}")
        
        print(f"\n File exploration complete!")
        
        return priority_files
        
    except Exception as e:
        print(f" Error exploring files: {e}")
        return []

if __name__ == "__main__":
    files = explore_ks4_files()