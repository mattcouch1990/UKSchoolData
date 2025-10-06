from dotenv import load_dotenv
load_dotenv()

from ukeducationdbconnection import UKEducationDB
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

class OfstedInspectionLoader:
    """Load Ofsted inspection data into the database"""
    
    def __init__(self):
        self.db = UKEducationDB()
        self.data_file = Path("../../data/ofsted/State_funded_schools_inspections_and_outcomes_as_at_31_December_2024.csv")
        self.encoding = 'utf-8'  # Will be detected during inspection
        
    def inspect_file(self):
        """Inspect the CSV file structure"""
        print("INSPECTING OFSTED DATA FILE")
        print("=" * 70)
        
        if not self.data_file.exists():
            print(f"Error: File not found at {self.data_file}")
            return False
        
        print(f"\nFile: {self.data_file.name}")
        print(f"Size: {self.data_file.stat().st_size / (1024*1024):.2f} MB")
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                print(f"Trying encoding: {encoding}")
                df = pd.read_csv(self.data_file, nrows=5, encoding=encoding)
                print(f"Success with {encoding}")
                self.encoding = encoding
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            print("Error: Could not read file with any standard encoding")
            return False
        
        print(f"\nColumns ({len(df.columns)}):")
        for col in df.columns:
            print(f"   - {col}")
        
        print(f"\nSample data:")
        pd.set_option('display.max_columns', 10)
        pd.set_option('display.width', None)
        print(df.head(3))
        
        return True
    
    def get_column_mapping(self):
        """Map CSV columns to database columns"""
        
        # First, let's see what columns are actually in the file
        df_sample = pd.read_csv(self.data_file, nrows=1, encoding=self.encoding)
        print("\nAvailable columns in CSV:")
        for i, col in enumerate(df_sample.columns, 1):
            print(f"{i:2d}. {col}")
        
        # Map CSV columns to database columns (updated for actual schema)
        column_mapping = {
            'URN': 'urn',
            'Inspection start date': 'inspection_date',
            'Publication date': 'publication_date',
            'Inspection type': 'inspection_type',
            'Overall effectiveness': 'overall_effectiveness',
            'Quality of education': 'quality_of_education',
            'Behaviour and attitudes': 'behaviour_and_attitudes',
            'Personal development': 'personal_development',
            'Effectiveness of leadership and management': 'leadership_and_management',
            'Previous inspection start date': 'previous_inspection_date',
            'Previous graded inspection overall effectiveness': 'previous_overall_rating'  # Note: rating not effectiveness
        }
        
        # Note: school_name and safeguarding_is_effective are NOT in the database table
        # special_measures and serious_weaknesses would need to be derived if needed
        
        return column_mapping
    
    def load_data(self, batch_size=1000, dry_run=False):
        """Load Ofsted data into database"""
        
        print("\n" + "=" * 70)
        print("LOADING OFSTED INSPECTION DATA")
        print("=" * 70)
        
        if not self.data_file.exists():
            print(f"Error: File not found")
            return False
        
        # Read the CSV - try multiple encodings if needed
        print(f"\nReading CSV file...")
        print(f"Attempting with detected encoding: {self.encoding}")
        
        df = None
        encodings_to_try = [self.encoding, 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings_to_try:
            try:
                print(f"Trying {encoding}...")
                df = pd.read_csv(self.data_file, low_memory=False, encoding=encoding, encoding_errors='replace')
                print(f"Success! Loaded {len(df):,} rows with {encoding}")
                self.encoding = encoding  # Update to working encoding
                break
            except Exception as e:
                print(f"Failed with {encoding}: {str(e)[:100]}")
                continue
        
        if df is None:
            print("Error: Could not read file with any encoding")
            return False
        
        # Show actual columns for user to verify mapping
        print(f"\nActual columns in file:")
        for col in df.columns[:15]:
            print(f"   - {col}")
        
        # Get column mapping
        column_mapping = self.get_column_mapping()
        
        # Check which columns exist
        print(f"\nChecking column mapping:")
        available_mappings = {}
        for csv_col, db_col in column_mapping.items():
            if csv_col in df.columns:
                available_mappings[csv_col] = db_col
                print(f"   Found: {csv_col} -> {db_col}")
            else:
                print(f"   Missing: {csv_col}")
        
        if not available_mappings:
            print("\nWarning: No expected columns found!")
            print("The file structure may have changed.")
            print("\nWould you like to see the actual file structure and create a custom mapping?")
            return False
        
        # Rename columns
        df_mapped = df[list(available_mappings.keys())].copy()
        df_mapped.columns = [available_mappings[col] for col in df_mapped.columns]
        
        print(f"\nMapped {len(df_mapped.columns)} columns successfully")
        
        # Handle date columns with UK date format (DD/MM/YYYY)
        date_columns = ['inspection_date', 'publication_date', 'previous_inspection_date']
        for col in date_columns:
            if col in df_mapped.columns:
                df_mapped[col] = pd.to_datetime(df_mapped[col], format='%d/%m/%Y', errors='coerce')
        
        # Clean data
        print("\nCleaning data...")
        
        # Ensure URN is integer
        if 'urn' in df_mapped.columns:
            df_mapped['urn'] = pd.to_numeric(df_mapped['urn'], errors='coerce')
            df_mapped = df_mapped.dropna(subset=['urn'])
            df_mapped['urn'] = df_mapped['urn'].astype(int)
        
        # Convert rating columns to integers
        rating_columns = [
            'overall_effectiveness', 
            'quality_of_education', 
            'behaviour_and_attitudes',
            'personal_development',
            'leadership_and_management',
            'previous_overall_rating'
        ]
        
        for col in rating_columns:
            if col in df_mapped.columns:
                # Convert to numeric, treating 9 as NULL (not applicable)
                df_mapped[col] = pd.to_numeric(df_mapped[col], errors='coerce')
                df_mapped[col] = df_mapped[col].replace(9.0, pd.NA)  # 9 = Not applicable
                df_mapped[col] = df_mapped[col].astype('Int64')  # Nullable integer type
        
        # Truncate inspection_type to 50 characters (database limit)
        if 'inspection_type' in df_mapped.columns:
            df_mapped['inspection_type'] = df_mapped['inspection_type'].astype(str).str[:50]
        
        # Add metadata columns
        df_mapped['created_at'] = datetime.now()
        df_mapped['updated_at'] = datetime.now()
        
        print(f"After cleaning: {len(df_mapped):,} rows")
        
        # Filter to only include URNs that exist in schools table
        print(f"\nFiltering to schools that exist in database...")
        existing_urns_query = "SELECT DISTINCT urn FROM schools"
        existing_urns_df = self.db.read_sql(existing_urns_query)
        existing_urns = set(existing_urns_df['urn'].tolist())
        
        print(f"Schools in database: {len(existing_urns):,}")
        print(f"Inspections before filtering: {len(df_mapped):,}")
        
        df_mapped = df_mapped[df_mapped['urn'].isin(existing_urns)]
        
        print(f"Inspections after filtering: {len(df_mapped):,}")
        print(f"Filtered out: {21991 - len(df_mapped):,} inspections for schools not in database")
        
        # Remove rows with NULL inspection_date (required field)
        print(f"\nRemoving records with missing inspection_date...")
        before_count = len(df_mapped)
        df_mapped = df_mapped.dropna(subset=['inspection_date'])
        after_count = len(df_mapped)
        print(f"Removed {before_count - after_count} records with NULL inspection_date")
        print(f"Final record count: {after_count:,}")
        
        if dry_run:
            print("\nDRY RUN - Not loading to database")
            print("\nSample of prepared data:")
            print(df_mapped.head())
            return True
        
        # Load to database
        print(f"\nLoading to database...")
        
        try:
            # Clear existing data
            clear = input("Clear existing ofsted_inspections data? (y/n): ").strip().lower()
            if clear == 'y':
                from sqlalchemy import text
                delete_query = text("DELETE FROM ofsted_inspections")
                with self.db.engine.connect() as conn:
                    conn.execute(delete_query)
                    conn.commit()
                print(f"Cleared existing data")
            
            # Load in batches
            total_batches = (len(df_mapped) + batch_size - 1) // batch_size
            
            for i in range(0, len(df_mapped), batch_size):
                batch = df_mapped.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                self.db.write_dataframe(
                    batch,
                    'ofsted_inspections',
                    if_exists='append'
                )
                
                print(f"Loaded batch {batch_num}/{total_batches} ({len(batch)} rows)")
            
            print(f"\nSuccess! Loaded {len(df_mapped):,} inspection records")
            
            # Verify load
            count_query = "SELECT COUNT(*) as count FROM ofsted_inspections"
            result = self.db.read_sql(count_query)
            print(f"Database now contains {result['count'][0]:,} inspection records")
            
            return True
            
        except Exception as e:
            print(f"\nError loading data: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def validate_with_ge(self):
        """Validate loaded data with Great Expectations"""
        
        print("\n" + "=" * 70)
        print("VALIDATING WITH GREAT EXPECTATIONS")
        print("=" * 70)
        
        try:
            import great_expectations as gx
            
            context = gx.get_context()
            
            # We would need to add ofsted_inspections as a data asset first
            print("\nNote: ofsted_inspections needs to be added as a data asset")
            print("This would be done in the Great Expectations configuration")
            
            # For now, just do basic SQL validation
            print("\nRunning basic validation queries...")
            
            queries = {
                'Total records': 'SELECT COUNT(*) as count FROM ofsted_inspections',
                'Unique schools': 'SELECT COUNT(DISTINCT urn) as count FROM ofsted_inspections',
                'Date range': 'SELECT MIN(inspection_date) as earliest, MAX(inspection_date) as latest FROM ofsted_inspections',
                'Null URNs': 'SELECT COUNT(*) as count FROM ofsted_inspections WHERE urn IS NULL'
            }
            
            for name, query in queries.items():
                result = self.db.read_sql(query)
                print(f"\n{name}:")
                print(result)
            
            return True
            
        except Exception as e:
            print(f"Validation error: {e}")
            return False

def main():
    print("OFSTED INSPECTION DATA LOADER")
    print("=" * 70)
    
    loader = OfstedInspectionLoader()
    
    # Step 1: Inspect file
    if not loader.inspect_file():
        sys.exit(1)
    
    # Step 2: Ask user what to do
    print("\n" + "=" * 70)
    print("What would you like to do?")
    print("1. Dry run (inspect data without loading)")
    print("2. Load data to database")
    print("3. Just validate existing data")
    
    choice = input("\nChoice (1-3): ").strip()
    
    if choice == '1':
        loader.load_data(dry_run=True)
    elif choice == '2':
        if loader.load_data(dry_run=False):
            loader.validate_with_ge()
    elif choice == '3':
        loader.validate_with_ge()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()