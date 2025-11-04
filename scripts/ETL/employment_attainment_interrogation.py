import pandas as pd
from pathlib import Path

print("INSPECTING REGIONAL EDUCATION ATTAINMENT FILE")
print("=" * 70)

file_path = Path("../../data/ons/educationattainmentbydisabilityitl2region2022.xlsx")

if not file_path.exists():
    print(f"File not found: {file_path}")
    exit(1)

print(f"\n📂 File: {file_path.name}")

# Load Excel file
excel_file = pd.ExcelFile(file_path)

print(f"\n📑 Total sheets: {len(excel_file.sheet_names)}")
for i, sheet in enumerate(excel_file.sheet_names, 1):
    print(f"   {i}. {sheet}")

# Focus on tables 1-6
tables = ['Table 1', 'Table 2', 'Table 3', 'Table 4', 'Table 5', 'Table 6']

for table_name in tables:
    if table_name not in excel_file.sheet_names:
        print(f"\n⚠️  {table_name} not found in sheets")
        continue
    
    print(f"\n{'='*70}")
    print(f"📋 {table_name.upper()}")
    print(f"{'='*70}")
    
    # Load the table (skip first 4 rows, data starts on row 5)
    df = pd.read_excel(file_path, sheet_name=table_name, skiprows=4)
    
    print(f"\nDimensions: {len(df):,} rows × {len(df.columns)} columns")
    
    # Column names
    print(f"\nColumn Names:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i}. {col}")
    
    # Sample data (first 5 rows)
    print(f"\nSample Data (first 5 rows):")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)
    print(df.head(5))
    
    # Unique values for key columns (helps understand the data)
    print(f"\nUnique Values Analysis:")
    
    for col in df.columns:
        unique_count = df[col].nunique()
        
        # Show unique values if there aren't too many
        if unique_count <= 20:
            unique_vals = df[col].unique()
            print(f"\n   {col}: ({unique_count} unique values)")
            for val in unique_vals[:20]:
                print(f"      - {val}")
        else:
            print(f"\n   {col}: ({unique_count} unique values - too many to list)")
            # Show just first 10
            print(f"      First 10:")
            for val in df[col].unique()[:10]:
                print(f"      - {val}")
    
    # Data types
    print(f"\nData Types:")
    for col in df.columns:
        print(f"   {col}: {df[col].dtype}")
    
    # Check for nulls
    print(f"\nNull Counts:")
    null_counts = df.isnull().sum()
    for col in df.columns:
        if null_counts[col] > 0:
            print(f"   {col}: {null_counts[col]:,} nulls ({null_counts[col]/len(df)*100:.1f}%)")
    if null_counts.sum() == 0:
        print("   No null values found")

print(f"\n{'='*70}")
print("SUMMARY FOR LOADER DESIGN")
print(f"{'='*70}")

print("""
Based on this inspection, we can now design a loader that:
1. Handles all 6 tables properly
2. Captures disability severity (Table 4)
3. Captures health condition (Table 5)
4. Captures both (Table 6)
5. Stores them appropriately in graduate_outcomes table

Key information needed:
- Which columns contain: Region, Disability Status, Qualification, Counts
- How to handle Table 4's severity column
- How to handle Table 5's health condition column
- How to handle Table 6's combined columns
- Whether to concatenate into disability_status or create a custom solution
""")

print("\n✅ Inspection complete!")
print("Review the output above to design the proper loader.")