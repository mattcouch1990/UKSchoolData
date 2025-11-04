from dotenv import load_dotenv
load_dotenv()

from ukeducationdbconnection import UKEducationDB
import pandas as pd
from pathlib import Path
from sqlalchemy import text

print("SCHEMA INSPECTION AND DATA LENGTH ANALYSIS")
print("=" * 60)

db = UKEducationDB()

# 1. Check current table schema
print("\n📋 Current graduate_outcomes table schema:")
schema_query = """
SELECT column_name, data_type, character_maximum_length 
FROM information_schema.columns 
WHERE table_name = 'graduate_outcomes' 
AND table_schema = 'public'
ORDER BY ordinal_position;
"""

schema_result = db.read_sql(schema_query)
print(schema_result)

# 2. Analyze actual data lengths
print("\n📊 Analyzing data lengths from your Excel file:")
print("=" * 60)

file_path = Path("../../data/ons/educationattainmentbydisabilityitl2region2022.xlsx")

# Check each table to find maximum lengths
all_lengths = {}

tables_to_check = ['Table 1', 'Table 2', 'Table 3', 'Table 4', 'Table 5', 'Table 6']

for table_name in tables_to_check:
    print(f"\n🔍 Checking {table_name}:")
    
    df = pd.read_excel(file_path, sheet_name=table_name, skiprows=4)
    
    # Check each string column
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            max_len = df[col].astype(str).str.len().max()
            if col not in all_lengths:
                all_lengths[col] = max_len
            else:
                all_lengths[col] = max(all_lengths[col], max_len)
            
            print(f"   {col}: {max_len} chars")
            if max_len > 20:
                print(f"      🚨 TOO LONG for VARCHAR(20)!")
                longest_values = df[df[col].astype(str).str.len() == max_len][col].unique()[:2]
                print(f"      Examples: {longest_values}")

# Check combined disability_status for Table 6
print(f"\n🔍 Checking Table 6 combined disability_status:")
df6 = pd.read_excel(file_path, sheet_name='Table 6', skiprows=4)
combined = df6['Health Condition'] + ' - ' + df6['Disability Severity']
max_combined = combined.str.len().max()
print(f"   Combined disability_status: {max_combined} chars")
if max_combined > 20:
    print(f"      🚨 TOO LONG for VARCHAR(20)!")
    longest_combined = combined[combined.str.len() == max_combined].unique()[:2]
    print(f"      Examples: {longest_combined}")

print(f"\n📋 SUMMARY - Recommended VARCHAR sizes:")
print("=" * 60)

# Map Excel columns to database columns
column_mapping = {
    'ITL2 Region': 'region',
    'Highest Qualification': 'qualification_level', 
    'Disability Status': 'disability_status',
    'Health Condition': 'disability_status (part)',
    'Sex': 'gender',
    'Age Band': 'age_group'
}

for excel_col, max_len in all_lengths.items():
    if excel_col in column_mapping:
        db_col = column_mapping[excel_col]
        recommended_size = max(max_len + 10, 50)  # Add buffer, minimum 50
        print(f"   {db_col:20} -> VARCHAR({recommended_size:3}) (current max: {max_len})")

# Special case for combined disability_status
recommended_combined = max(max_combined + 10, 100)
print(f"   {'disability_status':20} -> VARCHAR({recommended_combined:3}) (combined health + severity)")

print(f"\n💡 Recommended ALTER TABLE statements:")
print("=" * 60)

alter_statements = [
    "ALTER TABLE graduate_outcomes ALTER COLUMN region TYPE VARCHAR(50);",
    "ALTER TABLE graduate_outcomes ALTER COLUMN qualification_level TYPE VARCHAR(50);", 
    "ALTER TABLE graduate_outcomes ALTER COLUMN disability_status TYPE VARCHAR(100);",
    "ALTER TABLE graduate_outcomes ALTER COLUMN gender TYPE VARCHAR(20);",
    "ALTER TABLE graduate_outcomes ALTER COLUMN age_group TYPE VARCHAR(20);"
]

for stmt in alter_statements:
    print(f"   {stmt}")

print(f"\n🔧 Would you like me to execute these ALTER statements? (y/n)")