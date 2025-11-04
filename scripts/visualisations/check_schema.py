import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../ETL/.env')  # if .env is in ETL folder

# Add path to database connection
sys.path.append('../ETL')
from ukeducationdbconnection import UKEducationDB

print("CHECKING SCHOOL_PERFORMANCE TABLE SCHEMA")
print("=" * 50)

db = UKEducationDB()

# Check table schema
schema_query = """
SELECT column_name, data_type, is_nullable, character_maximum_length
FROM information_schema.columns 
WHERE table_name = 'school_performance'
AND table_schema = 'public'
ORDER BY ordinal_position;
"""

print("📋 School Performance table columns:")
schema_result = db.read_sql(schema_query)
print(schema_result)

# Check sample data
print(f"\n📊 Sample data from school_performance table:")
sample_query = "SELECT * FROM school_performance LIMIT 5"
sample_result = db.read_sql(sample_query)
print(sample_result)

print(f"\n📈 Table stats:")
count_query = "SELECT COUNT(*) as total_rows FROM school_performance"
count_result = db.read_sql(count_query)
print(f"Total rows: {count_result['total_rows'][0]}")

# Check for URN overlap with schools table
print(f"\n🔗 Checking URN overlap with schools:")
overlap_query = """
SELECT 
    COUNT(DISTINCT s.urn) as schools_count,
    COUNT(DISTINCT sp.urn) as performance_count,
    COUNT(DISTINCT s.urn) FILTER (WHERE sp.urn IS NOT NULL) as overlap_count
FROM schools s
FULL OUTER JOIN school_performance sp ON s.urn = sp.urn
"""
overlap_result = db.read_sql(overlap_query)
print(overlap_result)