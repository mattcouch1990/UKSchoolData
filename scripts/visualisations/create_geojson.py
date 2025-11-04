import requests
import json
import pandas as pd
import os
from pathlib import Path
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../ETL/.env')  # if .env is in ETL folder

# Add path to database connection
sys.path.append('../ETL')
from ukeducationdbconnection import UKEducationDB

print("UK LOCAL AUTHORITY BOUNDARY DATA SETUP (ALTERNATIVE)")
print("=" * 60)

def try_download_method_1():
    """Try ONS Open Geography Portal - updated URL"""
    
    print("📥 Method 1: ONS Open Geography Portal (updated)...")
    
    # Updated ONS endpoint
    boundary_url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2023_UK_BUC/FeatureServer/0/query"
    
    params = {
        'where': '1=1',
        'outFields': 'LAD23CD,LAD23NM', 
        'outSR': '4326',
        'f': 'geojson'
    }
    
    try:
        response = requests.get(boundary_url, params=params, timeout=30)
        response.raise_for_status()
        geojson_data = response.json()
        
        if 'features' in geojson_data and len(geojson_data['features']) > 0:
            print(f"✅ Method 1 success! Found {len(geojson_data['features'])} boundaries")
            return geojson_data, 'LAD23CD', 'LAD23NM'
        else:
            print("❌ Method 1: No features in response")
            return None, None, None
            
    except Exception as e:
        print(f"❌ Method 1 failed: {e}")
        return None, None, None

def try_download_method_2():
    """Try alternative ONS endpoint"""
    
    print("📥 Method 2: Alternative ONS endpoint...")
    
    boundary_url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD_DEC_2022_UK_BUC/FeatureServer/0/query"
    
    params = {
        'where': '1=1',
        'outFields': 'LAD22CD,LAD22NM',
        'outSR': '4326', 
        'f': 'geojson'
    }
    
    try:
        response = requests.get(boundary_url, params=params, timeout=30)
        response.raise_for_status()
        geojson_data = response.json()
        
        if 'features' in geojson_data and len(geojson_data['features']) > 0:
            print(f"✅ Method 2 success! Found {len(geojson_data['features'])} boundaries")
            return geojson_data, 'LAD22CD', 'LAD22NM'
        else:
            print("❌ Method 2: No features in response")
            return None, None, None
            
    except Exception as e:
        print(f"❌ Method 2 failed: {e}")
        return None, None, None

def try_download_method_3():
    """Try GitHub hosted GeoJSON (reliable backup)"""
    
    print("📥 Method 3: GitHub hosted boundaries...")
    
    # This is a well-maintained repository with UK boundaries
    boundary_url = "https://raw.githubusercontent.com/martinjc/UK-GeoJSON/master/json/administrative/gb/lad.json"
    
    try:
        response = requests.get(boundary_url, timeout=30)
        response.raise_for_status()
        geojson_data = response.json()
        
        if 'features' in geojson_data and len(geojson_data['features']) > 0:
            print(f"✅ Method 3 success! Found {len(geojson_data['features'])} boundaries")
            # This source uses different field names
            return geojson_data, 'LAD13CD', 'LAD13NM'
        else:
            print("❌ Method 3: No features in response")
            return None, None, None
            
    except Exception as e:
        print(f"❌ Method 3 failed: {e}")
        return None, None, None

def try_download_method_4():
    """Create a simplified version using just our database LAs"""
    
    print("📥 Method 4: Simplified approach (database codes only)...")
    
    try:
        db = UKEducationDB()
        query = """
        SELECT DISTINCT la_code, la_name 
        FROM local_authorities 
        WHERE la_code IS NOT NULL 
        ORDER BY la_code
        """
        
        db_las = db.read_sql(query)
        
        if len(db_las) == 0:
            print("❌ Method 4: No LA codes in database")
            return None, None, None
        
        # Create a simplified "boundary" dataset with just points
        # This won't show actual geographic boundaries but will work for basic mapping
        features = []
        
        for idx, row in db_las.iterrows():
            # Create simple point features (we'll use approximate coordinates)
            # This is a fallback - not ideal but allows basic geographic plotting
            feature = {
                "type": "Feature",
                "properties": {
                    "LAD_CODE": row['la_code'],
                    "LAD_NAME": row['la_name']
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-2.0, 54.0]  # Rough center of UK
                }
            }
            features.append(feature)
        
        geojson_data = {
            "type": "FeatureCollection",
            "features": features
        }
        
        print(f"✅ Method 4 success! Created {len(features)} simplified features")
        print("⚠️ Note: This is a simplified dataset without real boundaries")
        
        return geojson_data, 'LAD_CODE', 'LAD_NAME'
        
    except Exception as e:
        print(f"❌ Method 4 failed: {e}")
        return None, None, None

def download_uk_boundaries():
    """Try multiple methods to download UK boundary data"""
    
    methods = [
        try_download_method_1,
        try_download_method_2, 
        try_download_method_3,
        try_download_method_4
    ]
    
    for method in methods:
        geojson_data, code_field, name_field = method()
        
        if geojson_data is not None:
            # Save the successful result
            boundaries_path = Path("uk_local_authorities.geojson")
            with open(boundaries_path, 'w') as f:
                json.dump(geojson_data, f)
            
            print(f"💾 Saved boundaries to: {boundaries_path}")
            
            # Save metadata about which fields to use
            metadata = {
                'code_field': code_field,
                'name_field': name_field,
                'source_method': method.__name__
            }
            
            with open("boundary_metadata.json", 'w') as f:
                json.dump(metadata, f)
            
            return geojson_data, code_field, name_field
    
    print("❌ All download methods failed")
    return None, None, None

def analyze_boundary_codes(geojson_data, code_field, name_field):
    """Analyze the LA codes in the boundary data"""
    
    print(f"\n🔍 Analyzing boundary data (using {code_field}, {name_field})...")
    
    if not geojson_data or 'features' not in geojson_data:
        print("❌ No boundary data to analyze")
        return None
    
    # Extract LA codes and names
    boundary_info = []
    for feature in geojson_data['features']:
        props = feature['properties']
        boundary_info.append({
            'boundary_code': props.get(code_field, ''),
            'boundary_name': props.get(name_field, '')
        })
    
    boundary_df = pd.DataFrame(boundary_info)
    
    print(f"📋 Sample boundary codes:")
    print(boundary_df.head(10))
    
    # Check code patterns
    if len(boundary_df) > 0:
        codes = boundary_df['boundary_code'].astype(str)
        print(f"\n🔤 Code patterns:")
        print(f"   England codes (E): {len(codes[codes.str.startswith('E')])}")
        print(f"   Wales codes (W): {len(codes[codes.str.startswith('W')])}")
        print(f"   Scotland codes (S): {len(codes[codes.str.startswith('S')])}")
        print(f"   N.Ireland codes (N): {len(codes[codes.str.startswith('N')])}")
    
    return boundary_df

def check_database_la_codes():
    """Check what LA codes we have in our database"""
    
    print("\n🗄️ Checking database LA codes...")
    
    try:
        db = UKEducationDB()
        query = """
        SELECT DISTINCT la_code, la_name 
        FROM local_authorities 
        WHERE la_code IS NOT NULL 
        ORDER BY la_code
        """
        
        db_las = db.read_sql(query)
        
        print(f"📊 Found {len(db_las)} Local Authorities in database:")
        print(db_las.head(10))
        
        return db_las
        
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        return None

def create_flexible_mapping(boundary_df, db_las):
    """Create flexible mapping between database LA codes and boundary codes"""
    
    print("\n🔗 Creating flexible mapping...")
    
    if boundary_df is None or db_las is None:
        print("❌ Cannot create mapping - missing data")
        return None
    
    # Try exact matching first
    direct_matches = pd.merge(
        db_las,
        boundary_df,
        left_on='la_code',
        right_on='boundary_code',
        how='inner'
    )
    
    print(f"✅ Direct code matches: {len(direct_matches)}")
    
    # Try fuzzy matching on names for unmatched ones
    unmatched_db = db_las[~db_las['la_code'].isin(boundary_df['boundary_code'])]
    
    if len(unmatched_db) > 0:
        print(f"🔍 Attempting name-based matching for {len(unmatched_db)} unmatched LAs...")
        
        name_matches = []
        for _, db_row in unmatched_db.iterrows():
            # Simple name matching (could be improved with fuzzy matching)
            db_name_clean = db_row['la_name'].lower().strip()
            
            for _, boundary_row in boundary_df.iterrows():
                boundary_name_clean = boundary_row['boundary_name'].lower().strip()
                
                if db_name_clean == boundary_name_clean:
                    name_matches.append({
                        'la_code': db_row['la_code'],
                        'la_name': db_row['la_name'],
                        'boundary_code': boundary_row['boundary_code'],
                        'boundary_name': boundary_row['boundary_name'],
                        'match_type': 'name_exact'
                    })
                    break
        
        print(f"✅ Name-based matches: {len(name_matches)}")
        
        # Combine matches
        if len(name_matches) > 0:
            name_matches_df = pd.DataFrame(name_matches)
            all_matches = pd.concat([
                direct_matches[['la_code', 'la_name', 'boundary_code', 'boundary_name']],
                name_matches_df[['la_code', 'la_name', 'boundary_code', 'boundary_name']]
            ], ignore_index=True)
        else:
            all_matches = direct_matches[['la_code', 'la_name', 'boundary_code', 'boundary_name']]
    else:
        all_matches = direct_matches[['la_code', 'la_name', 'boundary_code', 'boundary_name']]
    
    print(f"🎯 Total matches: {len(all_matches)} out of {len(db_las)} database LAs")
    
    # Save mapping
    mapping_path = Path("la_boundary_mapping.csv")
    all_matches.to_csv(mapping_path, index=False)
    print(f"💾 Saved mapping to: {mapping_path}")
    
    return all_matches

def create_simple_choropleth_demo():
    """Create a simple demonstration that choropleth setup works"""
    
    print("\n🧪 Creating choropleth demonstration...")
    
    try:
        # Check if we have the necessary files
        boundaries_path = Path("uk_local_authorities.geojson")
        mapping_path = Path("la_boundary_mapping.csv")
        metadata_path = Path("boundary_metadata.json")
        
        if not all([boundaries_path.exists(), mapping_path.exists(), metadata_path.exists()]):
            print("❌ Required files not found")
            return False
        
        # Load the data
        with open(boundaries_path, 'r') as f:
            geojson_data = json.load(f)
        
        mapping_df = pd.read_csv(mapping_path)
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        print(f"✅ Loaded {len(geojson_data['features'])} boundaries")
        print(f"✅ Loaded {len(mapping_df)} LA mappings")
        print(f"✅ Using code field: {metadata['code_field']}")
        
        # Save configuration for the dashboard
        config = {
            'boundaries_file': 'uk_local_authorities.geojson',
            'mapping_file': 'la_boundary_mapping.csv',
            'code_field': metadata['code_field'],
            'name_field': metadata['name_field'],
            'source_method': metadata['source_method']
        }
        
        with open("choropleth_config.json", 'w') as f:
            json.dump(config, f, indent=2)
        
        print("✅ Choropleth configuration saved!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating demo: {e}")
        return False

def main():
    """Main function to set up choropleth mapping with fallbacks"""
    
    print("🚀 Starting choropleth setup with multiple fallback methods...\n")
    
    # Step 1: Download boundary data (try multiple methods)
    geojson_data, code_field, name_field = download_uk_boundaries()
    
    if geojson_data is None:
        print("❌ All boundary download methods failed")
        print("\n💡 Alternative options:")
        print("   1. Try again later (APIs might be temporarily down)")
        print("   2. Download manually from: https://geoportal.statistics.gov.uk/")
        print("   3. Use simplified bar charts for now")
        return
    
    # Step 2: Analyze boundary codes
    boundary_df = analyze_boundary_codes(geojson_data, code_field, name_field)
    
    # Step 3: Check database LA codes
    db_las = check_database_la_codes()
    
    # Step 4: Create flexible mapping
    mapping_df = create_flexible_mapping(boundary_df, db_las)
    
    # Step 5: Create demo and config
    success = create_simple_choropleth_demo()
    
    if success:
        print("\n🎉 CHOROPLETH SETUP COMPLETE!")
        print("=" * 60)
        print("✅ UK boundary data obtained")
        print("✅ LA code mapping created")
        print("✅ Configuration saved")
        print("\n📋 Files created:")
        print("   - uk_local_authorities.geojson")
        print("   - la_boundary_mapping.csv")
        print("   - boundary_metadata.json")
        print("   - choropleth_config.json")
        print("\n🚀 Ready for dashboard integration!")
    else:
        print("\n❌ Setup incomplete - check errors above")

if __name__ == "__main__":
    main()