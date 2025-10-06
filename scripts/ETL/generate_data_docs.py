from dotenv import load_dotenv
load_dotenv()

import great_expectations as gx
from pathlib import Path
import webbrowser
import os

print("FINDING AND REBUILDING DATA DOCS")
print("=" * 70)

context = gx.get_context()

# Step 1: Find where GE is actually storing things
print("\n1️⃣ Locating Great Expectations directories...")
print("=" * 70)

# Check the context's root directory
try:
    context_root = context.root_directory
    print(f"\nGE Context Root: {context_root}")
except:
    context_root = None
    print("\nCould not determine context root")

# Search for all possible GE directories
search_paths = [
    Path.home() / ".great_expectations",
    Path.home() / "gx",
    Path.cwd() / "great_expectations",
    Path.cwd() / "gx",
    Path.cwd().parent / "great_expectations",
    Path.cwd().parent / "gx",
]

print("\nSearching for GE directories:")
ge_dirs = []
for path in search_paths:
    if path.exists():
        print(f"   Found: {path}")
        ge_dirs.append(path)
        
        # Look for data_docs
        data_docs_path = path / "uncommitted" / "data_docs" / "local_site"
        if data_docs_path.exists():
            print(f"      Has data_docs at: {data_docs_path}")

if not ge_dirs:
    print("   No GE directories found")

# Step 2: Run validations with checkpoints
print("\n2️⃣ Running validations using checkpoints...")
print("=" * 70)

# Create a comprehensive checkpoint
checkpoint_name = "data_quality_checkpoint"

try:
    # Try to get existing checkpoint or create new one
    try:
        checkpoint = context.get_checkpoint(checkpoint_name)
        print(f"Using existing checkpoint: {checkpoint_name}")
    except:
        print(f"Creating new checkpoint: {checkpoint_name}")
        checkpoint = context.add_or_update_checkpoint(
            name=checkpoint_name,
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "uk_education_db",
                        "data_asset_name": "schools"
                    },
                    "expectation_suite_name": "schools_quality"
                },
                {
                    "batch_request": {
                        "datasource_name": "uk_education_db",
                        "data_asset_name": "school_performance"
                    },
                    "expectation_suite_name": "performance_quality"
                },
                {
                    "batch_request": {
                        "datasource_name": "uk_education_db",
                        "data_asset_name": "ofsted_inspections"
                    },
                    "expectation_suite_name": "ofsted_quality"
                }
            ]
        )
    
    print("\nRunning checkpoint (this validates all datasets)...")
    checkpoint_result = checkpoint.run()
    
    print(f"\nCheckpoint Results:")
    print(f"   Success: {checkpoint_result['success']}")
    
    for run_id, validation_result in checkpoint_result['run_results'].items():
        suite_name = validation_result['validation_result']['meta']['expectation_suite_name']
        success = validation_result['validation_result']['success']
        stats = validation_result['validation_result']['statistics']
        
        status = "PASS" if success else "FAIL"
        print(f"\n   [{status}] {suite_name}")
        print(f"      Success Rate: {stats['success_percent']:.1f}%")
        print(f"      Passed: {stats['successful_expectations']}/{stats['evaluated_expectations']}")
    
except Exception as e:
    print(f"Error with checkpoint: {e}")
    import traceback
    traceback.print_exc()

# Step 3: Build Data Docs
print("\n3️⃣ Building Data Docs...")
print("=" * 70)

try:
    build_result = context.build_data_docs()
    print("Data Docs built successfully!")
    
    if build_result:
        print("\nData Docs locations:")
        for site_name, site_dict in build_result.items():
            print(f"   {site_name}:")
            if 'local_path' in site_dict:
                print(f"      Local: {site_dict['local_path']}")
            
except Exception as e:
    print(f"Error building Data Docs: {e}")
    import traceback
    traceback.print_exc()

# Step 4: Find and open the index.html
print("\n4️⃣ Finding Data Docs index.html...")
print("=" * 70)

# Search all found GE directories for index.html
index_paths = []
for ge_dir in ge_dirs:
    for index_file in ge_dir.rglob("index.html"):
        if "data_docs" in str(index_file):
            index_paths.append(index_file)
            print(f"Found: {index_file}")

if index_paths:
    # Use the most recently modified one
    latest_index = max(index_paths, key=lambda p: p.stat().st_mtime)
    print(f"\nOpening most recent: {latest_index}")
    
    # Open in browser
    try:
        webbrowser.open(f"file:///{latest_index.absolute()}")
        print("Data Docs should now be open in your browser!")
        
        print(f"\nDirect path to copy:")
        print(f"{latest_index.absolute()}")
        
    except Exception as e:
        print(f"Could not auto-open: {e}")
        print(f"\nManually open this file:")
        print(f"{latest_index.absolute()}")
else:
    print("No index.html found in data_docs directories")

print("\n" + "=" * 70)
print("NAVIGATION TIPS")
print("=" * 70)
print("""
Once in Data Docs, you should see:

1. HOME PAGE - Overview of all validations
   - Recent validation results
   - Links to expectation suites
   
2. VALIDATION RESULTS - Click on a timestamp
   - Pass/fail charts
   - Detailed metrics
   - Observed values vs expected values
   
3. EXPECTATION SUITES - Click suite names
   - List of all expectations
   - Validation history
   
If you only see expectation definitions:
   - Click on "Validation Results" in the sidebar
   - Or click on a recent timestamp on the home page
   
The validation results pages have:
   - Charts showing pass/fail rates
   - Statistics about your data
   - Details on what passed and what failed
""")