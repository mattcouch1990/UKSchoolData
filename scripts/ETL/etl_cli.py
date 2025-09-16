#!/usr/bin/env python3
"""
UK Education Data ETL Command Line Interface

Usage examples:
    python etl_cli.py list                    # List available pipelines
    python etl_cli.py datasets                # List available datasets  
    python etl_cli.py run ks4_performance     # Run with auto-detected file
    python etl_cli.py load path/to/file.csv   # Smart load with auto-detection
    python etl_cli.py history                 # Show load history
    python etl_cli.py status                  # Show database status
"""

import sys
import argparse
from pathlib import Path
from scripts.ETL.etl_pipeline_registery import registry
from scripts.ETL.etl_config import ETLConfig, list_available_datasets, get_latest_ks4_file, get_latest_sen_file
from scripts.ETL.ukeducationdbconnection import UKEducationDB

def create_parser():
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        description="UK Education Data ETL System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List pipelines
    subparsers.add_parser('list', help='List available ETL pipelines')
    
    # List datasets
    subparsers.add_parser('datasets', help='List available datasets')
    
    # Run specific pipeline
    run_parser = subparsers.add_parser('run', help='Run a specific pipeline')
    run_parser.add_argument('pipeline', help='Pipeline name')
    run_parser.add_argument('file', nargs='?', help='File path (optional - will auto-detect latest)')
    run_parser.add_argument('--academic-year', help='Academic year (e.g., 2023-24)')
    
    # Smart load
    load_parser = subparsers.add_parser('load', help='Smart load with auto-detection')
    load_parser.add_argument('file', help='File path')
    load_parser.add_argument('--academic-year', help='Academic year (e.g., 2023-24)')
    
    # History
    subparsers.add_parser('history', help='Show data loading history')
    
    # Status
    subparsers.add_parser('status', help='Show database status')
    
    # Test
    subparsers.add_parser('test', help='Test ETL framework')
    
    return parser

def cmd_list():
    """List available pipelines"""
    registry.list_pipelines()

def cmd_datasets():
    """List available datasets"""
    list_available_datasets()

def cmd_run(pipeline_name, file_path=None, academic_year=None):
    """Run a specific pipeline"""
    # Auto-detect file if not provided
    if not file_path:
        if pipeline_name == 'ks4_performance':
            file_path = get_latest_ks4_file()
        elif pipeline_name == 'sen_statistics':
            file_path = get_latest_sen_file()
        
        if not file_path:
            print(f"No file found for pipeline '{pipeline_name}'")
            return False
        
        print(f"🤖 Auto-detected file: {file_path}")
    
    # Auto-detect academic year if not provided
    if not academic_year:
        academic_year = ETLConfig.auto_detect_academic_year(str(file_path))
        print(f"Auto-detected academic year: {academic_year}")
    
    # Run pipeline
    kwargs = {}
    if academic_year:
        kwargs['academic_year'] = academic_year
    
    try:
        results = registry.run_pipeline(pipeline_name, str(file_path), **kwargs)
        
        print(f"\n PIPELINE RESULTS")
        print("=" * 50)
        print(f"Status: {results['status']}")
        print(f"Duration: {results['duration']}")
        print(f"Total Records: {results.get('total_records', 0)}")
        
        if results['records_loaded']:
            print(f"Records by table:")
            for table, count in results['records_loaded'].items():
                print(f"   {table}: {count:,}")
        
        return results['status'] == 'success'
        
    except Exception as e:
        print(f" Pipeline failed: {e}")
        return False

def cmd_load(file_path, academic_year=None):
    """Smart load with auto-detection"""
    if not Path(file_path).exists():
        print(f"File not found: {file_path}")
        return False
    
    # Auto-detect academic year
    if not academic_year:
        academic_year = ETLConfig.auto_detect_academic_year(file_path)
        print(f"Auto-detected academic year: {academic_year}")
    
    # Smart load
    kwargs = {}
    if academic_year:
        kwargs['academic_year'] = academic_year
    
    try:
        results = registry.smart_load(file_path, **kwargs)
        
        if results:
            print(f"\nSMART LOAD RESULTS")
            print("=" * 50)
            print(f"Status: {results['status']}")
            print(f"Duration: {results['duration']}")
            print(f"Total Records: {results.get('total_records', 0)}")
            
            return results['status'] == 'success'
        else:
            print("Smart load failed - could not detect pipeline")
            return False
            
    except Exception as e:
        print(f"Smart load failed: {e}")
        return False

def cmd_history():
    """Show data loading history"""
    registry.print_data_source_summary()

def cmd_status():
    """Show database status"""
    try:
        db = UKEducationDB()
        
        print("DATABASE STATUS")
        print("=" * 30)
        
        # Connection test
        if db.test_connection():
            print("Database connection: OK")
        else:
            print("Database connection: FAILED")
            return
        
        # Table counts
        counts = db.get_table_counts()
        print(f"\nTable Counts:")
        for table, count in counts.items():
            if count > 0:
                print(f"   {table}: {count:,}")
        
        # Recent loads
        print(f"\ Recent Data Sources:")
        try:
            recent = db.read_sql("""
                SELECT source_name, records_loaded, load_status, created_at 
                FROM data_sources 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            
            for _, row in recent.iterrows():
                status_icon = "✅" if row['load_status'] == 'success' else "❌"
                print(f"   {status_icon} {row['source_name']}: {row['records_loaded']:,} records")
                
        except Exception as e:
            print(f"   Error loading recent sources: {e}")
        
    except Exception as e:
        print(f" Database status check failed: {e}")

def cmd_test():
    """Test ETL framework"""
    from test_etl_framework import test_etl_framework
    test_etl_framework()

def main():
    """Main CLI entry point"""
    parser = create_parser()
    
    if len(sys.argv) == 1:
        # No arguments - show help and available datasets
        print(" UK EDUCATION DATA ETL SYSTEM")
        print("=" * 40)
        parser.print_help()
        print("\n")
        cmd_datasets()
        return
    
    args = parser.parse_args()
    
    # Route commands
    if args.command == 'list':
        cmd_list()
    
    elif args.command == 'datasets':
        cmd_datasets()
    
    elif args.command == 'run':
        success = cmd_run(args.pipeline, args.file, args.academic_year)
        sys.exit(0 if success else 1)
    
    elif args.command == 'load':
        success = cmd_load(args.file, args.academic_year)
        sys.exit(0 if success else 1)
    
    elif args.command == 'history':
        cmd_history()
    
    elif args.command == 'status':
        cmd_status()
    
    elif args.command == 'test':
        cmd_test()
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()