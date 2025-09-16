import os
from typing import Dict, List, Type
from pathlib import Path
import pandas as pd
from scripts.ETL.ukeducationdbconnection import UKEducationDB

class PipelineRegistry:
    """Registry for managing all ETL pipelines"""
    
    def __init__(self):
        self.pipelines = {}
        self.db = UKEducationDB()
    
    def register_pipeline(self, name: str, pipeline_class: Type, description: str = ""):
        """Register an ETL pipeline"""
        self.pipelines[name] = {
            'class': pipeline_class,
            'description': description,
            'registered_at': pd.Timestamp.now()
        }
        print(f"✅ Registered pipeline: {name}")
    
    def list_pipelines(self):
        """List all registered pipelines"""
        print("REGISTERED ETL PIPELINES")
        print("=" * 40)
        
        for name, info in self.pipelines.items():
            print(f"{name}")
            if info['description']:
                print(f"   Description: {info['description']}")
            print(f"   Class: {info['class'].__name__}")
            print(f"   Registered: {info['registered_at'].strftime('%Y-%m-%d %H:%M')}")
            print()
    
    def run_pipeline(self, name: str, file_path: str, **kwargs):
        """Run a specific pipeline"""
        if name not in self.pipelines:
            raise ValueError(f"Pipeline '{name}' not found. Available: {list(self.pipelines.keys())}")
        
        pipeline_class = self.pipelines[name]['class']
        pipeline = pipeline_class(**kwargs)
        
        print(f" Running pipeline: {name}")
        results = pipeline.run(file_path)
        
        return results
    
    def get_data_source_history(self):
        """Get history of data loads"""
        try:
            history = self.db.read_sql("""
                SELECT 
                    source_name,
                    file_name,
                    academic_year,
                    records_loaded,
                    load_status,
                    created_at
                FROM data_sources 
                ORDER BY created_at DESC
            """)
            return history
        except Exception as e:
            print(f"Error getting data source history: {e}")
            return pd.DataFrame()
    
    def print_data_source_summary(self):
        """Print summary of all data loads"""
        history = self.get_data_source_history()
        
        if history.empty:
            print("No data sources loaded yet")
            return
        
        print("DATA SOURCE SUMMARY")
        print("=" * 50)
        
        total_records = history['records_loaded'].sum()
        successful_loads = len(history[history['load_status'] == 'success'])
        total_loads = len(history)
        
        print(f"Total Records Loaded: {total_records:,}")
        print(f"Successful Loads: {successful_loads}/{total_loads}")
        print(f"Success Rate: {successful_loads/total_loads*100:.1f}%")
        
        print(f"\n📋 Recent Loads:")
        for _, row in history.head(10).iterrows():
            status_icon = "✅" if row['load_status'] == 'success' else "❌"
            print(f"   {status_icon} {row['source_name']}: {row['records_loaded']:,} records ({row['created_at']})")
    
    def auto_detect_pipeline(self, file_path: str) -> str:
        """Auto-detect which pipeline to use based on file path/name"""
        file_path = Path(file_path)
        file_name = file_path.name.lower()
        parent_dir = file_path.parent.name.lower()
        
        # Detection rules
        if 'key-stage-4' in str(file_path).lower() or 'performance_tables' in file_name:
            return 'ks4_performance'
        elif 'special-educational-needs' in str(file_path).lower() or 'sen_' in file_name:
            return 'sen_statistics'
        elif 'ofsted' in str(file_path).lower() or 'inspection' in file_name:
            return 'ofsted_inspections'
        elif 'graduate' in file_name or 'labour' in file_name:
            return 'graduate_outcomes'
        
        return None
    
    def smart_load(self, file_path: str, pipeline_name: str = None, **kwargs):
        """Smart loading with auto-detection"""
        if pipeline_name is None:
            pipeline_name = self.auto_detect_pipeline(file_path)
            if pipeline_name:
                print(f"Auto-detected pipeline: {pipeline_name}")
            else:
                print("Could not auto-detect pipeline type")
                self.list_pipelines()
                return None
        
        return self.run_pipeline(pipeline_name, file_path, **kwargs)

# Global registry instance
registry = PipelineRegistry()

# Register our KS4 pipeline
try:
    from scripts.ETL.ks4_etl_pipeline import KS4PerformancePipeline
    registry.register_pipeline(
        'ks4_performance', 
        KS4PerformancePipeline, 
        'Key Stage 4 (GCSE) Performance Data - School level results'
    )
except ImportError:
    print("KS4 pipeline not available")

def main():
    """Main CLI interface for the ETL system"""
    import sys
    
    if len(sys.argv) < 2:
        print("🔧 UK EDUCATION ETL SYSTEM")
        print("=" * 30)
        print("Commands:")
        print("  list                    - List all pipelines")
        print("  history                 - Show data load history")
        print("  run <pipeline> <file>   - Run specific pipeline")
        print("  smart <file>            - Auto-detect and run pipeline")
        print()
        registry.list_pipelines()
        return
    
    command = sys.argv[1].lower()
    
    if command == 'list':
        registry.list_pipelines()
    
    elif command == 'history':
        registry.print_data_source_summary()
    
    elif command == 'run' and len(sys.argv) >= 4:
        pipeline_name = sys.argv[2]
        file_path = sys.argv[3]
        results = registry.run_pipeline(pipeline_name, file_path)
        
        if results['status'] == 'success':
            print(f"\n Pipeline completed successfully!")
        else:
            print(f"\n Pipeline failed")
    
    elif command == 'smart' and len(sys.argv) >= 3:
        file_path = sys.argv[2]
        results = registry.smart_load(file_path)
        
        if results and results['status'] == 'success':
            print(f"\n Smart load completed successfully!")
        else:
            print(f"\n Smart load failed")
    
    else:
        print(" Invalid command or missing arguments")

if __name__ == "__main__":
    main()