import os
from pathlib import Path

class ETLConfig:
    """Configuration settings for the ETL framework"""
    
    # Database settings
    DATABASE_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'uk_education_analytics',
        'username': 'uk_edu_user'
    }
    
    # Data paths
    DATA_ROOT = Path(r"C:\Users\matth\Desktop\UKSchoolData\UKSchoolData\data")
    DFE_DATA_PATH = DATA_ROOT / "dfe"
    ONS_DATA_PATH = DATA_ROOT / "ons" 
    OFSTED_DATA_PATH = DATA_ROOT / "ofsted"
    
    # File patterns for auto-detection
    FILE_PATTERNS = {
        'ks4_performance': [
            '**/key-stage-4-performance*/**/*performance_tables*schools*.csv',
            '**/ks4*/**/*performance*.csv'
        ],
        'sen_statistics': [
            '**/special-educational-needs*/**/*sen*.csv',
            '**/special-educational-needs*/**/*sen*.ods'
        ],
        'ofsted_inspections': [
            '**/ofsted*/**/*inspection*.csv',
            '**/maintained-schools*/**/*inspection*.csv'
        ],
        'graduate_outcomes': [
            '**/graduate-labour-markets*/**/*.csv',
            '**/longer-term-destinations*/**/*.csv'
        ]
    }
    
    # Data quality settings
    QUALITY_CHECKS = {
        'urn_range': (100000, 9999999),
        'percentage_range': (0, 100),
        'progress_8_range': (-3, 3),
        'attainment_8_range': (0, 90),
        'ebacc_aps_range': (0, 9)
    }
    
    # DfE standard codes to clean
    DFE_CODES = ['z', 'c', 'x', '.', 'SUPP', 'NE', 'LOW', 'HIGH', 'LOWCOV']
    
    # Logging configuration
    LOGGING_CONFIG = {
        'level': 'INFO',
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'log_file': 'etl_pipeline.log'
    }
    
    # Academic year mappings
    ACADEMIC_YEARS = {
        '202324': '2023-24',
        '202223': '2022-23',
        '202122': '2021-22'
    }
    
    @classmethod
    def get_data_path(cls, source_type: str) -> Path:
        """Get data path for a specific source type"""
        path_map = {
            'dfe': cls.DFE_DATA_PATH,
            'ons': cls.ONS_DATA_PATH,
            'ofsted': cls.OFSTED_DATA_PATH
        }
        return path_map.get(source_type, cls.DATA_ROOT)
    
    @classmethod
    def find_files(cls, pattern: str, source_type: str = None) -> list:
        """Find files matching a pattern"""
        if source_type:
            search_path = cls.get_data_path(source_type)
        else:
            search_path = cls.DATA_ROOT
            
        return list(search_path.glob(pattern))
    
    @classmethod
    def auto_detect_academic_year(cls, file_path: str) -> str:
        """Auto-detect academic year from file path"""
        file_path_str = str(file_path)
        
        for code, year in cls.ACADEMIC_YEARS.items():
            if code in file_path_str:
                return year
        
        # Default to current academic year
        return '2023-24'

# Example usage and helper functions
def get_latest_ks4_file():
    """Get the latest KS4 performance file"""
    patterns = ETLConfig.FILE_PATTERNS['ks4_performance']
    files = []
    
    for pattern in patterns:
        found_files = ETLConfig.find_files(pattern)
        files.extend(found_files)
    
    if files:
        # Return the largest file (most comprehensive)
        return max(files, key=lambda f: f.stat().st_size)
    return None

def get_latest_sen_file():
    """Get the latest SEN statistics file"""
    patterns = ETLConfig.FILE_PATTERNS['sen_statistics']
    files = []
    
    for pattern in patterns:
        found_files = ETLConfig.find_files(pattern)
        files.extend(found_files)
    
    if files:
        return max(files, key=lambda f: f.stat().st_size)
    return None

def list_available_datasets():
    """List all available datasets"""
    print(" Datasets")
    print("=" * 40)
    
    for dataset_type, patterns in ETLConfig.FILE_PATTERNS.items():
        print(f"\n {dataset_type.upper()}:")
        
        all_files = []
        for pattern in patterns:
            files = ETLConfig.find_files(pattern)
            all_files.extend(files)
        
        if all_files:
            for file in all_files:
                size_mb = file.stat().st_size / (1024 * 1024)
                academic_year = ETLConfig.auto_detect_academic_year(str(file))
                print(f" {file.name} ({size_mb:.1f} MB, {academic_year})")
                print(f" {file}")
        else:
            print("No files found")

if __name__ == "__main__":
    # Test configuration
    print("ETL CONFIGURATION TEST")
    print("=" * 30)
    
    print(f"Data root: {ETLConfig.DATA_ROOT}")
    print(f"DfE path: {ETLConfig.DFE_DATA_PATH}")
    
    # Test file finding
    ks4_file = get_latest_ks4_file()
    if ks4_file:
        print(f"Latest KS4 file: {ks4_file.name}")
        print(f"Academic year: {ETLConfig.auto_detect_academic_year(str(ks4_file))}")
    
    # List all datasets
    list_available_datasets()