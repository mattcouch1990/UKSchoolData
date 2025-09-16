import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path
from datetime import datetime
from scripts.ETL.ukeducationdbconnection import UKEducationDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class BaseETLPipeline(ABC):
    """
    Class for all pipelines in projects allowing for shared functionality
    """
    
    def __init__(self, source_name: str, source_url: str, academic_year: str):
        self.source_name = source_name
        self.source_url = source_url
        self.academic_year = academic_year
        self.logger = logging.getLogger(f"ETL.{self.__class__.__name__}")
        self.db = UKEducationDB()
        self.errors = []
        self.warnings = []
        
    @abstractmethod
    def extract(self, file_path: str) -> pd.DataFrame:
        """Extracts data from source"""
        pass
        
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Transforms data to match db schema"""
        pass
        
    @abstractmethod
    def get_load_config(self) -> Dict[str, Dict]:
        """Return configuration for loading each table"""
        pass
    
    def validate_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """Data validation"""
        if df.empty:
            self.errors.append(f"Empty dataset for {table_name}")
            return False
            
        # Check for required columns based on table
        required_cols = self._get_required_columns(table_name)
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            self.errors.append(f"Missing required columns for {table_name}: {missing_cols}")
            return False
            
        return True
    
    def clean_dfe_codes(self, series: pd.Series) -> pd.Series:
        """Clean DfE standard codes (z, c, SUPP, etc.)"""
        dfe_codes = ['z', 'c', 'x', '.', 'SUPP', 'NE', 'LOW', 'HIGH']
        cleaned = series.copy()
        for code in dfe_codes:
            cleaned = cleaned.replace(code, np.nan)
        return cleaned
    
    def clean_numeric_column(self, series: pd.Series, integer: bool = False) -> pd.Series:
        """Clean and convert numeric columns"""
        cleaned = self.clean_dfe_codes(series)
        numeric = pd.to_numeric(cleaned, errors='coerce')
        
        if integer:
            return numeric
        return numeric
    
    def load(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Load data to database tables"""
        load_config = self.get_load_config()
        results = {}
        
        for table_name, df in transformed_data.items():
            if table_name not in load_config:
                self.warnings.append(f"No load config for table: {table_name}")
                continue
                
            config = load_config[table_name]
            
            try:
                # Validate data
                if not self.validate_data(df, table_name):
                    continue
                
                # Apply any pre-load transformations
                if 'pre_load_transform' in config:
                    df = config['pre_load_transform'](df)
                
                # Handle duplicates
                if config.get('drop_duplicates'):
                    initial_count = len(df)
                    if isinstance(config['drop_duplicates'], list):
                        df = df.drop_duplicates(subset=config['drop_duplicates'])
                    else:
                        df = df.drop_duplicates()
                    
                    if len(df) < initial_count:
                        self.logger.info(f"Removed {initial_count - len(df)} duplicates from {table_name}")
                
                # Load to database
                if_exists = config.get('if_exists', 'append')
                self.db.write_dataframe(df, table_name, if_exists=if_exists)
                
                results[table_name] = len(df)
                self.logger.info(f"Loaded {len(df)} records to {table_name}")
                
            except Exception as e:
                error_msg = f"Failed to load {table_name}: {str(e)}"
                self.errors.append(error_msg)
                self.logger.error(error_msg)
                results[table_name] = 0
        
        return results
    
    def record_data_source(self, records_loaded: int, status: str = 'success'):
        """Record the data load in data_sources table"""
        source_record = pd.DataFrame([{
            'source_name': self.source_name,
            'source_url': self.source_url,
            'file_name': getattr(self, 'current_file', 'Unknown'),
            'download_date': datetime.now().date(),
            'academic_year': self.academic_year,
            'records_loaded': records_loaded,
            'load_status': status,
            'error_message': '; '.join(self.errors) if self.errors else None
        }])
        
        try:
            self.db.write_dataframe(source_record, 'data_sources', if_exists='append')
            self.logger.info("Data source recorded")
        except Exception as e:
            self.logger.error(f"Failed to record data source: {e}")
    
    def run(self, file_path: str) -> Dict[str, Any]:
        """Run the complete ETL pipeline"""
        self.logger.info(f"Starting ETL pipeline for {self.source_name}")
        self.current_file = Path(file_path).name
        
        start_time = datetime.now()
        results = {
            'status': 'success',
            'start_time': start_time,
            'records_loaded': {},
            'errors': [],
            'warnings': []
        }
        
        try:
            # Extract
            self.logger.info("Starting extraction...")
            raw_data = self.extract(file_path)
            self.logger.info(f"Extracted {len(raw_data)} raw records")
            
            # Transform
            self.logger.info("Starting transformation...")
            transformed_data = self.transform(raw_data)
            
            # Load
            self.logger.info("Starting load...")
            load_results = self.load(transformed_data)
            
            # Record results
            total_records = sum(load_results.values())
            self.record_data_source(total_records)
            
            results['records_loaded'] = load_results
            results['total_records'] = total_records
            
        except Exception as e:
            error_msg = f"ETL pipeline failed: {str(e)}"
            self.errors.append(error_msg)
            self.logger.error(error_msg)
            results['status'] = 'failed'
            self.record_data_source(0, 'failed')
        
        # Finalize results
        results['end_time'] = datetime.now()
        results['duration'] = results['end_time'] - start_time
        results['errors'] = self.errors
        results['warnings'] = self.warnings
        
        self.logger.info(f"ETL pipeline completed: {results['status']}")
        return results
    
    def _get_required_columns(self, table_name: str) -> List[str]:
        """Get required columns for each table"""
        required_columns = {
            'schools': ['urn', 'school_name'],
            'school_performance': ['urn', 'academic_year', 'key_stage'],
            'sen_pupils': ['academic_year', 'sen_provision'],
            'local_authorities': ['la_code', 'la_name', 'region'],
            'ofsted_inspections': ['urn', 'inspection_date']
        }
        return required_columns.get(table_name, [])

class DataQualityMixin:
    
    def check_urn_validity(self, df: pd.DataFrame, urn_col: str = 'urn') -> pd.DataFrame:
        """Validate URN format and values"""
        if urn_col not in df.columns:
            return df
            
        initial_count = len(df)
        
        # Remove non-numeric URNs
        df[urn_col] = pd.to_numeric(df[urn_col], errors='coerce')
        df = df.dropna(subset=[urn_col])
        
        # URNs should be 6-7 digits
        df = df[(df[urn_col] >= 100000) & (df[urn_col] <= 9999999)]
        
        if len(df) < initial_count:
            self.warnings.append(f"Removed {initial_count - len(df)} records with invalid URNs")
        
        return df
    
    def check_percentage_ranges(self, df: pd.DataFrame, percentage_cols: List[str]) -> pd.DataFrame:
        """Validate percentage columns are within 0-100 range"""
        for col in percentage_cols:
            if col in df.columns:
                invalid_mask = (df[col] < 0) | (df[col] > 100)
                invalid_count = invalid_mask.sum()
                
                if invalid_count > 0:
                    self.warnings.append(f"Found {invalid_count} invalid percentages in {col}")
                    df.loc[invalid_mask, col] = np.nan
        
        return df
    
    def check_score_ranges(self, df: pd.DataFrame, score_configs: Dict[str, tuple]) -> pd.DataFrame:
        """Validate score columns are within expected ranges"""
        for col, (min_val, max_val) in score_configs.items():
            if col in df.columns:
                invalid_mask = (df[col] < min_val) | (df[col] > max_val)
                invalid_count = invalid_mask.sum()
                
                if invalid_count > 0:
                    self.warnings.append(f"Found {invalid_count} out-of-range values in {col}")
                    df.loc[invalid_mask, col] = np.nan
        
        return df