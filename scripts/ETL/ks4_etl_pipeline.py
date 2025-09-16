from scripts.ETL.base_ETL_framework import BaseETLPipeline, DataQualityMixin
import pandas as pd
from typing import Dict

class KS4PerformancePipeline(BaseETLPipeline, DataQualityMixin):
    """ETL Pipeline for Key Stage 4 Performance Data"""
    
    def __init__(self, academic_year: str = '2023-24'):
        super().__init__(
            source_name='DfE Key Stage 4 Performance Tables',
            source_url='https://explore-education-statistics.service.gov.uk/find-statistics/key-stage-4-performance',
            academic_year=academic_year
        )
    
    def extract(self, file_path: str) -> pd.DataFrame:
        """Extract KS4 performance data from CSV"""
        self.logger.info(f"Loading KS4 data from {file_path}")
        
        # Load with string types to avoid casting issues
        df = pd.read_csv(file_path, dtype=str, low_memory=False)
        
        # Filter to school-level data only
        if 'geographic_level' in df.columns:
            df = df[df['geographic_level'] == 'School'].copy()
            self.logger.info(f"Filtered to {len(df)} school-level records")
        
        return df
    
    def transform(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Transform KS4 data to database schema"""
        self.logger.info("Transforming KS4 data...")
        
        # Get unique schools first
        school_data = self._transform_schools(df)
        
        # Get performance data
        performance_data = self._transform_performance(df)
        
        return {
            'schools': school_data,
            'school_performance': performance_data
        }
    
    def _transform_schools(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform school information"""
        # Get unique schools
        schools = df.groupby('school_urn').first().reset_index()
        
        school_records = []
        for _, row in schools.iterrows():
            try:
                record = {
                    'urn': int(row['school_urn']),
                    'school_name': str(row['school_name'])[:200] if pd.notna(row['school_name']) else 'Unknown School',
                    'local_authority_code': row.get('new_la_code'),
                    'school_type_code': None,  # Map later from establishment_type_group
                    'phase': 'Secondary',  # KS4 is secondary
                    'is_active': True
                }
                school_records.append(record)
            except (ValueError, TypeError):
                continue
        
        schools_df = pd.DataFrame(school_records)
        
        # Data quality checks
        schools_df = self.check_urn_validity(schools_df)
        
        self.logger.info(f"Transformed {len(schools_df)} school records")
        return schools_df
    
    def _transform_performance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform performance metrics"""
        # Get one record per school (first occurrence for overall performance)
        performance = df.groupby('school_urn').first().reset_index()
        
        perf_records = []
        for _, row in performance.iterrows():
            try:
                record = {
                    'urn': int(row['school_urn']),
                    'academic_year': self.academic_year,
                    'key_stage': 'KS4',
                    'attainment_8_score': self.clean_numeric_column(pd.Series([row.get('avg_att8')]))[0],
                    'progress_8_score': self.clean_numeric_column(pd.Series([row.get('avg_p8score')]))[0],
                    'ebacc_average_point_score': self.clean_numeric_column(pd.Series([row.get('avg_ebaccaps')]))[0],
                    'ebacc_entries_percentage': self.clean_numeric_column(pd.Series([row.get('pt_ebacc_95')]))[0],
                    'grade_5_english_maths_percentage': self.clean_numeric_column(pd.Series([row.get('pt_l2basics_95')]))[0],
                    'grade_4_english_maths_percentage': self.clean_numeric_column(pd.Series([row.get('pt_l2basics_94')]))[0],
                    'total_pupils': self.clean_numeric_column(pd.Series([row.get('t_pupils')]), integer=True)[0],
                    'pupils_included_progress_8': self.clean_numeric_column(pd.Series([row.get('t_inp8calc')]), integer=True)[0],
                    'pupils_included_attainment_8': self.clean_numeric_column(pd.Series([row.get('t_att8')]), integer=True)[0]
                }
                perf_records.append(record)
            except (ValueError, TypeError):
                continue
        
        perf_df = pd.DataFrame(perf_records)
        
        # Data quality checks
        perf_df = self.check_urn_validity(perf_df)
        
        # Check percentage ranges
        percentage_cols = ['ebacc_entries_percentage', 'grade_5_english_maths_percentage', 'grade_4_english_maths_percentage']
        perf_df = self.check_percentage_ranges(perf_df, percentage_cols)
        
        # Check score ranges
        score_ranges = {
            'attainment_8_score': (0, 90),  # Typical A8 range
            'progress_8_score': (-3, 3),    # Typical P8 range
            'ebacc_average_point_score': (0, 9)  # EBacc APS range
        }
        perf_df = self.check_score_ranges(perf_df, score_ranges)
        
        self.logger.info(f"Transformed {len(perf_df)} performance records")
        return perf_df
    
    def get_load_config(self) -> Dict[str, Dict]:
        """Configuration for loading each table"""
        return {
            'schools': {
                'drop_duplicates': ['urn'],
                'if_exists': 'append'
            },
            'school_performance': {
                'drop_duplicates': ['urn', 'academic_year', 'key_stage'],
                'if_exists': 'append',
                'pre_load_transform': self._filter_existing_schools
            }
        }
    
    def _filter_existing_schools(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter performance data to only schools that exist in database"""
        existing_schools = self.db.read_sql("SELECT urn FROM schools")
        existing_urns = set(existing_schools['urn'].tolist())
        
        initial_count = len(df)
        df_filtered = df[df['urn'].isin(existing_urns)]
        
        if len(df_filtered) < initial_count:
            self.logger.info(f"Filtered to {len(df_filtered)} records matching existing schools")
        
        return df_filtered

# Example usage function
def run_ks4_pipeline(file_path: str):
    """Run the KS4 ETL pipeline"""
    pipeline = KS4PerformancePipeline()
    results = pipeline.run(file_path)
    
    print(f"\n KS4 ETL PIPELINE RESULTS")
    print("=" * 40)
    print(f"Status: {results['status']}")
    print(f"Duration: {results['duration']}")
    print(f"Total Records: {results.get('total_records', 0)}")
    
    if results['records_loaded']:
        print(f"Records by table:")
        for table, count in results['records_loaded'].items():
            print(f"   {table}: {count}")
    
    if results['warnings']:
        print(f"\nWarnings:")
        for warning in results['warnings']:
            print(f"{warning}")
    
    if results['errors']:
        print(f"\nErrors:")
        for error in results['errors']:
            print(f"{error}")
    
    return results

if __name__ == "__main__":
    # Example usage
    file_path = r"C:\Users\matth\Desktop\UKSchoolData\UKSchoolData\data\dfe\key-stage-4-performance_2023-24\data\202324_performance_tables_schools_final.csv"
    
    results = run_ks4_pipeline(file_path)
    
    if results['status'] == 'success':
        print(f"\n KS4 Pipeline completed successfully!")
    else:
        print(f"\n KS4 Pipeline failed")