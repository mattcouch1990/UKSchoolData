import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import RealDictCursor

class UKEducationDB:
    """Database connection manager for UK Education Analytics project"""
    
    def __init__(self):
        # Configure Database
        self.host = "localhost" #Required
        self.port = 5432 #Specify Port
        self.database = "uk_education_analytics" #DB Name
        self.username = "uk_edu_user" # DB User Note: not superhost
        self.password = os.getenv('UK_EDU_DB_PASSWORD') #Automatically get password without hardcoding
        
        if not self.password:
            raise ValueError("Database password not found!") # false password message
        
        # Creatw SQLAlchemy engine for pandas integration
        self.connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.engine = create_engine(self.connection_string)


    def test_connection(self):
        """Check to see if we can access the db"""
        try:
            # Try connection with psycopg2
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT version();")
                #return db version
                db_version = cursor.fetchone()['version']
                
                cursor.execute("SELECT current_database(), current_user;")
                #get db and user
                current_info = cursor.fetchone()
                
                # Count tables
                cursor.execute("""
                    SELECT COUNT(*) as table_count 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """)
                table_count = cursor.fetchone()['table_count']
                
                # Check reference data
                cursor.execute("SELECT COUNT(*) as sen_count FROM sen_categories;")
                sen_count = cursor.fetchone()['sen_count']
                
                cursor.execute("SELECT COUNT(*) as school_types_count FROM school_types;")
                school_types_count = cursor.fetchone()['school_types_count']
            
            conn.close()
            
            return {
                'status': 'success',
                'database': current_info['current_database'],
                'user': current_info['current_user'],
                'version': db_version,
                'table_count': table_count,
                'sen_categories': sen_count,
                'school_types': school_types_count
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
        
    def get_table_info(self, table_name):
        """Run SQL command to get information from table"""
        sql = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
        """
        return pd.read_sql(sql, self.engine, params=[table_name])
    
    def get_table_counts(self):
        """Get row counts for all tables"""
        # List of tables to test
        tables = ['schools', 'school_performance', 'sen_pupils', 'ofsted_inspections', 
                 'graduate_outcomes', 'sen_categories', 'school_types', 'local_authorities']
        
        counts = {}
        for table in tables:
            try:
                df = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", self.engine)
                counts[table] = df.iloc[0]['count']
            except:
                counts[table] = 'Error'
        
        return counts
    
    def read_sql(self, query, params=None):
        """Execute SQL query and return pandas DataFrame"""
        return pd.read_sql(query, self.engine, params=params)
    
    def write_dataframe(self, df, table_name, if_exists='append'):
        """Write pandas DataFrame to database table"""
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, chunksize=1000)

if __name__ == "__main__":
    print("Testing UK Education Database Connection...")
    print("******************************************")
    
    try:
        db = UKEducationDB()
        result = db.test_connection()
        
        if result['status'] == 'success':
            print(f"Database: {result['database']}")
            print(f"User: {result['user']}")
            print(f"Tables Created: {result['table_count']}")
            print(f"SEN Categories: {result['sen_categories']}")
            print(f"School Types: {result['school_types']}")
            
            print("Current Table Counts:")
            counts = db.get_table_counts()
            for table, count in counts.items():
                print(f"   {table}: {count}")
            
            print()
            print("All checks passed")
            
        else:
            print("Database Connection Failed!")
            print(f"Error: {result['error']}")
            
    except Exception as e:
        print(f"Connection Error: {e}")

        