import sqlite3
import os
from config import DATABASE_PATH, CSV_DATA_PATH

class BabyNameDatabase:
    def __init__(self):
        self.db_path = DATABASE_PATH
    
    def connect(self):
        """Create connection to database"""
        return sqlite3.connect(self.db_path)
    
    def create_tables(self):
        """Create the tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS baby_names (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                year INTEGER NOT NULL,
                gender TEXT NOT NULL,
                count INTEGER NOT NULL,
                UNIQUE(name, year, gender)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_name ON baby_names(name)
        ''')
        
        conn.commit()
        conn.close()
    
    def load_data_from_csv(self):
        """Load all data from SSA CSV files in babynames folder"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Get all CSV files in babynames folder
        babynames_dir = CSV_DATA_PATH
        
        if not os.path.exists(babynames_dir):
            print(f"Error: {babynames_dir} folder not found")
            return False
        
        csv_files = [f for f in os.listdir(babynames_dir) if f.endswith('.csv')]
        
        if not csv_files:
            print(f"Error: No CSV files found in {babynames_dir}")
            return False
        
        loaded_count = 0
        
        for csv_file in csv_files:
            file_path = os.path.join(babynames_dir, csv_file)
            print(f"Loading {csv_file}...")
            
            try:
                with open(file_path, 'r') as f:
                    # Skip header
                    header = f.readline()
                    
                    for line in f:
                        parts = line.strip().split(',')
                        
                        if len(parts) < 4:
                            continue
                        
                        name = parts[0].strip()
                        year = parts[1].strip()
                        gender = parts[2].strip()
                        count = parts[3].strip()
                        
                        try:
                            year = int(year)
                            count = int(count)
                            
                            cursor.execute('''
                                INSERT INTO baby_names (name, year, gender, count)
                                VALUES (?, ?, ?, ?)
                            ''', (name, year, gender, count))
                            
                            loaded_count += 1
                        except (ValueError, sqlite3.IntegrityError):
                            pass
            
            except Exception as e:
                print(f"Error loading {csv_file}: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        print(f"Successfully loaded {loaded_count} records!")
        return True
    
    def get_name_stats(self, name):
        """Get stats for a specific name"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Get all records for this name
        cursor.execute('''
            SELECT year, SUM(count) as total_count
            FROM baby_names
            WHERE LOWER(name) = LOWER(?)
            GROUP BY year
            ORDER BY year
        ''', (name,))
        
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            return None
        
        # First year = smallest year
        first_year = results[0][0]
        
        # Most popular year = year with highest count
        most_popular = max(results, key=lambda x: x[1])
        most_popular_year = most_popular[0]
        
        # Top 10 years by count
        sorted_years = sorted(results, key=lambda x: x[1], reverse=True)[:10]
        top_10 = [year[0] for year in sorted_years]
        
        return {
            'first_year': first_year,
            'most_popular_year': most_popular_year,
            'top_10_years': top_10
        }
    
    def add_name(self, name, year, gender, count):
        """CREATE: Add a new record"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO baby_names (name, year, gender, count)
                VALUES (?, ?, ?, ?)
            ''', (name, year, gender, count))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()
    
    def update_count(self, name, year, gender, new_count):
        """UPDATE: Modify a record"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE baby_names
            SET count = ?
            WHERE LOWER(name) = LOWER(?) AND year = ? AND gender = ?
        ''', (new_count, name, year, gender))
        conn.commit()
        rows_affected = cursor.rowcount
        conn.close()
        return rows_affected > 0
    
    def delete_record(self, name, year, gender):
        """DELETE: Remove a record"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM baby_names
            WHERE LOWER(name) = LOWER(?) AND year = ? AND gender = ?
        ''', (name, year, gender))
        conn.commit()
        rows_affected = cursor.rowcount
        conn.close()
        return rows_affected > 0
    
    def search_name(self, name):
        """SEARCH: Find all records for a name"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT name, year, gender, count
            FROM baby_names
            WHERE LOWER(name) = LOWER(?)
            ORDER BY year
        ''', (name,))
        results = cursor.fetchall()
        conn.close()
        return results
