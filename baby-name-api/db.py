import sqlite3
from config import DATABASE_PATH

class BabyNameDatabase:
    def __init__(self):
        self.db_path = DATABASE_PATH
    
    def connect(self):
        """Create connection to database"""
        return sqlite3.connect(self.db_path)
    
    def create_tables(self):
        """Create the tables from schema"""
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
        conn.commit()
        conn.close()
    
    def load_data_from_csv(self, csv_file):
        """Load all data from SSA CSV file"""
        conn = self.connect()
        cursor = conn.cursor()
        
        with open(csv_file, 'r') as f:
            next(f)  # Skip header
            for line in f:
                name, year, gender, count = line.strip().split(',')
                try:
                    cursor.execute('''
                        INSERT INTO baby_names (name, year, gender, count)
                        VALUES (?, ?, ?, ?)
                    ''', (name, int(year), gender, int(count)))
                except sqlite3.IntegrityError:
                    pass  # Skip duplicates
        
        conn.commit()
        conn.close()
    
    def get_name_stats(self, name):
        """Get stats for a specific name"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Get all records for this name
        cursor.execute('''
            SELECT year, COUNT(count) as total_count
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
