import sqlite3
from typing import List, Dict, Optional, Tuple

class MovieManager:
    
    def __init__(self, db_file: str = "movies.db"):
        self.db_file = db_file
        self.init_database()
    
    def init_database(self):
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    year INTEGER,
                    director TEXT,
                    description TEXT,
                    rating REAL                    
                )
                            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tags(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL
                )
                           """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movie_tags (
                    movie_id INTEGER,
                    tag_id INTEGER,
                    FOREIGN KEY (movie_id) REFERENCES movies (id),
                    FOREIGN KEY (tag_id) REFERENCES tags (id),
                    PRIMARY KEY (movie_id, tag_id)
                )
                            """)
            conn.commit()
            
    def add_movie(self, title: str, year: int, director: str, 
              description: str, rating: float, tags: List[str]):
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO movies (title, year, director, description, rating)
                VALUES (?, ?, ?, ?, ?)
            """, (title, year, director, description, rating))
        
            movie_id = cursor.lastrowid
            
            for tag_name in tags:
                cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
                cursor.execute("SELECT id FROM tags WHERE name = ?", (tag_name,))
                tag_id = cursor.fetchone()[0]
            
                cursor.execute("""
                    INSERT INTO movie_tags (movie_id, tag_id)
                    VALUES (?, ?)
                """, (movie_id, tag_id))
        
            conn.commit()
            
                           