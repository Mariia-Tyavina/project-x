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
            print(f"Фильм '{title}' добавлен")
    
    def search_by_tags(self, tags: List[str], match_all: bool = False) -> List[Dict]:
        with sqlite3.connect(self.db_file) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if not tags:
                return []
            
            tagline = ','.join(['?' for _ in tags])
            
            if match_all:
                cursor.execute(f"""
                    SELECT m.*
                    FROM movies m
                    JOIN movie_tags mt ON m.id = mt.movie_id
                    JOIN tags t ON mt.tag_id = t.id
                    WHERE t.name IN ({tagline})
                    GROUP BY m.id
                    HAVING COUNT(DISTINCT t.id) = ?
                """, tags + [len(tags)])
            else:
                cursor.execute(f"""
                    SELECT DISTINCT m.*
                    FROM movies m
                    JOIN movie_tags mt ON m.id = mt.movie_id
                    JOIN tags t ON mt.tag_id = t.id
                    WHERE t.name IN ({tagline})
                    ORDER BY m.rating DESC
                """, tags)
            
            movies = [dict(row) for row in cursor.fetchall()]
        
            for movie in movies:
                cursor.execute("""
                    SELECT t.name
                    FROM tags t
                    JOIN movie_tags mt ON t.id = mt.tag_id
                    WHERE mt.movie_id = ?
                """, (movie['id'],))
                movie['tags'] = [row[0] for row in cursor.fetchall()]
            
            return movies
        

if __name__ == "__main__":
    manager = MovieManager("test_movies.db")
    # user_title = str(input("Название: "))
    # user_year = int(input("Год выпуска: ")) 
    # user_director = str(input("Режиссер: ")) 
    # user_description = str(input("Описание: ")) 
    # user_rating = float(input("Рейтинг: "))
    # user_tags = str(input("Теги: "))
    
    # manager.add_movie(
    #     title=user_title,
    #     year=user_year,
    #     director=user_director,
    #     description=user_description,
    #     rating=user_rating,
    #     tags=user_tags.split()
    # )    
    print(manager.search_by_tags(['драма', 'триллер'], match_all=False))

                           
