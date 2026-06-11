import sqlite3
from typing import List, Dict
import requests
from dotenv import load_dotenv
import os
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


load_dotenv()
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')


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
            cursor.execute("PRAGMA table_info(movies)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'poster_path' not in columns:
                cursor.execute("ALTER TABLE movies ADD COLUMN poster_path TEXT")
            conn.commit()
            

    def add_movie(self, title: str, year: int, director: str, 
                description: str, rating: float, tags: List[str],
                poster_path: str = None):
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO movies (title, year, director, description, rating, poster_path)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (title, year, director, description, rating, poster_path))
            movie_id = cursor.lastrowid
            for tag_name in tags:
                cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", 
                               (tag_name,))
                cursor.execute("SELECT id FROM tags WHERE name = ?", 
                               (tag_name,))
                tag_id = cursor.fetchone()[0]
                cursor.execute("""
                    INSERT INTO movie_tags (movie_id, tag_id)
                    VALUES (?, ?)
                """, (movie_id, tag_id))
            conn.commit()
            print(f"Фильм '{title}' добавлен")
    

    def search_by_tags(self, tags: List[str], 
                       match_all: bool = False) -> List[Dict]:
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
       

    def get_all_movies(self) -> List[Dict]:
        with sqlite3.connect(self.db_file) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM movies 
                ORDER BY rating DESC
            """)
            
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
    

    def movie_exists(self, title: str, year: int) -> bool:
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id FROM movies 
                WHERE title = ? AND year = ?
            """, (title, year))
            return cursor.fetchone() is not None
    
           

    def search_by_tmdb(self, searching: str):
        API_KEY = os.getenv('TMDB_API')
        try:
            search_url = "https://api.themoviedb.org/3/search/movie"
            params = {"api_key": API_KEY, "query": searching, "language": "ru-RU"}
            response = requests.get(search_url, params=params)
            results = response.json().get('results', [])
            if not results:
                return False, "Ничего не найдено"      # ← замена print
            movie = results[0]
            movie_id = movie['id']
            details_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
            params = {"api_key": API_KEY, "language": "ru-RU", "append_to_response": "credits"}
            response = requests.get(details_url, params=params)
            full_info = response.json()
            director = "Не указан"
            for crew in full_info.get('credits', {}).get('crew', []):
                if crew.get('job') == 'Director':
                    director = crew.get('name')
                    break
            tags = [genre['name'].lower() for genre in full_info.get('genres', [])]
            year = 0
            if full_info.get('release_date'):
                year = int(full_info['release_date'][:4])
            poster_path = full_info.get('poster_path')
            if self.movie_exists(full_info['title'], year):
                return False, "Фильм уже есть в базе"   # ← замена print
            self.add_movie(
                title=full_info['title'],
                year=year,
                director=director,
                description=full_info.get('overview', 'Нет описания')[:500],
                rating=full_info.get('vote_average', 0),
                tags=tags,
                poster_path=poster_path
            )
            return True, f"Фильм '{full_info['title']}' добавлен"   # ← замена print
        except Exception as e:
            return False, f"Ошибка: {str(e)}"
        
    def create_search_index(self):
        all_movies = self.get_all_movies()
        if not all_movies:
            print("В базе нет фильмов! Сначала добавьте фильмы.")
            return
        descriptions = []
        movie_ids = []
        for movie in all_movies:
            description = movie.get("description", '')
            if not description or description == "Нет описания":
                description = movie.get('title', '')
            descriptions.append(description)
            movie_ids.append(movie['id'])
        description_embeddings = model.encode(descriptions, show_progress_bar=True)
        vector_dimension = description_embeddings.shape[1]
        index = faiss.IndexFlatIP(vector_dimension)
        index_with_ids = faiss.IndexIDMap(index)
        faiss.normalize_L2(description_embeddings)
        index_with_ids.add_with_ids(description_embeddings, np.array(movie_ids))
        index_filename = "movie_search.index"
        faiss.write_index(index_with_ids, index_filename)
        
    def search_by_index(self, query: str, amount = 5):
        exact_matches = []
        lower_query = query.lower()
        all_movies = self.get_all_movies()  # получаем все фильмы с тегами

        for movie in all_movies:
            title_lower = movie['title'].lower()
            desc_lower = movie['description'].lower()
            if lower_query in title_lower or lower_query in desc_lower:
                movie_copy = movie.copy()
                movie_copy['similarity_score'] = 1.0
                exact_matches.append(movie_copy)
                if len(exact_matches) >= amount:
                    break

        index_filename = "movie_search.index"
        semantic_results = []
        if os.path.exists(index_filename):
            index_with_ids = faiss.read_index(index_filename)
            query_embedding = model.encode([query])
            faiss.normalize_L2(query_embedding)
            distances, indices = index_with_ids.search(query_embedding, amount)
            if indices is not None and len(indices) > 0 and len(indices[0]) > 0:
                found_ids = indices[0].tolist()
                found_distances = distances[0].tolist()
                movies_by_id = {movie['id']: movie for movie in all_movies}
                for movie_id, distance in zip(found_ids, found_distances):
                    if movie_id in movies_by_id:
                        movie = movies_by_id[movie_id].copy()
                        movie['similarity_score'] = round(distance, 3)
                        semantic_results.append(movie)
        
        combined = exact_matches.copy()
        existing_ids = {m['id'] for m in exact_matches}
        for movie in semantic_results:
            if movie['id'] not in existing_ids:
                combined.append(movie)

        return combined[:amount]


    def show_index_results(self, results: list, query: str):        
        if not results:
            print(f"\nПо запросу '{query}' ничего не найдено")
            return
        
        print(f"\n{'='*60}")
        print(f"РЕЗУЛЬТАТЫ ПОИСКА ПО ЗАПРОСУ: '{query}'")
        print(f"{'='*60}")
        
        for i, movie in enumerate(results, 1):
            score = movie.get('similarity_score', 0)
            similarity_percent = int(score * 100)
            
            print(f"\nРЕЗУЛЬТАТ #{i} (совпадение: {similarity_percent}%)")
            print(f"Название: {movie['title']} ({movie['year']})")
            print(f"Режиссёр: {movie['director']}")
            print(f"Рейтинг: {movie['rating']}/10")
            print(f"Теги: {', '.join(movie['tags'])}")
            
            description = movie.get('description', '')
            if description and description != "Нет описания":
                print(f"Описание: {description[:150]}...")
            print(f"   {'-'*60}")
        pass
        

