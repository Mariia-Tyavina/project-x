import sqlite3
from typing import List


connect = sqlite3.connect('table_of_colors.db')
cursor = connect.cursor()


cursor.execute("""
    CREATE TABLE IF NOT EXISTS colors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tags TEXT UNIQUE NOT NULL
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS color_tags (
        color_id INTEGER,
        tag_id INTEGER,
        FOREIGN KEY (color_id) REFERENCES colors (id),
        FOREIGN KEY (tag_id) REFERENCES tags (id),
        PRIMARY KEY (color_id, tag_id)
    )
                """)
connect.commit()


def submit(color: str, tags: List[str]):
    cursor.execute("SELECT id FROM colors WHERE name = ?", (color,))
    existing_color = cursor.fetchone()
    
    if existing_color:
        color_id = existing_color[0]
    else:
        cursor.execute("""
            INSERT INTO colors (name)
            VALUES (?)
        """, (color,))
        color_id = cursor.lastrowid
    
    for tag_name in tags:
        cursor.execute("INSERT OR IGNORE INTO tags (tags) VALUES (?)", (tag_name,))
        cursor.execute("SELECT id FROM tags WHERE tags = ?", (tag_name,))
        tag_id = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 1 FROM color_tags 
            WHERE color_id = ? AND tag_id = ?
        """, (color_id, tag_id))
        
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO color_tags (color_id, tag_id)
                VALUES (?, ?)
            """, (color_id, tag_id))
    
    connect.commit()

while True:
    color = input("Введите цвет: ")
    tags = input("Теги (через пробел): ").lower().split()
    submit(color,tags)