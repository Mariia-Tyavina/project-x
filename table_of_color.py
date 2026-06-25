# table_of_color.py — модуль для управления цветами и их тегами.
# Использует глобальное соединение с БД 


import sqlite3
from typing import List, Dict

# Глобальное соединение с БД 
connect = sqlite3.connect('table_of_colors.db', check_same_thread=False)
cursor = connect.cursor()


# Создание таблиц, если их нет
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
    """
    Добавляет цвет в таблицу colors (если его ещё нет) и связывает с тегами.

    Аргументы:
        color (str): Название цвета.
        tags (List[str]): Список тегов для этого цвета.
    """
    # Проверяем, существует ли уже такой цвет
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
    
    # Добавляем теги (если их нет — создаём)
    for tag_name in tags:
        cursor.execute(
            "INSERT OR IGNORE INTO tags (tags) VALUES (?)", (tag_name,)
        )
        cursor.execute(
            "SELECT id FROM tags WHERE tags = ?", (tag_name,)
        )
        tag_id = cursor.fetchone()[0]
        
        # Проверяем, не существует ли уже связь цвет-тег
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

def get_all_colors() -> List[Dict]:
    """
    Возвращает список всех цветов с их тегами.

    Returns:
        List[Dict]: Список словарей с ключами 'id', 'name', 'tags'.
    """
    cursor.execute("SELECT id, name FROM colors ORDER BY name")
    colors = []
    for color_id, name in cursor.fetchall():
        cursor.execute("""
            SELECT t.tags FROM tags t
            JOIN color_tags ct ON t.id = ct.tag_id
            WHERE ct.color_id = ?
        """, (color_id,))
        tags = [row[0] for row in cursor.fetchall()]
        colors.append({"id": color_id, "name": name, "tags": tags})
    return colors

def search_by_tag(tag: str) -> List[Dict]:
    """
    Ищет цвета по точному совпадению тега.

    Аргументы:
        tag (str): Искомый тег.

    Returns:
        List[Dict]: Список цветов с их тегами.
    """
    cursor.execute("""
        SELECT c.id, c.name FROM colors c
        JOIN color_tags ct ON c.id = ct.color_id
        JOIN tags t ON ct.tag_id = t.id
        WHERE t.tags = ?
    """, (tag.lower(),))
    colors = []
    for color_id, name in cursor.fetchall():
        cursor.execute("""
            SELECT t.tags FROM tags t
            JOIN color_tags ct ON t.id = ct.tag_id
            WHERE ct.color_id = ?
        """, (color_id,))
        tags = [row[0] for row in cursor.fetchall()]
        colors.append({"id": color_id, "name": name, "tags": tags})
    return colors
