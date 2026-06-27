# app.py — основной модуль Flask-приложения.
# Содержит маршруты для работы с фильмами и цветами.

from flask import Flask, render_template, request, redirect, url_for, flash
from table_movie import MovieManager
from table_of_color import submit, get_all_colors, search_by_tag
import random
from math import ceil

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'


def get_poster_url(poster_path, size='w342'):
    """
    Возвращает полный URL постера фильма из TMDB.

    Аргументы:
    poster_path (str): путь к постеру (например, '/abc.jpg').
    size (str): Размер изображения (по умолчанию 'w342').

    Возвращает:
        str: Полный URL или None, если poster_path отсутствует.
    """
    if poster_path:
        return f"https://image.tmdb.org/t/p/{size}{poster_path}"
    return None

# Делаем функцию доступной во всех шаблонах Jinja2
app.jinja_env.globals.update(get_poster_url=get_poster_url)

# Создаём экземпляр менеджера фильмов и строим поисковый индекс
movie_mgr = MovieManager("table_of_movies.db")
movie_mgr.create_search_index()


# Главная страница
@app.route('/')
def index():
    all_movies = movie_mgr.get_all_movies()
    movies_with_poster = [m for m in all_movies if m.get('poster_path')]
    random_movies = random.sample(movies_with_poster, 
                                  min(6, len(movies_with_poster)))
    return render_template('index.html', random_movies=random_movies)


# Фильмы список
@app.route('/movies')
def movies_list():
    """
    Отображает список всех фильмов с пагинацией (15 фильмов на страницу).
    """
    page = request.args.get('page', 1, type=int)
    per_page = 15
    all_movies = movie_mgr.get_all_movies()
    total = len(all_movies)
    total_pages = ceil(total / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    movies = all_movies[start:end]
    return render_template(
        'movies/list.html', 
        movies=movies, 
        page=page, 
        total_pages=total_pages
    )


# Добавление фильмов вручную
@app.route('/movies/add_manual', methods=['GET', 'POST'])
def movies_add_manual():
    """
    Страница и обработчик формы добавления фильма вручную.
    После добавления перестраивается поисковый индекс.
    """
    if request.method == 'POST':
        title = request.form['title']
        year = int(request.form['year'])
        director = request.form['director']
        description = request.form['description']
        rating = float(request.form['rating'].replace(',', '.'))
        tags = [t.strip().lower() for t in request.form['tags'].split()
                 if t.strip()]
        movie_mgr.add_movie(title, year, director, description, rating, 
                            tags, poster_path=None)
        movie_mgr.create_search_index()
        flash(f'Фильм "{title}" добавлен', 'success')
        return redirect(url_for('movies_list'))
    return render_template('movies/add_manual.html')


# Добавление фильма через TMDB
@app.route('/movies/add_tmdb', methods=['GET', 'POST'])
def movies_add_tmdb():
    """
    Страница добавления фильма автоматически через TMDB API.
    После успешного добавления перестраивается индекс.
    """
    if request.method == 'POST':
        query = request.form['query'].strip()
        if not query:
            flash('Введите название', 'warning')
            return redirect(url_for('movies_add_tmdb'))
        success, msg = movie_mgr.search_by_tmdb(query)
        if success:
            movie_mgr.create_search_index()
            flash(msg, 'success')
        else:
            flash(msg, 'danger')
        return redirect(url_for('movies_list'))
    return render_template('movies/add_tmdb.html')


# Поиск фильмов по тегам
@app.route('/movies/search_tags', methods=['GET', 'POST'])
def movies_search_tags():
    """
    Поиск фильмов по одному или нескольким тегам.
    Поддерживает режим 'совпадение всех тегов'.
    Результаты разбиты.
    """
    page = request.args.get('page', 1, type=int)
    per_page = 15

    if request.method == 'POST':
        tags_str = request.form['tags']
        tags = [t.strip().lower() for t in tags_str.split() if t.strip()]
        match_all = 'match_all' in request.form
        if not tags:
            flash('Введите теги', 'warning')
            return redirect(url_for('movies_search_tags'))
        return redirect(url_for(
            'movies_search_tags',
            tags=','.join(tags), 
            match_all=match_all,
            page=1)
        )

    # GET-запрос — читаем параметры из URL
    tags_param = request.args.get('tags', '')
    match_all = request.args.get('match_all', 'false') == 'true'
    if not tags_param:
        return render_template('movies/search_tags.html')

    tags = tags_param.split(',')
    all_results = movie_mgr.search_by_tags(tags, match_all)
    total = len(all_results)
    total_pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    results = all_results[start:end]

    return render_template(
        'movies/search_results.html',
        results=results,
        query=', '.join(tags),
        page=page,
        total_pages=total_pages,
        total=total,
        search_type='tags' 
    )


# Поиск по описанию
@app.route('/movies/search_desc', methods=['GET', 'POST'])
def movies_search_desc():
    """
    Поиск фильмов по смыслу описания с использованием FAISS-индекса.
    Результаты пагинируются.
    """
    page = request.args.get('page', 1, type=int)
    per_page = 15

    if request.method == 'POST':
        query = request.form.get('query', '').strip()
        if not query:
            flash('Введите описание', 'warning')
            return redirect(url_for('movies_search_desc'))
        return redirect(url_for('movies_search_desc', query=query, page=1))

    query = request.args.get('query', '')
    if not query:
        return render_template('movies/search_desc.html')

    # Получаем все результаты (ограничиваем 500 для производительности) 
    all_results = movie_mgr.search_by_index(query, amount=500)
    total = len(all_results)
    total_pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    results = all_results[start:end]

    return render_template(
        'movies/search_results.html',
        results=results,
        query=query,
        page=page,
        total_pages=total_pages,
        total=total,
        search_type='desc'
    )


# цвета 
@app.route('/colors')
def colors_list():
    """
    Отображает список всех цветов с их тегами.
    """
    colors = get_all_colors()
    return render_template('colors/list.html', colors=colors)


@app.route('/colors/add', methods=['GET', 'POST'])
def colors_add():
    """
    Страница добавления нового цвета с тегами.
    """
    if request.method == 'POST':
        color = request.form['color'].strip()
        tags = [t.strip().lower() for t in request.form['tags'].split() 
                if t.strip()]
        if not color:
            flash('Введите название цвета', 'warning')
        else:
            submit(color, tags)
            flash(f'Цвет "{color}" добавлен', 'success')
        return redirect(url_for('colors_list'))
    return render_template('colors/add.html')


@app.route('/colors/search', methods=['GET', 'POST'])
def colors_search():
    """
    Поиск цветов по тегу.
    Результаты пагинируются.
    """
    page = request.args.get('page', 1, type=int)
    per_page = 15

    if request.method == 'POST':
        tag = request.form['tag'].strip().lower()
        if not tag:
            flash('Введите тег', 'warning')
            return redirect(url_for('colors_search'))
        return redirect(url_for('colors_search', tag=tag, page=1))

    tag = request.args.get('tag', '')
    if not tag:
        return render_template('colors/search.html')

    all_results = search_by_tag(tag)
    total = len(all_results)
    total_pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    results = all_results[start:end]

    return render_template('colors/search_results.html',
                         results=results,
                         tag=tag,
                         page=page,
                         total_pages=total_pages,
                         total=total)

if __name__ == '__main__':
    app.run(debug=True)