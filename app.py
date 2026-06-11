from flask import Flask, render_template, request, redirect, url_for, flash
from table_movie import MovieManager
from table_of_color import submit, get_all_colors, search_by_tag

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'


def get_poster_url(poster_path, size='w342'):
    """Возвращает полный URL постера TMDB или None"""
    if poster_path:
        return f"https://image.tmdb.org/t/p/{size}{poster_path}"
    return None

app.jinja_env.globals.update(get_poster_url=get_poster_url)

# Инициализация менеджера фильмов
movie_mgr = MovieManager("table_of_movies.db")
# Создаём поисковый индекс, если есть фильмы
movie_mgr.create_search_index()

# Главная страница 
@app.route('/')
def index():
    return render_template('index.html')

# Раздел фильмы
@app.route('/movies')
def movies_list():
    movies = movie_mgr.get_all_movies()
    return render_template('movies/list.html', movies=movies)

@app.route('/movies/add_manual', methods=['GET', 'POST'])
def movies_add_manual():
    if request.method == 'POST':
        title = request.form['title']
        year = int(request.form['year'])
        director = request.form['director']
        description = request.form['description']
        rating = float(request.form['rating'].replace(',', '.'))
        tags = [t.strip().lower() for t in request.form['tags'].split() if t.strip()]
        movie_mgr.add_movie(title, year, director, description, rating, tags, poster_path=None)
        movie_mgr.create_search_index()  # обновляем индекс
        flash(f'Фильм "{title}" добавлен', 'success')
        return redirect(url_for('movies_list'))
    return render_template('movies/add_manual.html')

@app.route('/movies/add_tmdb', methods=['GET', 'POST'])
def movies_add_tmdb():
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

@app.route('/movies/search_tags', methods=['GET', 'POST'])
def movies_search_tags():
    if request.method == 'POST':
        tags_str = request.form['tags']
        tags = [t.strip().lower() for t in tags_str.split() if t.strip()]
        match_all = 'match_all' in request.form
        if not tags:
            flash('Введите теги', 'warning')
            return redirect(url_for('movies_search_tags'))
        results = movie_mgr.search_by_tags(tags, match_all)
        return render_template('movies/search_results.html', results=results, query=', '.join(tags))
    else:  # GET-запрос (переход по ссылке)
        tags_str = request.args.get('tags', '')
        if tags_str:
            tags = [t.strip().lower() for t in tags_str.split() if t.strip()]
            if tags:
                match_all = request.args.get('match_all', 'false') == 'true'
                results = movie_mgr.search_by_tags(tags, match_all)
                return render_template('movies/search_results.html', results=results, query=', '.join(tags))
        return render_template('movies/search_tags.html')
    
@app.route('/movies/search_desc', methods=['GET', 'POST'])
def movies_search_desc():
    if request.method == 'POST':
        query = request.form['query'].strip()
        if not query:
            flash('Введите описание', 'warning')
            return redirect(url_for('movies_search_desc'))
        results = movie_mgr.search_by_index(query, amount=5)
        return render_template('movies/search_results.html', results=results, query=query)
    return render_template('movies/search_desc.html')

# Раздел цвета
@app.route('/colors')
def colors_list():
    colors = get_all_colors()
    return render_template('colors/list.html', colors=colors)

@app.route('/colors/add', methods=['GET', 'POST'])
def colors_add():
    if request.method == 'POST':
        color = request.form['color'].strip()
        tags = [t.strip().lower() for t in request.form['tags'].split() if t.strip()]
        if not color:
            flash('Введите название цвета', 'warning')
        else:
            submit(color, tags)
            flash(f'Цвет "{color}" добавлен', 'success')
        return redirect(url_for('colors_list'))
    return render_template('colors/add.html')

@app.route('/colors/search', methods=['GET', 'POST'])
def colors_search():
    if request.method == 'POST':
        tag = request.form['tag'].strip().lower()
        if not tag:
            flash('Введите тег', 'warning')
            return redirect(url_for('colors_search'))
        results = search_by_tag(tag)
        return render_template('colors/search_results.html', results=results, tag=tag)
    return render_template('colors/search.html')

if __name__ == '__main__':
    app.run(debug=True)