-- ============================================================
-- 0. Inspect database structure
-- ============================================================

-- List all tables in the database
SELECT name FROM sqlite_master WHERE type='table';

-- Inspect table schema (SQLite)
PRAGMA table_info(movies);


-- ============================================================
-- 1. Basic exploration queries
-- ============================================================

-- Show 10 movies
SELECT * FROM movies LIMIT 10;

-- Count total movies stored
SELECT COUNT(*) AS total_movies FROM movies;

-- Count movies per original language
SELECT original_language, COUNT(*) AS num_movies
FROM movies
GROUP BY original_language
ORDER BY num_movies DESC;


-- ============================================================
-- 2. Popularity & Rating Insights
-- ============================================================

-- Top 20 most popular movies
SELECT title, popularity, vote_average, vote_count, release_date
FROM movies
ORDER BY popularity DESC
LIMIT 20;

-- Top rated movies (min 100 votes)
SELECT title, vote_average, vote_count, popularity
FROM movies
WHERE vote_count >= 100
ORDER BY vote_average DESC
LIMIT 20;

-- Rating distribution (rounded bins)
SELECT ROUND(vote_average, 0) AS rating_bin, COUNT(*) AS count
FROM movies
GROUP BY rating_bin
ORDER BY rating_bin;


-- ============================================================
-- 3. Temporal Analysis
-- ============================================================

-- Movies per year
SELECT
    strftime('%Y', release_date) AS year,
    COUNT(*) AS num_movies
FROM movies
WHERE release_date IS NOT NULL
GROUP BY year
ORDER BY year;

-- Popularity evolution by year
SELECT
    strftime('%Y', release_date) AS year,
    AVG(popularity) AS avg_popularity
FROM movies
GROUP BY year
ORDER BY year;


-- ============================================================
-- 4. Genre Analysis (via junction table)
-- ============================================================

-- If you have a 'movie_genres' table:
-- List genres
SELECT * FROM genres;

-- Most common genres
SELECT
    g.name AS genre,
    COUNT(*) AS num_movies
FROM movie_genres mg
JOIN genres g ON mg.genre_id = g.id
GROUP BY g.id
ORDER BY num_movies DESC;

-- Average popularity per genre
SELECT
    g.name AS genre,
    AVG(m.popularity) AS avg_popularity
FROM movie_genres mg
JOIN genres g ON mg.genre_id = g.id
JOIN movies m ON mg.movie_id = m.id
GROUP BY genre
ORDER BY avg_popularity DESC;


-- ============================================================
-- 5. Advanced Queries
-- ============================================================

-- Language × Year cross-analysis
SELECT
    original_language,
    strftime('%Y', release_date) AS year,
    COUNT(*) AS count
FROM movies
GROUP BY original_language, year
ORDER BY count DESC;

-- Movies with extreme popularity (above 99th percentile)
SELECT *
FROM movies
WHERE popularity > (
    SELECT percentile_99
    FROM (
        SELECT popularity,
               PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY popularity) AS percentile_99
        FROM movies
    )
);
