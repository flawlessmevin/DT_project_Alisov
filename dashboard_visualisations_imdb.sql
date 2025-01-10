USE DATABASE IMDB_FLAMINGO;
USE SCHEMA IMDB_FLAMINGO.stage;


-- GRAPH 1 - average movie rating by premier year

SELECT 
    dm.dim_year, 
    AVG(fr.avg_rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_movies dm ON dm.dim_movie_id = fr.movie_id
GROUP BY dm.dim_year
ORDER BY dm.dim_year;


-- GRAPH 2 - average movie rating by film duration

SELECT 
    dm.duration AS duration, 
    AVG(fr.avg_rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_movies dm ON dm.dim_movie_id = fr.movie_id
GROUP BY dm.duration
ORDER BY dm.duration;


-- GRAPH 3 - Movie ratings by directors' age groups

SELECT 
    d.age_group, 
    AVG(fr.avg_rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_directors d ON d.dim_director_id = fr.director_id
WHERE d.age_group != 'Unknown' 
GROUP BY d.age_group
ORDER BY d.age_group;


-- GRAPH 4 movies count by genres

SELECT 
    TRIM(genre_split.value) AS genre,
    COUNT(*) AS movie_count
FROM 
    dim_movies dm,
    LATERAL FLATTEN(input => SPLIT(dm.dim_genres, ', ')) AS genre_split
GROUP BY 
    genre_split.value
ORDER BY 
    movie_count DESC;


-- GRAPH 5 - top 10 countries by number of films released
SELECT 
    dm.country, 
    COUNT(*) AS movie_count
FROM dim_movies dm
GROUP BY dm.country
ORDER BY movie_count DESC
LIMIT 10;
