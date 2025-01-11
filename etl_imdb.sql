
CREATE DATABASE IMDB_FLAMINGO;
USE DATABASE IMDB_FLAMINGO;


CREATE SCHEMA IMDB_FLAMINGO.stage;
USE SCHEMA IMDB_FLAMINGO.stage;



--create movie table (staging)
CREATE TABLE movie_stage
 (
  id VARCHAR(10) NOT NULL PRIMARY KEY,
  title VARCHAR(200) DEFAULT NULL,
  year INT DEFAULT NULL,
  date_published DATE DEFAULT null,
  duration INT,
  country VARCHAR(250),
  worlwide_gross_income VARCHAR(30),
  languages VARCHAR(200),
  production_company VARCHAR(200),

);

--create genre table (staging)
CREATE TABLE genre_stage
 (
	movie_id VARCHAR(10),
    genre VARCHAR(20),
	PRIMARY KEY (movie_id, genre)
);

--create director_mapping table (staging)
CREATE TABLE director_mapping_stage	
 (
	movie_id VARCHAR(10),
    name_id VARCHAR(10),
	PRIMARY KEY (movie_id, name_id)
);

--create role_mapping table (staging)
CREATE TABLE role_mapping_stage
 (
	movie_id VARCHAR(10) NOT NULL,
    name_id VARCHAR(10) NOT NULL,
    category VARCHAR(10),
	PRIMARY KEY (movie_id, name_id)
);

--create names table (staging)
CREATE TABLE names_stage
 (
  id varchar(10) NOT NULL,
  name varchar(100) DEFAULT NULL,
  height int DEFAULT NULL,
  date_of_birth date DEFAULT null,
  known_for_movies varchar(100),
  PRIMARY KEY (id)
);

--create ratings table (staging)
CREATE TABLE ratings_stage
(
	movie_id VARCHAR(10) NOT NULL,
	avg_rating DECIMAL(3,1),
	total_votes INT,
	median_rating INT,
    PRIMARY KEY (movie_id)
);



--create stage for csv files
CREATE OR REPLACE STAGE IMDB_STAGE;

--import csv files to stage 

COPY INTO movie_stage
FROM @IMDB_STAGE/movie.csv
FILE_FORMAT = ( TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"'SKIP_HEADER = 1
    -- table movie had errors during import (Found character 'T' instead of record delimiter '\n')
    RECORD_DELIMITER = '\n'  
    FIELD_DELIMITER = ','   
)
ON_ERROR = 'CONTINUE';



COPY INTO genre_stage
FROM @IMDB_STAGE/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO director_mapping_stage
FROM @IMDB_STAGE/director_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO role_mapping_stage
FROM @IMDB_STAGE/role_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ratings_stage
FROM @IMDB_STAGE/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO names_stage
FROM @IMDB_STAGE/names.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL')
);



    
--dim directors    

CREATE TABLE dim_directors AS
SELECT DISTINCT 
     n.id as dim_director_id,
     CASE 
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) < 18 THEN 'Under 18'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 18 AND 24 THEN '18-24'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 25 AND 34 THEN '25-34'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 35 AND 44 THEN '35-44'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 45 AND 54 THEN '45-54'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) >= 55 THEN '55+'
        ELSE 'Unknown' -- if no data - set "Unknown"
    END AS age_group,
    FROM names_stage n 
    JOIN director_mapping_stage d ON d.name_id = n.id;


--dim movies 

CREATE TABLE dim_movies AS
SELECT DISTINCT 
    m.id AS dim_movie_id,
    m.title AS title,
    LISTAGG(g.genre, ', ') WITHIN GROUP (ORDER BY g.genre) AS dim_genres, -- split genres with commas  
    n.name AS director,
    m.year AS dim_year,
    m.date_published AS date_published,
    m.duration AS duration, 
    m.country AS country,
    m.production_company AS production_company,
    m.worlwide_gross_income AS worldwide_gross_income
FROM movie_stage m
JOIN genre_stage g ON g.movie_id = m.id
JOIN role_mapping_stage rm ON rm.movie_id = m.id
JOIN names_stage n ON n.id = rm.name_id
GROUP BY 
    m.id, m.title, n.name, m.year, m.date_published, m.duration, m.country, m.production_company, m.worlwide_gross_income; 





--dim actors 

CREATE TABLE dim_actors AS
SELECT DISTINCT 
    n.id as dim_actor_id,
    CASE 
        WHEN r.category = 'actress' THEN 'female'
        WHEN r.category = 'actor' THEN 'male'
        ELSE 'Unknown'
    END AS gender,
    CASE 
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) < 18 THEN 'Under 18'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 18 AND 24 THEN '18-24'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 25 AND 34 THEN '25-34'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 35 AND 44 THEN '35-44'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) BETWEEN 45 AND 54 THEN '45-54'
        WHEN DATEDIFF(YEAR, n.date_of_birth, CURRENT_DATE) >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group,
    FROM names_stage n
    JOIN role_mapping_stage r ON r.name_id = n.id;


--dim date

CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY date_published) AS dim_date_id,
    date_published as date,
    DATE_PART(day, date_published) AS day,             
    DATE_PART(week, date_published) AS week,          
    CASE DATE_PART(month, date_published)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Oktober'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month,             
    DATE_PART(quarter, date_published) AS quarter,     
    DATE_PART(year, date_published) AS year            
FROM
    (SELECT DISTINCT date_published FROM movie_stage) t 
WHERE
    date_published IS NOT NULL;         



--fact ratings

CREATE TABLE fact_ratings AS
SELECT
    ROW_NUMBER() OVER (ORDER BY r.movie_id) AS fact_rating_id,
    r.avg_rating as avg_rating,
    r.total_votes as total_votes,
    r.median_rating as median_rating,
    m.dim_movie_id as movie_id,
    d.dim_director_id as director_id,
    dt.dim_date_id AS film_date_id
FROM ratings_stage r
JOIN dim_movies m ON m.dim_movie_id = r.movie_id
JOIN dim_directors d ON d.dim_director_id = m.director_id
JOIN dim_date dt ON m.date_published = dt.date;



--drop staging tables
DROP TABLE IF EXISTS ratings_stage;
DROP TABLE IF EXISTS names_stage;
DROP TABLE IF EXISTS role_mapping_stage;
DROP TABLE IF EXISTS director_mapping_stage;
DROP TABLE IF EXISTS genre_stage;
DROP TABLE IF EXISTS movie_stage
