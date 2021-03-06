-- This script finds the actors/actresses with the highest number of good movies

raw_roles = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars-test.tsv' USING PigStorage('\t') AS (star, title, year, num, type, episode, billing, char, gender);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output


raw_ratings = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test.tsv' USING PigStorage('\t') AS (dist, votes, score, title, year, num, type, episode);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' to see the full output

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
-- Now to implement the script

-- We want to compute the top actors / top actresses (separately).
-- Actors should be one output file, actresses in the other.
-- Gender is now given as 'MALE'/'FEMALE' in the gender column of raw_roles

-- To do so, we want to count how many good movies each starred in.
-- We count a movie as good if:
--   it has at least (>=) 10,001 votes (votes in raw_rating) 
--   it has a score >= 7.8 (score in raw_rating)

-- The best actors/actresses are those with the most good movies.

-- An actor/actress plays one role in each movie 
--   (more accurately, the roles are concatenated on one line like "role A/role B")

-- If an actor/actress does not star in a good movie
--  a count of zero should be returned (i.e., the actor/actress
--   should still appear in the output).

-- The results should be sorted descending by count.

-- We only want to count entries of type THEATRICAL_MOVIE (not tv series, etc.).
-- Again, note that only CONCAT(title,'##',year,'##',num) acts as a key for movies.

-- Test on smaller file first (as given above),
--  then test on larger file to get the results.

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
