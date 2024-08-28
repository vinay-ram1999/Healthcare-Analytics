-- CVD Exploratory Data Analysis

USE Healthcare;

SELECT * FROM CVD_topic_gen;

SELECT * FROM CVD_question_gen;

SELECT * FROM CVD_break_out_category_gen;

SELECT * FROM CVD_break_out_gen;

-- number of records per each state in a year
SELECT year_start, location_desc, COUNT(*)
FROM stagging_CVD
GROUP BY year_start, location_desc;

-- Average mortality rate in entire USA over the years
SELECT year_start, AVG(data_value) AS avg_mr
FROM stagging_CVD
GROUP BY year_start;

-- Average mortality rate in each state over the years
SELECT location_desc, year_start, AVG(data_value) AS avg_mr
FROM stagging_CVD
GROUP BY year_start, location_desc
ORDER BY location_desc, year_start;

-- Average mortality rate by topic overall
WITH topic_avg AS (
SELECT topic_id, AVG(data_value) AS avg_mr
FROM stagging_CVD
GROUP BY topic_id)

SELECT topic, avg_mr
FROM topic_avg
NATURAL JOIN CVD_topic_gen
ORDER BY avg_mr DESC;

-- Average mortality rate by topic over the years
WITH topic_avg AS (
SELECT topic_id, year_start, AVG(data_value) AS avg_mr
FROM stagging_CVD
GROUP BY topic_id, year_start)

SELECT topic, year_start, avg_mr
FROM topic_avg
NATURAL JOIN CVD_topic_gen
ORDER BY topic, year_start;

-- there are 8 different questions but only 6 different topics lets find which question belongs to which topic and their Average mortality rate over the years
WITH topic_question AS (
SELECT topic_id, question_id, AVG(data_value) AS avg_mr
FROM stagging_CVD
GROUP BY topic_id, question_id)

SELECT topic, question, avg_mr
FROM topic_question
NATURAL JOIN CVD_topic_gen
NATURAL JOIN CVD_question_gen
ORDER BY topic, question;

-- breakdown the above results by each year
WITH topic_question AS (
SELECT topic_id, question_id, year_start, AVG(data_value) AS avg_mr
FROM stagging_CVD
GROUP BY topic_id, question_id, year_start)

SELECT year_start, topic, question, avg_mr
FROM topic_question
NATURAL JOIN CVD_topic_gen
NATURAL JOIN CVD_question_gen
ORDER BY year_start, topic, question;


-- 
WITH break_out_avg AS (
SELECT break_out_category_id, break_out_id, AVG(data_value) AS avg_mr, COUNT(*) AS sample_count
FROM stagging_CVD
GROUP BY break_out_category_id, break_out_id)

SELECT break_out_category_id, break_out_category, break_out_id, break_out, avg_mr, sample_count
FROM break_out_avg
NATURAL JOIN CVD_break_out_category_gen
NATURAL JOIN CVD_break_out_gen
ORDER BY break_out_category_id, break_out_id;


SELECT COUNT(*) FROM stagging_CVD WHERE data_value_type_id = 'AgeStdz';


SELECT * FROM stagging_CVD;

