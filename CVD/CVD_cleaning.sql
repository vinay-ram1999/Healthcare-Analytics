-- Loading Data --

-- Create a new SCHEMA named SRDBv5
DROP SCHEMA IF EXISTS `Healthcare`;
CREATE SCHEMA `Healthcare`;
USE Healthcare;

-- Create an empty table named CVD (Cardiovascular Disease) to load the data
DROP TABLE IF EXISTS CVD;
CREATE TABLE CVD (
    row_id VARCHAR(50),
    year_start INTEGER,
    location_abbr VARCHAR(5),
    location_desc VARCHAR(25),
    data_source VARCHAR(10),
    priority_area_1 VARCHAR(50),
    priority_area_2 VARCHAR(50),
    priority_area_3 VARCHAR(50),
    priority_area_4 VARCHAR(50),
    cls VARCHAR(50),
    topic VARCHAR(50),
    question TEXT,
    data_value_type VARCHAR(50),
    data_value_unit VARCHAR(50),
    data_value FLOAT,
    data_value_alt FLOAT,
    data_value_footnote_symbol VARCHAR(5),
    data_value_footnote TEXT,
    low_confidence_limit FLOAT,
    high_confidence_limit FLOAT,
    break_out_category VARCHAR(50),
    break_out VARCHAR(50),
    cls_id VARCHAR(10),
    topic_id VARCHAR(10),
    question_id VARCHAR(10),
    data_value_type_id VARCHAR(10),
    break_out_category_id VARCHAR(10),
    break_out_id VARCHAR(10),
    location_id VARCHAR(10),
    geo_location TEXT
);

-- Now import the data from csv into the CVD table
SET GLOBAL LOCAL_INFILE=TRUE;
SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';

LOAD DATA 
LOCAL INFILE '~/Developer/GitHub/Healthcare-Analytics/CVD/National_Vital_Statistics_System__NVSS__-_National_Cardiovascular_Disease_Surveillance_Data_20240823.csv' -- Change this file path to where you have saved your .csv file
INTO TABLE CVD
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@1,@2,@3,@4,@5,@6,@7,@8,@9,@10,@11,@12,@13,@14,@15,@16,@17,@18,@19,@20,@21,@22,@23,@24,@25,@26,@27,@28,@29,@30) -- @1 is the alias for the 1st column in csv and etc.
SET row_id = IF(@1 = '', NULL, @1),
    year_start = IF(@2 = '', NULL, @2),
    location_abbr = IF(@3 = '', NULL, @3),
    location_desc = IF(@4 = '', NULL, @4),
    data_source = IF(@5 = '', NULL, @5),
    priority_area_1 = IF(@6 = '', NULL, @6),
    priority_area_2 = IF(@7 = '', NULL, @7),
    priority_area_3 = IF(@8 = '', NULL, @8),
    priority_area_4 = IF(@9 = '', NULL, @9),
    cls = IF(@10 = '', NULL, @10),
    topic = IF(@11 = '', NULL, @11),
    question = IF(@12 = '', NULL, @12),
    data_value_type = IF(@13 = '', NULL, @13),
    data_value_unit = IF(@14 = '', NULL, @14),
    data_value = IF(@15 = '', NULL, @15),
	data_value_alt = IF(@16 = '', NULL, @16),
    data_value_footnote_symbol = IF(@17 = '', NULL, @17),
    data_value_footnote = IF(@18 = '', NULL, @18),
    low_confidence_limit = IF(@19 = '', NULL, @19),
    high_confidence_limit = IF(@20 = '', NULL, @20),
    break_out_category = IF(@21 = '', NULL, @21),
    break_out = IF(@22 = '', NULL, @22),
    cls_id = IF(@23 = '', NULL, @23),
    topic_id = IF(@24 = '', NULL, @24),
    question_id = IF(@25 = '', NULL, @25),
    data_value_type_id = IF(@26 = '', NULL, @26),
    break_out_category_id = IF(@27 = '', NULL, @27),
    break_out_id = IF(@28 = '', NULL, @28),
    location_id = IF(@29 = '', NULL, @29),
    geo_location = IF(@30 = '', NULL, @30);


-- Data Cleaning --

-- Before manuplating the data, create a staging or copy of original data
DROP TABLE IF EXISTS stagging_CVD;
CREATE TABLE stagging_CVD
LIKE CVD;

INSERT INTO stagging_CVD
SELECT * FROM CVD;


-- drop rows with data_value_footnote_symbol = '~' which means 'Statistically unstable estimates not presented [unstable by NCHS standards: (standard error/estimate>0.23 or numerator <20)]'
DELETE FROM stagging_CVD
WHERE data_value_footnote_symbol = '~' AND data_value IS NULL;
-- now we can delete columns data_value_footnote_symbol and data_value_footnote as they are no longer needed


SELECT break_out_category_id, break_out_category, break_out_id, break_out, COUNT(*)
FROM stagging_CVD
GROUP BY break_out_category_id, break_out_category, break_out_id, break_out;

-- There are 6 break_out's for 'AGE' break_out_category which are 'AGE01' (18-24), 'AGE04' (25-44), 'AGE05' (45-64), 'AGE06' (65+), 'AGE07' (35+), 'AGE08' (75+)
-- There is no way we can accurately merge all these groups cause 25-44 can't be added to a single other group
-- In order to better generalize the population and based on the number of data samples per break_out (after cleaning rows) the following groups are created
-- 'AGE1' (18-34) = 'AGE01' + 'AGE04'
-- 'AGE2' (35-64) = 'AGE05' + 'AGE07'
-- 'AGE3' (65-74) = 'AGE06'
-- 'AGE4' (75+) = 'AGE08'

UPDATE stagging_CVD
SET break_out_id = 'AGE1', break_out = '18-34'
WHERE break_out_id = 'AGE01' OR break_out_id = 'AGE04';

UPDATE stagging_CVD
SET break_out_id = 'AGE2', break_out = '35-64'
WHERE break_out_id = 'AGE05' OR break_out_id = 'AGE07';

UPDATE stagging_CVD
SET break_out_id = 'AGE3', break_out = '65-74'
WHERE break_out_id = 'AGE06';

UPDATE stagging_CVD
SET break_out_id = 'AGE4', break_out = '75+'
WHERE break_out_id = 'AGE08';


-- Generalizing the data using the id's
SELECT location_abbr, location_desc, location_id
FROM stagging_CVD
GROUP BY location_abbr, location_desc, location_id;

SELECT cls, cls_id
FROM stagging_CVD
GROUP BY cls, cls_id;

CREATE TABLE CVD_topic_gen AS
SELECT topic, topic_id
FROM stagging_CVD
GROUP BY topic, topic_id;

CREATE TABLE CVD_question_gen AS
SELECT question, question_id
FROM stagging_CVD
GROUP BY question, question_id;

CREATE TABLE CVD_break_out_category_gen AS
SELECT break_out_category, break_out_category_id
FROM stagging_CVD
GROUP BY break_out_category, break_out_category_id;

CREATE TABLE CVD_break_out_gen AS
SELECT break_out, break_out_id
FROM stagging_CVD
GROUP BY break_out, break_out_id;


-- Drop unwanted columns
SELECT DISTINCT priority_area_1 FROM stagging_CVD; -- remove

SELECT DISTINCT priority_area_2 FROM stagging_CVD; -- remove

SELECT DISTINCT priority_area_3 FROM stagging_CVD; -- remove, unable to draw useful info from 'Healthy People 2030'

SELECT priority_area_3, COUNT(*)
FROM stagging_CVD
GROUP BY priority_area_3;

SELECT DISTINCT priority_area_4 FROM stagging_CVD; -- remove

SELECT DISTINCT cls FROM stagging_CVD; -- remove (generalize in a table before removing)

SELECT DISTINCT topic FROM stagging_CVD; -- remove (generalize in a table before removing)

SELECT DISTINCT question FROM stagging_CVD; -- remove (generalize in a table before removing)

SELECT DISTINCT break_out_category FROM stagging_CVD; -- remove (generalize in a table before removing)

SELECT DISTINCT break_out FROM stagging_CVD; -- remove (generalize in a table before removing)

SELECT DISTINCT data_value_unit FROM stagging_CVD; -- remove since the unit is 'Rate per 100,000' constant for all records

SELECT DISTINCT cls_id FROM stagging_CVD; -- generalize using these values

SELECT DISTINCT topic_id FROM stagging_CVD; -- generalize using these values

SELECT DISTINCT break_out_category_id FROM stagging_CVD; -- generalize using these values

SELECT DISTINCT break_out_id FROM stagging_CVD; -- generalize using these values

SELECT DISTINCT location_id FROM stagging_CVD; -- remove

SELECT COUNT(*) 
FROM stagging_CVD
WHERE data_value_alt != data_value AND data_value IS NOT NULL AND data_value_alt IS NOT NULL;
-- remove data_value_alt since both data_value_alt and data_value are same in all cases

SELECT COUNT(*) FROM (SELECT DISTINCT geo_location FROM stagging_CVD WHERE geo_location IS NOT NULL) AS geo;
-- there are only 51 distinct geo_locations pointing out that they are indicating location of each state so we can remove it

-- Drop columns from stagging_CVD
ALTER TABLE stagging_CVD
DROP COLUMN location_abbr,
DROP COLUMN data_source,
DROP COLUMN priority_area_1,
DROP COLUMN priority_area_2,
DROP COLUMN priority_area_3,
DROP COLUMN priority_area_4,
DROP COLUMN cls,
DROP COLUMN topic,
DROP COLUMN question,
DROP COLUMN data_value_type,
DROP COLUMN data_value_type_id,		-- We don't need this column as well
DROP COLUMN data_value_unit,
DROP COLUMN data_value_alt,
DROP COLUMN data_value_footnote_symbol,
DROP COLUMN data_value_footnote,
DROP COLUMN break_out_category,
DROP COLUMN break_out,
DROP COLUMN location_id,
DROP COLUMN geo_location;


-- In question column at the end remove ' (18+); NVSS'
UPDATE CVD_question_gen
SET question = LEFT(question, char_length(question) - 12);


SELECT * FROM stagging_CVD;

SELECT COUNT(*) FROM stagging_CVD;
