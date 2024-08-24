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
    breat_out_id VARCHAR(10),
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
    breat_out_id = IF(@28 = '', NULL, @28),
    location_id = IF(@29 = '', NULL, @29),
    geo_location = IF(@30 = '', NULL, @30);



SELECT * FROM CVD;


