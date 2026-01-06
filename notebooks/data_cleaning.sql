use sample;

#Turn off safe mode
SET SQL_SAFE_UPDATES = 0;

#Create copy of a job_scrape to clean data
CREATE TABLE job_temp AS SELECT * FROM job_scrape;

#Check duplicated rows
SELECT * FROM job_scrape;
SELECT ID, COUNT(*)
FROM job_scrape
GROUP BY ID
HAVING COUNT(*) > 1;

#Delete duplicated rows
DELETE FROM job_scrape WHERE ID IN (SELECT ID FROM (SELECT ID, ROW_NUMBER() OVER (PARTITION  BY ID) AS rownum FROM job_scrape) AS sub WHERE rownum > 1);

#Count total rows 
SELECT COUNT(*) AS total_rows FROM job_scrape;

#Create table states
#Count total rows 
SELECT COUNT(*) AS total_rows FROM job_temp;

SELECT * FROM job_scrape;
#Create table for all states
CREATE TABLE states (
    state_id VARCHAR(2) NOT NULL PRIMARY KEY,
    state_name VARCHAR(255) NOT NULL
);
INSERT INTO states (state_name, state_id) VALUES
('Alabama', 'AL'),
('Alaska', 'AK'),
('Arizona', 'AZ'),
('Arkansas', 'AR'),
('California', 'CA'),
('Colorado', 'CO'),
('Connecticut', 'CT'),
('Delaware', 'DE'),
('Florida', 'FL'),
('Georgia', 'GA'),
('Hawaii', 'HI'),
('Idaho', 'ID'),
('Illinois', 'IL'),
('Indiana', 'IN'),
('Iowa', 'IA'),
('Kansas', 'KS'),
('Kentucky', 'KY'),
('Louisiana', 'LA'),
('Maine', 'ME'),
('Maryland', 'MD'),
('Massachusetts', 'MA'),
('Michigan', 'MI'),
('Minnesota', 'MN'),
('Mississippi', 'MS'),
('Missouri', 'MO'),
('Montana', 'MT'),
('Nebraska', 'NE'),
('Nevada', 'NV'),
('New Hampshire', 'NH'),
('New Jersey', 'NJ'),
('New Mexico', 'NM'),
('New York', 'NY'),
('North Carolina', 'NC'),
('North Dakota', 'ND'),
('Ohio', 'OH'),
('Oklahoma', 'OK'),
('Oregon', 'OR'),
('Island (Guam, Santa Cruz)', 'IS'),
('Pennsylvania', 'PA'),
('Puero Rico', 'PR'),
('Rhode Island', 'RI'),
('South Carolina', 'SC'),
('South Dakota', 'SD'),
('Tennessee', 'TN'),
('Texas', 'TX'),
('Utah', 'UT'),
('Vermont', 'VT'),
('Virginia', 'VA'),
('Washington', 'WA'),
('Washington, DC', 'DC'),
('West Virginia', 'WV'),
('Wisconsin', 'WI'),
('Wyoming', 'WY');

#Show messy data points in "State" column
SELECT jt.state, jt.location
FROM job_temp jt
LEFT JOIN states st ON jt.State = st.state_id
WHERE st.state_id IS NULL;

SELECT id, location, state FROM job_temp
WHERE state NOT IN (SELECT state_id from states);

#Update messy data points in "State" column
UPDATE job_temp jt
INNER JOIN (
    SELECT jt1.id, st.state_id
    FROM job_temp jt1
    JOIN states st ON jt1.location LIKE CONCAT('%', st.state_name, '%')
    WHERE jt1.state NOT IN (SELECT state_id FROM states)
) AS temp_result
ON jt.id = temp_result.id
SET jt.state = temp_result.state_id;

#Create work_model table
CREATE TABLE model (
    model_id VARCHAR(2) NOT NULL PRIMARY KEY,
    work_model VARCHAR(100) NOT NULL
);
INSERT INTO model (model_id, work_model) VALUES
('Re', 'Remote'),
('On', 'On-site'),
('Hy', 'Hybrid');

SELECT id, location, type, link FROM job_temp1;
CREATE TABLE job_temp1 AS SELECT * FROM job_temp;

#Update "Type" column
SELECT id, location, link, type
FROM job_temp
WHERE description LIKE '%telework%'
   OR description LIKE '%hybrid%';

UPDATE job_temp j
JOIN model m ON j.location LIKE CONCAT('%', m.work_model, '%')
SET j.Type = m.model_id;

#Add "Annual Salary" column
SELECT id, salary, annual_salary FROM job_temp;
ALTER TABLE job_temp
ADD salary_per_year DECIMAL(10,2);

ALTER TABLE job_temp
RENAME COLUMN salary_per_year to annual_salary;

SELECT salary, annual_salary FROM job_temp
WHERE (salary IS NOT NULL) and (annual_salary IS NULL);

SELECT MIN(annual_salary) AS min_salary,
MAX(annual_salary) AS max_salary,
ROUND(AVG(annual_salary),2) AS avg_salary
FROM job_temp WHERE annual_salary IS NOT NULL AND annual_salary <>0;

SELECT id, annual_salary FROM job_temp
WHERE annual_salary > 19000 and annual_salary < 20000;

#Create table salaries
CREATE TABLE salaries (
    range_id VARCHAR(1) NOT NULL PRIMARY KEY,
    salary_range VARCHAR(255) NOT NULL
);
INSERT INTO salaries (range_id, salary_range) VALUES
('A', '$19,000-$40,000'),
('B', '$40,000-$60,000'),
('C', '$60,000-80,000'),
('D', '$80,000-$100,000'),
('E', '$100,000-$120,000'),
('F', '$120,000-$140,000'),
('G', '$140,000-$160,000'),
('H', '$160,000-$180,000'),
('I', '$180,000-$200,000'),
('J', '> $200,000');

#Clean "Salary" column and update "Annual_salary" column
SELECT
    s.range_id,
    s.salary_range,
    COUNT(j.annual_salary) AS count_jobs
FROM salaries s
LEFT JOIN job_temp j ON
    (
        (j.annual_salary >= REPLACE(REPLACE(SUBSTRING_INDEX(s.salary_range, '-', 1), '$', ''), ',', '') AND
        j.annual_salary < REPLACE(REPLACE(SUBSTRING_INDEX(s.salary_range, '-', -1), '$', ''), ',', '')) OR
        (s.salary_range = '> $200,000' AND j.annual_salary >= 200000)
    )
    AND j.annual_salary IS NOT NULL
GROUP BY s.range_id, s.salary_range
ORDER BY s.range_id;

#Convert hour pay to annual_salary 
UPDATE job_temp
SET annual_salary = 
    CASE
        WHEN salary LIKE '%$%hr%' OR salary LIKE '%$%hour%'
        THEN ROUND(CAST(SUBSTRING_INDEX(salary, '$', -1) AS DECIMAL(10, 2)) * 40 * 4 * 12)
        ELSE annual_salary  -- Keep the existing annual_salary if condition not met
    END
WHERE 
    salary NOT LIKE '%-%' 
    AND (salary LIKE '%$%hr%' OR salary LIKE '%$%hour%');
    
#Convert monthly pay to annual_salary
UPDATE job_temp
SET annual_salary = 
    CASE
        WHEN salary LIKE '%$%month%'
        THEN ROUND(CAST(REPLACE((SUBSTRING_INDEX(salary, '$', -1)), ',', '') AS DECIMAL(10, 2)) * 12)
        ELSE annual_salary  -- Keep the existing annual_salary if condition not met
    END
WHERE 
    salary NOT LIKE '%-%' 
    AND ( salary LIKE '%$%month%');

SELECT id, salary, annual_salary FROM job_temp WHERE salary LIKE '%hr%' or salary LIKE '%hour%';
SELECT id, salary, annual_salary FROM job_temp WHERE salary NOT LIKE '%-%' and (salary LIKE '%hr%' or salary LIKE '%hour%');
SELECT id, salary, annual_salary FROM job_temp WHERE salary LIKE '%month%';

#Create table skills
CREATE TABLE skills (
    skill VARCHAR(225) NOT NULL PRIMARY KEY,
    skill_group VARCHAR(255) NOT NULL
);
SELECT * FROM job_temp;

SELECT s.skill, COUNT(*) AS skill_count
FROM skills s
JOIN job_temp j ON j.skills LIKE CONCAT('%', s.skill, '%') OR j.description LIKE CONCAT('%', s.skill, '%')
GROUP BY s.skill
ORDER BY skill_count DESC;

#Add "City" column
SELECT * FROM job_temp;
ALTER TABLE job_temp
ADD city VARCHAR(255);

UPDATE job_temp
SET city = SUBSTRING_INDEX(location, ',', 1)
WHERE location LIKE '%, %';

#Extract company list to scrape industry information
SELECT DISTINCT company
FROM job_temp;

#Clean and update industry information
ALTER TABLE job_temp
ADD industry VARCHAR(255);

UPDATE job_temp jt
JOIN industry_dict ind
ON jt.company = ind.company
SET jt.industry = ind.industry;

#Frequencies of each industry
SELECT industry, COUNT(*) AS frequency
FROM job_temp
WHERE industry IS NOT NULL  
GROUP BY industry
ORDER BY frequency DESC;

#Other cleaning part
SELECT description, title, location, COUNT(*) AS count
FROM job_temp
GROUP BY description, title, location
HAVING COUNT(*) > 1;
    
SELECT * FROM job_temp 
WHERE salary IS NOT NULL and (annual_salary IS NULL or annual_salary = 0.00);

SELECT state, city, COUNT(*) AS job_count
FROM job_temp
GROUP BY state, city
ORDER BY state, city;

SELECT title, COUNT(*) AS title_freq FROM job_temp WHERE title LIKE '%associate%' GROUP BY title ORDER BY title_freq DESC;
SELECT title, COUNT(*) AS title_freq FROM job_temp GROUP BY title ORDER BY title_freq DESC;
SELECT COUNT(*) AS hr_count FROM job_temp WHERE salary LIKE '%hour%' or salary LIKE '%hr%';
SELECT COUNT(*) AS key_word_count FROM job_temp WHERE title LIKE '%portfolio%';

SELECT type, COUNT(*) AS model_count FROM job_temp GROUP BY type ORDER BY model_count DESC; 
SELECT industry, COUNT(*) AS industry_count FROM job_temp GROUP BY industry ORDER BY industry_count DESC;

SELECT 
    i.industry,
    j.salary_range,
    COUNT(*) AS salary_range_count
FROM job_temp j
JOIN industries i ON j.industry = i.industry
GROUP BY i.industry, j.salary_range
ORDER BY i.industry, salary_range_count DESC;

SELECT COUNT(*) AS type_count FROM job_temp WHERE type = 'Hy' or type = 'On' or type = 'Re';

SELECT salary, state FROM job_temp WHERE state = 'CA';