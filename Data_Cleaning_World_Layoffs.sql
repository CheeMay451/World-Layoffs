-- SQL Data Cleaning
-- Kaggle Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
FROM world_layoffs.layoffs;

# Create a staging table of the raw data. This is the data we will work on. In case something happens, we still have the original data.
CREATE TABLE world_layoffs.layoffs_staging # Create table structure
LIKE world_layoffs.layoffs; 

SELECT *
FROM world_layoffs.layoffs_staging;

INSERT world_layoffs.layoffs_staging # Insert data into staging table from raw data
SELECT *
FROM world_layoffs.layoffs;

-- Start the process of Data Cleaning. Steps:
-- 1. Removes duplicates
-- 2. Standardise data and fix errors
-- 3. Look at NULL and black
-- 4. Removes any unecessary column



-- 1. Removes duplicates
SELECT *
FROM world_layoffs.layoffs_staging;

# Add row number
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) AS row_num # use backtick because date is a function in SQL.
FROM world_layoffs.layoffs_staging

# Put in CTE and find duplicates
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) AS row_num 
FROM world_layoffs.layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# Check some of the companies to see if they really have duplicates
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'; 
# Altho they look similar but they are not the same. 

# Therefore, we should add each column in the CTE.
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num 
FROM world_layoffs.layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# Check again
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Casper';

# Delete the duplicates
# Therefore, we should add each column in the CTE.
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num 
FROM world_layoffs.layoffs_staging
)

DELETE
FROM duplicate_cte
WHERE row_num > 1;
# The output said that CTE is not updatable. 

# Let's see the correct way to remove duplicates.
# Create table
CREATE TABLE `world_layoffs`.`layoffs_staging2` (		
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# Insert data
INSERT INTO world_layoffs.layoffs_staging2 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num 
FROM world_layoffs.layoffs_staging;

# Check for duplicates
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

# Delete duplicates
DELETE 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;





-- Standardise data and fix errors
SELECT *
FROM world_layoffs.layoffs_staging2;

# Removes whitespace in front and end of the company name
# Check for errors
SELECT company 
FROM world_layoffs.layoffs_staging2
ORDER BY 1; # ASC

# Trim the whitespace
SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

# Update to the table
UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);




# Look at industry
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;
# Seems like there are NULL and blank.

# Look at the NULL and blank
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL # Use 'IS', not '='
OR industry = '';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb'; # The other Airbnb company has defined the industry as Travel

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%'; # nothing wrong here

# Change all blank to NULL so that it would be easier for the following work. 
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry IS NULL 
OR industry = '';

# Check if all is NULL
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = '';

# Now we need to populate the NULL. How?
# For instance, Some Airbnb has definied their industries but some don't. 
# What we can do is to write a query to join the table by itself
# which allows each row in t1 to match with every row in t2 as long as they have the same company
# then update the NULL accordingly.
UPDATE world_layoffs.layoffs_staging2 AS t1
JOIN world_layoffs.layoffs_staging2 AS t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

# Look at the NULL again. Now, Bally is the only company with NULL in industry 
# but it cannot be populated since it doesn't have any matching rows with the same company
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL;




# There are different industry names for crypto. Some are crypto or cryptocurrency. Let's standardise them.
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';




# Look at date
SELECT *
FROM world_layoffs.layoffs_staging2;

# Convert it to date format 
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') # %m/%d/%Y specifies that which one is the m, d, and Y, and requires SQL to convert it to date format
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

# Convert the data type from text to date
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;



# Now, look at country
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1;
# Everything looks fine except that spelling of United States is not standardised.

# Removes period '.' at the end of the string
SELECT DISTINCT country,
TRIM(TRAILING '.' FROM country)
FROM world_layoffs.layoffs_staging2;

# Update to table
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);




-- 3. Look at NULL values and blank
SELECT *
FROM world_layoffs.layoffs_staging2;

-- NULL values now only exist in total_laid_off, percentage_laid_off, and funds_raised_millions. 
-- Nothing should be changed because they all look normal.




-- 4. Removes any unecessary columns and rows
# Delete any useless data
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 
# Since they are both NULL, that means we don't need them 
# because they can't provide us any useful information anyway

DELETE 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

# Drop the row_num column
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

# Final check on the dataset
SELECT *
FROM world_layoffs.layoffs_staging2; 
# FINISH






