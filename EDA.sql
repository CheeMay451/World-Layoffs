-- Exploratory Data Analysis
-- Explore data and identify any trends / patterns / outliers in the dataset.

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

# percentage of laid off = 1 means bankrupt.
# Look at companies that completely went under (bankrupt). 
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1 
ORDER BY total_laid_off DESC; # Check out which company has the largest laid off when they completely went under.

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC; # Check out companies that have a lot of funding

# Look at companies and their total laid off in summary 
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

# Look at date range
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# See which industries got hit the most
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# See which countries got impacted the most
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
# US is by far the most 

# See total laid off in each year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


# See which stage has the most laid off
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;
# seed funding = initial amount of funding of startup companies raised to cover costs.
# series A to J = stages of raising capital

-- Rolling total
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE `month` IS NOT NULL # Error because WHERE clause runs before any alias created in the SELECT statement.
GROUP BY `month`
ORDER BY 1;

# The correct method is either use original column name:
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL # Error because WHERE clause runs before any alias created in the SELECT statement.
GROUP BY `month`
ORDER BY 1;

# Or use HAVING clause, which also works the same
# Use HAVING after GROUP BY and use ALIAS
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `month`
HAVING `month` IS NOT NULL
ORDER BY 1;


# Let's look at the accumulated number of laid off in each month and year. 
# To do the rolling total, I need to use the CTE.
WITH rolling_total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_fired # necessary to rename the column so that SQL can identify it
FROM layoffs_staging2
GROUP BY `month`
HAVING `month` IS NOT NULL
ORDER BY 1
) 

SELECT `month`, total_fired, SUM(total_fired) OVER (ORDER BY `month`) AS accumulated_number # not necessary to do partition here because we alr used GROUP BY
FROM rolling_total;



# Practice again without looking at the previous query
WITH rolling_total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_fired
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL # Error because WHERE clause runs before any alias created in the SELECT statement.
GROUP BY `month`
ORDER BY 1
)

SELECT `month`, total_fired, SUM(total_fired) OVER (ORDER BY `month`) AS accu_num
FROM rolling_total;
# From 2022-05 to 2023-03, the report indicates 383159 laid off around the world.

# This query looks at total layoffs per year for each companies. 
# Some companies have multiple layoffs in 3 years
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 1;

# This identifies which companies had the largest layoffs in 3 years
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;



-- This query is particularly difficult because it is a bit confusing.

-- This query does the following:
-- 1. It calculates the total layoffs per company for each year (using SUM(total_laid_off)).
-- 2. It ranks companies within each year based on this total:
-- a. The company with the highest total layoffs in a given year receives Rank 1.
-- b. The company with the second-highest total receives Rank 2, and so on.

-- So, the final result will show:
-- For each year, the list of companies ordered by their total layoffs.
-- The company with the most layoffs in each year has Rank 1.

WITH comp_layoffs (Company, Years, Total_Layoffs) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)

SELECT *, 
DENSE_RANK () OVER (PARTITION BY Years ORDER BY Total_Layoffs DESC) AS Ranking
FROM comp_layoffs
WHERE Years IS NOT NULL
ORDER BY Ranking;
# In 2020, Uber has the biggest layoffs
# In 2021, Bytedance has the biggest layoffs
# In 2022, Meta has the biggest layoffs
# In 2023, Google has the biggest layoffs

# Let's add another CTE inside the CTE.
WITH comp_layoffs (Company, Years, Total_Layoffs) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
),

company_layoffs_rank AS # second CTE
(
SELECT *, 
DENSE_RANK () OVER (PARTITION BY Years ORDER BY Total_Layoffs DESC) AS Ranking
FROM comp_layoffs
WHERE Years IS NOT NULL
# ORDER BY Ranking - Remove this
# If I don't remove this, it will arrange the ranking in ascending.
# Example, it shows rank 1 from 2021 to 2023 first, then rank 2 from the 2021 to 2023, and so on.
)

SELECT *
FROM company_layoffs_rank
WHERE Ranking <= 5; # This look at top five highest layoffs from 2020 to 2023
