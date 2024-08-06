USE world_layoffs;
# DATA CLEANING
SELECT * FROM layoffs;


-- STEPS OF DATA CLEANING:
	-- 1. Remove duplicates
    -- 2. Standardize data
    -- 3. Handle null and missing values
    -- 4. Remove any unnecessary column


-- CREATING COPY OF LAYOFFS TABLE
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;


-- 1. REMOVE DUPLICATES
-- 1.1 CHECK IF THERE IS ANY DUPLICATE ROW
WITH duplicate_cte AS
(
SELECT *,
	ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- 1.2 CREATING A DUPLICATE TABLE TO REMOVE DUPLICATE ROWS
CREATE TABLE `layoffs_staging2` (
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

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num = 2;

-- 1.3 DELETING DUPLICATE ROWS FROM THE DUPLICATE TABLE
DELETE FROM layoffs_staging2
WHERE row_num =2;

SELECT * FROM layoffs_staging2
WHERE row_num =2;

-- 1.4 SUBSTITUTE OLD TABLE WITH NEW TABLE WITHOUT DUPLICATE ROWS
DROP TABLE layoffs_staging;

RENAME TABLE layoffs_staging2 TO layoffs_staging;

SELECT * FROM layoffs_staging;

-- 2. STANDARDIZE DATA
-- 2.1 CLEANING 'company' COLUMN
UPDATE layoffs_staging
SET company = TRIM(company);

-- 2.2 CLEANING 'industry' COLUMN
SELECT DISTINCT industry FROM layoffs_staging
ORDER BY industry;

SELECT * FROM layoffs_staging
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry FROM layoffs_staging;

-- 2.3 CLEANING 'location' & 'country' COLUMN
SELECT DISTINCT location FROM layoffs_staging
ORDER BY location;

SELECT DISTINCT country FROM layoffs_staging
ORDER BY country;

SELECT country,
ROW_NUMBER() OVER (PARTITION BY country ORDER BY country) AS running_count
FROM layoffs_staging;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);

-- 2.4 CHANGING 'date' DATA TYPE & FORMAT
UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

-- 3. HANDLE NULL & MISSING VALUES
SELECT * FROM layoffs_staging
WHERE industry IS NULL OR industry = '';

-- 3.1 HANDLING MISSING VALUES IN THE 'industry' COLUMN
SELECT *
FROM layoffs_staging t1
JOIN layoffs_staging t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry= '')
	AND t2.industry IS NOT NULL;

SELECT t1.industry, t2.industry
FROM layoffs_staging t1
JOIN layoffs_staging t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry= '')
	AND t2.industry IS NOT NULL;

UPDATE layoffs_staging -- This is needed because it the following code did not succeed in updating the blank spaces
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging t1
JOIN layoffs_staging t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
	AND t2.industry IS NOT NULL;
    
SELECT t1.industry, t2.industry
FROM layoffs_staging t1
JOIN layoffs_staging t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry= '')
	AND t2.industry IS NOT NULL;
    
-- 3.2 HANDLING NULL VALUES IN THE 'industry' COLUMN
SELECT * FROM layoffs_staging
WHERE industry IS NULL;

-- 3.3 DELETING ROWS WITH NULL VALUES IN THE 'total_laid_off' & 'percentage_laid_off' COLUMNS
DELETE FROM layoffs_staging -- We delete these NULL values because we do not have enough data to fill missing values about layoffs
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- 4 REMOVE ANY UNNECESSARY COLUMN
-- 4.1 DROPPING 'row_num' COLUMN
ALTER TABLE layoffs_staging
DROP COLUMN row_num;
