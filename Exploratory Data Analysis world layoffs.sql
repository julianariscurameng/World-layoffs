USE world_layoffs;
# EXPLORATORY DATA ANALYSIS
SELECT * FROM layoffs_staging;

-- Max values of layoffs in total and in percentage
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging;

-- Info of 100% layoffs
SELECT * FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Info of 100% layoffs ordered by millions raised for funds
SELECT * FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- When layoffs started and ended
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging;

-- Total layoffs for each company
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

-- Total layoffs for each industry
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

-- Total layoffs for each country
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

-- Total layoffs for each company stage
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;

-- Total layoffs by year
SELECT YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY `year`
ORDER BY 2 DESC;

-- Rolling total layoffs by month
WITH rolling_total AS
(
	SELECT SUBSTRING(`date`, 1, 7) AS `month`,
		SUM(total_laid_off) AS rolling_layoffs
	FROM layoffs_staging
	WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
	GROUP BY `month`
	ORDER BY 1 ASC
)
SELECT `month`,
	rolling_layoffs,
	SUM(rolling_layoffs) OVER (ORDER BY `month`) AS rolling_total_layoffs
FROM rolling_total;

-- Rolling total layoffs by month across the years
WITH total_layoffs AS
(
	SELECT YEAR(`date`) AS `year`,
		MONTH(`date`) AS `month`,
		SUM(total_laid_off) AS total_laid_off
	FROM layoffs_staging
    WHERE YEAR(`date`) IS NOT NULL AND MONTH(`date`) IS NOT NULL
	GROUP BY YEAR(`date`), MONTH(`date`)
	ORDER BY `year`ASC , `month` ASC
)
SELECT `year`,
	`month`,
    total_laid_off,
    SUM(total_laid_off) OVER (PARTITION BY `year` ORDER BY `year`, `month`) AS rolling_total_layoffs
FROM total_layoffs;

-- Total layoffs by companies across the years
SELECT company, YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
GROUP BY company, `year`
ORDER BY company;

-- Companies with multiple layoffs across the years
WITH multiple_layoffs AS
(
	SELECT company,
    YEAR(`date`) AS `year`,
    SUM(total_laid_off) AS total_layoffs,
    ROW_NUMBER() OVER (PARTITION BY company ORDER BY company, YEAR(`date`)) AS row_num
	FROM layoffs_staging
	GROUP BY company, `year`
)
SELECT *
FROM multiple_layoffs
WHERE row_num >= 2
ORDER BY company, row_num;

-- Top 5 companies with the most layoffs across the years
WITH most_layoffs AS
(
	SELECT company,
		YEAR(`date`) AS `year`,
		SUM(total_laid_off) AS total_layoffs
	FROM layoffs_staging
    GROUP BY company, YEAR(`date`)
), layoffs_ranking AS
(
SELECT *,
	DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_layoffs DESC) AS ranking
FROM most_layoffs
WHERE `year` IS NOT NULL AND total_layoffs IS NOT NULL
ORDER BY ranking, `year`
)
SELECT * FROM layoffs_ranking
WHERE ranking <= 5
ORDER BY `year` ASC, ranking ASC;

-- Top 5 industries with the most layoffs across the years
WITH most_layoffs AS
(
	SELECT industry,
		YEAR(`date`) AS `year`,
		SUM(total_laid_off) AS total_layoffs
	FROM layoffs_staging
    GROUP BY industry, YEAR(`date`)
), layoffs_ranking AS
(
SELECT *,
	DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_layoffs DESC) AS ranking
FROM most_layoffs
WHERE `year` IS NOT NULL AND total_layoffs IS NOT NULL
ORDER BY ranking, `year`
)
SELECT * FROM layoffs_ranking
WHERE ranking <= 5
ORDER BY `year` ASC, ranking ASC;