-- Abstarct the cst_id for crm_customer_info

SELECT 
CASE WHEN cst_id LIKE 'NAS%' THEN SUBSTRING(cst_id,4,LEN(cst_id))
	 ELSE cst_id
	 END cst_id,
birth_Date,
gender
FROM Bronze.erp_cust_az12 
WHERE CASE WHEN cst_id LIKE 'NAS%' THEN SUBSTRING(cst_id,4,LEN(cst_id))
	  ELSE cst_id
	  END NOT IN (SELECT cst_key FROM Silver.crm_customer_info)


-- Check for INVALID Date:

SELECT 
		birth_Date
from Bronze.erp_cust_az12
WHERE birth_Date > GETDATE() OR birth_Date < '1924-01-01'

-- Check for SPACES

SELECT
		gender
FROM Bronze.erp_cust_az12
WHERE gender != TRIM(gender)

SELECT DISTINCT Gender FROM Bronze.erp_cust_az12

-- Table Transforation:-

INSERT INTO Silver.erp_cust_az12 (cst_id,birth_Date,gender)

SELECT
		CASE WHEN cst_id LIKE 'NAS%' THEN SUBSTRING(cst_id,4,LEN(cst_id))
			 ELSE cst_id
			 END cst_id,
		CASE WHEN birth_Date > GETDATE() THEN NULL
			 ELSE birth_Date
			 END birth_date,
		CASE WHEN UPPER(TRIM(gender)) IN ('Male','M') THEN 'Male'
			 WHEN UPPER(TRIM(gender)) IN ('Female','F') THEN 'Female'
	  		 ELSE 'N/A'
			 END gender
FROM Bronze.erp_cust_az12






-- Now for Silver.erp_loc_a101

SELECT
REPLACE(cst_id, '-', '') cst_id, country
FROM Bronze.erp_loc_a101
WHERE REPLACE(cst_id, '-', '') NOT IN (SELECT cst_key FROM Silver.crm_customer_info)

-- Data Standerlization & Consistency

SELECT DISTINCT country, 
CASE WHEN UPPER(TRIM(country)) IN ('USA','US', 'United States') THEN 'USA'
	 WHEN UPPER(TRIM(country)) IN ('DE', 'Germany') THEN 'Germany'
	 WHEN UPPER(TRIM(country)) IN ('', NULL) THEN NULL
	 ELSE TRIM(country)
	 END country
FROM Bronze.erp_loc_a101

-- Table Transformation:-


INSERT INTO Silver.erp_loc_a101 (cst_id,country)
SELECT 
		REPLACE(cst_id, '-', '') cst_id,
		CASE WHEN UPPER(TRIM(country)) IN ('USA','US', 'United States') THEN 'USA'
		WHEN UPPER(TRIM(country)) IN ('DE', 'Germany') THEN 'Germany'
		WHEN UPPER(TRIM(country)) IN ('', NULL) THEN NULL
		ELSE TRIM(country)
		END country
FROM Bronze.erp_loc_a101


-- Data Transfer of erp_px_cat_g1v2

-- Unwanted Spaces 

SELECT 
		category, sub_category, maintenance
FROM Bronze.erp_px_cat_g1v2
WHERE category != TRIM (category) OR sub_category != TRIM(sub_category) OR maintenance != TRIM(maintenance)

-- Data Standerdlization & Consistency

SELECT
		DISTINCT category
FROM Bronze.erp_px_cat_g1v2


SELECT
		DISTINCT sub_category
FROM Bronze.erp_px_cat_g1v2


SELECT
		DISTINCT maintenance
FROM Bronze.erp_px_cat_g1v2



-- Table Transformation:-
INSERT INTO Silver.erp_px_cat_g1v2 (cst_id,category,sub_category,maintenance)
SELECT
		cst_id,
		category,
		sub_category,
		maintenance
FROM Bronze.erp_px_cat_g1v2