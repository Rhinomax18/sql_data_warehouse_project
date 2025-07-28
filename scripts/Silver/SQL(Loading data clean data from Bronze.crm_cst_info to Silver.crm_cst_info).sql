-- Check for NULLs or Duplicates in the primary key

SELECT cst_id, COUNT(*) FROM Bronze.crm_customer_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Use Rank functions to find the latest cst_id in the table

SELECT * FROM (
		SELECT
		*, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) FLAG
		FROM Bronze.crm_customer_info
)T
WHERE FLAG != 1

-- Check for Unwanted Spaces

SELECT
cst_fn
FROM Bronze.crm_customer_info
WHERE LEN(cst_fn) != LEN(TRIM(cst_fn))

SELECT
cst_ln
FROM Bronze.crm_customer_info
WHERE LEN(cst_ln) != LEN(TRIM(cst_ln))



-- Table Transformation:-

INSERT INTO Silver.crm_customer_info (
		cst_id,
		cst_key,
		cst_fn,
		cst_ln,
		cst_marital_status,
		cst_gender,
		cst_create_date
)
SELECT
cst_id,
cst_key,
TRIM(cst_fn) cst_fn,
TRIM(cst_ln) cst_ln,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		  ELSE 'N/A'
		  END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
	 WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		  ELSE 'N/A'
		  END,
cst_create_date
FROM (
		
			SELECT
			*, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) FLAG
			FROM Bronze.crm_customer_info
)T WHERE FLAG = 1

SELECT * FROM Silver.crm_customer_info