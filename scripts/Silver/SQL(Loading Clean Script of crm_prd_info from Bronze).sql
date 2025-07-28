-- Check for Duplicates or NULLs in crm_product_info

SELECT
	prd_id, COUNT(*)
FROM Bronze.crm_product_info
GROUP BY prd_id
HAVING COUNT(*) > 1

-- Check the data NOT IN the erp_px_cat_g1v2

SELECT [prd_id]
      ,[prd_key]
      ,REPLACE(SUBSTRING(prd_key,1,5), '-', '_') as cat_id
      ,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
      ,[prd_name]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_date]
      ,[prd_end_date]
  FROM [DataWarehouse].[Bronze].[crm_product_info]
  WHERE REPLACE(SUBSTRING(prd_key,1,5), '-', '_') NOT IN 
  (SELECT cst_id from Bronze.erp_px_cat_g1v2)


  -- Check the Data NOT IN the crm_sales_details


  SELECT [prd_id]
      ,[prd_key]
      ,REPLACE(SUBSTRING(prd_key,1,5), '-', '_') as cat_id
      ,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
      ,[prd_name]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_date]
      ,[prd_end_date]
  FROM [DataWarehouse].[Bronze].[crm_product_info]
  WHERE SUBSTRING(prd_key,7,LEN(prd_key)) IN 
  (SELECT sls_prd_key from Bronze.crm_sales_details)
  

  -- Check for Unwanted Spaces
  -- Expection: No Result

  SELECT
        prd_name,
        TRIM(prd_name),
        LEN(prd_name),
        LEN(TRIM(prd_name))
  FROM Bronze.crm_product_info
 -- WHERE LEN(prd_name) != LEN(TRIM(prd_name))


 -- Check for NULLs or Negative Numbers
 -- Expecion: No Results

 SELECT
        *
 FROM Bronze.crm_product_info
 WHERE prd_cost IS NULL OR prd_cost < 0


 -- Data Standerdization & Consistency
 SELECT DISTINCT prd_line FROM Bronze.crm_product_info


 -- Check for Invalid Date Orders
 SELECT * FROM Bronze.crm_product_info 
 WHERE prd_end_date < prd_start_date

 -- FROM the above CASE we will use LEAD(prd_start_date) which will be replaced by prd_end_date (Data Enrichment)

 SELECT prd_key,prd_start_date, CAST(LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date) - 1 AS DATE) as prd_end_date from Bronze.crm_product_info 



 -- Change the DATETIME --> DATE

 IF OBJECT_ID('silver.crm_product_info', 'U') IS NOT NULL
DROP TABLE silver.crm_product_info;
create table silver.crm_product_info (
		prd_id INT,
        cat_id NVARCHAR(50),
		prd_key NVARCHAR(50),
		prd_name NVARCHAR(50),
		prd_cost INT,
		prd_line NVARCHAR(50),
		prd_start_date DATE,
		prd_end_date DATE,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);




INSERT INTO Silver.crm_product_info (
            prd_id,
            cat_id,
            prd_key,
            prd_name,
            prd_cost,
            prd_line,
            prd_start_date,
            prd_end_date
)
  SELECT prd_id
      ,REPLACE(SUBSTRING(prd_key,1,5), '-', '_') as cat_id  
      ,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key 
      ,prd_name
      ,ISNULL(prd_cost,0) prd_cost
      ,CASE UPPER(TRIM(prd_line)) 
                     WHEN 'M' THEN 'Mountain'
                     WHEN 'R' THEN 'Road'
                     WHEN 'S' THEN 'Other Sales'
                     WHEN 'T' THEN 'Touring'
                     ELSE 'N/A'
       END prd_line
      ,CAST(prd_start_date AS DATE) as prd_start_date
      ,CAST(LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date) - 1 AS DATE) as prd_end_date
  FROM [DataWarehouse].[Bronze].[crm_product_info]