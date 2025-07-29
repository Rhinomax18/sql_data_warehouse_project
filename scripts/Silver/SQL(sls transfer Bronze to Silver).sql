-- Check that the table [crm_product_info] is not having a problem joining with the sls_cst_info.

SELECT  [sls_order_num]
      ,[sls_prd_key]
      ,[sls_cst_id]
      ,[sls_order_date]
      ,[sls_ship_date]
      ,[sls_due_date]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[Bronze].[crm_sales_details]
  WHERE sls_prd_key NOT IN (SELECT prd_key FROM Silver.crm_product_info)


  -- Check that the table [crm_customer_info] is not having a problem joining with the sls_cst_info.

  SELECT  [sls_order_num]
      ,[sls_prd_key]
      ,[sls_cst_id]
      ,[sls_order_date]
      ,[sls_ship_date]
      ,[sls_due_date]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[Bronze].[crm_sales_details]
  WHERE sls_cst_id  NOT IN (SELECT cst_id FROM Silver.crm_customer_info)

  -- Check for any Invalid DATE for sls_order_date

  SELECT NULLIF(sls_order_date , 0) sls_order_date FROM Bronze.crm_sales_details WHERE sls_order_date <=  0 OR LEN(sls_order_date) !=8

  -- Check for any Invalid DATE Order

  SELECT * FROM Bronze.crm_sales_details WHERE sls_order_date > sls_ship_date OR sls_order_date > sls_due_date


  -- Check NULLs or Negative or Zeroes in Sales, quantity and price

  SELECT sls_sales FROM Bronze.crm_sales_details WHERE sls_sales IS NULL OR  sls_sales <= 0  --Issue FOUND (NULLs , Negative , Zeroes)
   SELECT sls_quantity FROM Bronze.crm_sales_details WHERE sls_quantity IS NULL OR  sls_quantity <= 0 -- No Issue
    SELECT sls_price FROM Bronze.crm_sales_details WHERE sls_price IS NULL OR  sls_price <= 0   --Issue FOUND (NULLs , Zeroes)
    -- OR
    SELECT sls_sales, sls_quantity, sls_price FROM Silver.crm_sales_details 
    WHERE sls_sales != sls_quantity * sls_price OR
          sls_price IS NULL OR  sls_price <= 0 OR
          sls_sales IS NULL OR  sls_sales <= 0
    ORDER BY sls_sales, sls_quantity, sls_price

-- Check that Sales = Quantity * Price

SELECT * FROM Bronze.crm_sales_details WHERE sls_sales != sls_quantity * sls_price



--Change the DATA TYPE of the Silver.crm_sales_details:-

IF OBJECT_ID('Silver.crm_sales_details' , 'U') IS NOT NULL
    DROP TABLE Silver.crm_sales_details;
    CREATE TABLE Silver.crm_sales_details (
            sls_order_num nvarchar(50),
            sls_prd_key nvarchar(50),
            sls_cst_id INT,
            sls_order_date DATE,
            sls_ship_date DATE,
            sls_due_date DATE,
            sls_sales INT,
            sls_quantity INT,
            sls_price INT,
            sls_create_date DATETIME2 DEFAULT GETDATE()
                                        )

   -- Table Transformation:-


   INSERT INTO Silver.crm_sales_details (
            sls_order_num,
            sls_prd_key,
            sls_cst_id,
            sls_order_date,
            sls_ship_date,
            sls_due_date,
            sls_sales,
            sls_quantity,
            sls_price
   )
    SELECT  [sls_order_num]
          ,[sls_prd_key]
          ,[sls_cst_id]
          ,CASE WHEN sls_order_date = 0 OR LEN(sls_order_date) !=8 THEN NULL
                ELSE CAST(CAST(sls_order_date AS VARCHAR) AS DATE) 
                END sls_order_date
          ,CASE WHEN sls_ship_date = 0 OR LEN(sls_ship_date) !=8 THEN NULL
                ELSE CAST(CAST(sls_ship_date AS VARCHAR) AS DATE) 
                END sls_ship_date
          ,CASE WHEN sls_due_date = 0 OR LEN(sls_due_date) !=8 THEN NULL
                ELSE CAST(CAST(sls_due_date AS VARCHAR) AS DATE) 
                END sls_due_date
          , CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales
                 END sls_sales
          ,[sls_quantity]  
          ,CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
          ELSE sls_price
          END sls_price
      FROM [DataWarehouse].[Bronze].[crm_sales_details]


      SELECT * FROM Silver.crm_sales_details