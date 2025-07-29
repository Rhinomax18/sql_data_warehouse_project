-- Execution of Silver.load_silver:-
                                   EXEC Silver.load_silver


CREATE OR ALTER PROCEDURE Silver.load_silver AS

BEGIN
            DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY 
            SET @batch_start_time = GETDATE();

            PRINT '=============================';
            PRINT '>> LOADING SILVER LAYER...';
            PRINT '=============================';


            PRINT '-----------------------------';
            PRINT '>> INSERTING CRM TABLES...';
            PRINT '-----------------------------';

    -- Table Transformation:- (Bronze.crm_customer_info to Silver.crm_customer_info)
    
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_customer_info';
    TRUNCATE TABLE Silver.crm_customer_info;
    PRINT '>> Inserting Data Into: silver.crm_customer_info';
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
	    		    WHERE cst_id IS NOT NULL
    )T WHERE FLAG = 1;
    SET @end_time = GETDATE();
    PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds.'
    PRINT '----------------------------------------------------------';


    -- Table Transformation:- (Bronze.crm_product_info to Silver.crm_product_info)
    
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_product_info';
    TRUNCATE TABLE Silver.crm_product_info;
    PRINT '>> Inserting Data Into: silver.crm_product_info';
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
      FROM [DataWarehouse].[Bronze].[crm_product_info];
      SET @end_time = GETDATE();
      PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds.';
      PRINT '----------------------------------------------------------';



    -- Table Transformation:- (Bronze.crm_sales_details to Silver.crm_sales_details)
    
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE Silver.crm_sales_details;
    PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
          FROM [DataWarehouse].[Bronze].[crm_sales_details];
          SET @end_time = GETDATE();
          PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds.';
          PRINT '----------------------------------------------------------';



    PRINT '-----------------------------';
    PRINT '>> INSERTING ERP TABLES';
    PRINT '-----------------------------';
    -- Table Transformation:- (Bronze.erp_cust_az12 to Silver.erp_cust_az12)

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE Silver.erp_cust_az12;
    PRINT '>> Inserting Data Into: silver.erp_cust_az12';
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
    FROM Bronze.erp_cust_az12;
    SET @end_time = GETDATE();
    PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds.';
    PRINT '----------------------------------------------------------';



    -- Table Transformation:- (Bronze.erp_loc_a101 to Silver.erp_loc_a101)

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE Silver.erp_loc_a101;
    PRINT '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO Silver.erp_loc_a101 (cst_id,country)
    SELECT 
		    REPLACE(cst_id, '-', '') cst_id,
		    CASE WHEN UPPER(TRIM(country)) IN ('USA','US', 'United States') THEN 'USA'
		    WHEN UPPER(TRIM(country)) IN ('DE', 'Germany') THEN 'Germany'
		    WHEN UPPER(TRIM(country)) IN ('', NULL) THEN NULL
		    ELSE TRIM(country)
		    END country
    FROM Bronze.erp_loc_a101;
    SET @end_time = GETDATE();
    PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds.';
    PRINT '----------------------------------------------------------';



    -- Table Transformation:- (Bronze.erp_px_cat_g1v2 to Silver.erp_px_cat_g1v2)

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE Silver.erp_px_cat_g1v2;
    PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO Silver.erp_px_cat_g1v2 (cst_id,category,sub_category,maintenance)
    SELECT
		    cst_id,
		    category,
		    sub_category,
		    maintenance
    FROM Bronze.erp_px_cat_g1v2;
    SET @end_time = GETDATE();
    PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds.';

    SET @batch_end_time = GETDATE();
    PRINT '===========================================';
    PRINT 'LOADING SILVER LAYER COMPLETE...';
    PRINT '===========================================';
    PRINT '>> LOAD COMPLETE DURATION: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds.'
    PRINT '===========================================';

    END TRY
    BEGIN CATCH
        PRINT '===============================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH
END
