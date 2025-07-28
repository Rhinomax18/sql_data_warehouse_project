IF OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL
DROP TABLE Bronze.crm_customer_info;
create table bronze.crm_customer_info (
		cst_id INT,
		cst_key NVARCHAR(50),
		cst_fn NVARCHAR(50),
		cst_ln NVARCHAR(50),
		cst_marital_status NVARCHAR(50),
		cst_gender NVARCHAR(50),
		cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_product_info', 'U') IS NOT NULL
DROP TABLE Bronze.crm_product_info;
create table bronze.crm_product_info (
		prd_id INT,
		prd_key NVARCHAR(50),
		prd_name NVARCHAR(50),
		prd_cost INT,
		prd_line NVARCHAR(10),
		prd_start_date DATETIME,
		prd_end_date DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE Bronze.crm_sales_details;
create table bronze.crm_sales_details (
		sls_order_num NVARCHAR(50),
		sls_prd_key NVARCHAR(50),
		sls_cst_id INT,
		sls_order_date INT,
		sls_ship_date DATE,
		sls_due_date DATE,
		sls_sales INT,
		sls_quantity INT,
		sls_price INT
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE Bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
		cst_id NVARCHAR(50),
		birth_Date DATE,
		gender NVARCHAR (10)
);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE Bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
		cst_id NVARCHAR(50),
		country NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE Bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
		cst_id NVARCHAR(50),
		category NVARCHAR (50),
		sub_category NVARCHAR(50),
		maintenance NVARCHAR(50)
);

SELECT * FROM Bronze.crm_sales_details