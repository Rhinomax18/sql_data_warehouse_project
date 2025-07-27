
/*
===============================================
Uplaoding Data from the internal storage to the SQL Server by using Stored Procedure
(Source --> Bronze)

Execute by 'EXEC bronze.load_bronze;'
===============================================
*/


CREATE OR ALTER PROCEDURE Bronze.load_bronze as 

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time = GETDATE();

			PRINT '==================================';
			PRINT 'LOADING Bronze Layer....';
			PRINT '==================================';

			PRINT '----------------------------------';
			PRINT 'Loading CRM Table';
			PRINT '----------------------------------';

			SET @start_time = GETDATE();
			PRINT 'Truncating the table : crm_customer_info';
			TRUNCATE TABLE bronze.crm_customer_info;

			PRINT 'Truncating the table INTO: crm_customer_info';
			Bulk insert bronze.crm_customer_info 
			FROM 'C:\Games\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
			PRINT '----------------'

			SET @start_time = GETDATE();
			PRINT 'Truncating the table : crm_product_info';
			TRUNCATE TABLE bronze.crm_product_info;

			PRINT 'Truncating the table INTO: crm_product_info';
			Bulk insert bronze.crm_product_info 
			FROM 'C:\Games\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
			PRINT '----------------'


			SET @start_time = GETDATE();
			PRINT 'Truncating the table : crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details;

			PRINT 'Truncating the table INTO: crm_sales_details';
			Bulk insert bronze.crm_sales_details 
			FROM 'C:\Games\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @Start_time, @end_time) as nvarchar) + ' seconds.' ;
			PRINT '-------------';


			PRINT '----------------------------------';
			PRINT 'Loading ERP Table';
			PRINT '----------------------------------';


			SET @start_time = GETDATE();
			PRINT 'Truncating the table : erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12;

			PRINT 'Truncating the table INTO: erp_cust_az12';
			Bulk insert bronze.erp_cust_az12 
			FROM 'C:\Games\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS nvarchar) + ' seconds';
			PRINT '-------------';


			SET @start_time = GETDATE();
			PRINT 'Truncating the table : erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101;

			PRINT 'Truncating the table INTO: erp_loc_a101';
			Bulk insert bronze.erp_loc_a101 
			FROM 'C:\Games\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds.'
			PRINT '-------------';


			SET @start_time = GETDATE();
			PRINT 'Truncating the table : erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;

			PRINT 'Truncating the table INTO: erp_px_cat_g1v2';
			Bulk insert bronze.erp_px_cat_g1v2 
			FROM 'C:\Games\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';
			PRINT '-------------';

			SET @batch_end_time = GETDATE();
			PRINT '==============================';
			PRINT 'LOADING BRONZE LAYER COMPLETE...';
			PRINT 'Load Duration of the Whole Batch: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) as nvarchar) + ' seconds.'
			PRINT '==============================';

	END TRY
	BEGIN CATCH
		PRINT '==================================';
		PRINT 'ERROR OCCUREED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==================================';
	END CATCH
END
	
