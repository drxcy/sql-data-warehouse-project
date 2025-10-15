/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
	BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
		BEGIN TRY
		SET @batch_start_time =GETDATE();
		Print '============================================================================';
		Print 'Loading Bronze Layer';
		Print '============================================================================';

		Print '----------------------------------------------------------';
		Print 'Loading CRM Tables';
		Print '----------------------------------------------------------';

		SET @start_time = GETDATE();
		Print '>> Truncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		Print '>> Inserting Data Into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR=',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		Print '>> Loading Duration : '+  CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		Print '>> -------------------------------------------- <<';
		 -- SELECT COUNT(*) FROM bronze.crm_cust_info;

		SET @start_time = GETDATE();
		Print '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		Print '>> Inserting Data Into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR=',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		Print '>> Loading Duration : '+  CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		Print '>> -------------------------------------------- <<';

		 -- SELECT COUNT(*) FROM bronze.crm_prd_info;

		SET @start_time = GETDATE();
		Print '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		Print '>> Inserting Data Into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR=',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		Print '>> Loading Duration : '+  CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		Print '>> -------------------------------------------- <<';
		 --  SELECT COUNT(*) FROM bronze.crm_sales_details;

		Print '----------------------------------------------------------';
		Print 'Loading ERP Tables';
		Print '----------------------------------------------------------';

		SET @start_time = GETDATE();
		Print '>> Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		Print '>> Inserting Data Into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR=',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		Print '>> Loading Duration : '+  CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		Print '>> -------------------------------------------- <<';
		 -- SELECT COUNT(*) FROM bronze.erp_loc_a101;

		SET @start_time = GETDATE();
		Print '>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		Print '>> Inserting Data Into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR=',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		Print '>> Loading Duration : '+  CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		Print '>> -------------------------------------------- <<';
		  -- SELECT COUNT(*) FROM bronze.erp_cust_az12;

		SET @start_time = GETDATE();
		Print '>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		Print '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR=',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		Print '>> Loading Duration : '+  CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		Print '>> -------------------------------------------- <<';
		SET @batch_end_time =GETDATE();
		Print '====================================================';
		Print 'Loading Bronze Layer is Completed';
		Print 'Total Load Duration :  '+ CAST (DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) +'seconds';
		Print '====================================================';
		  --  SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		END TRY
		BEGIN CATCH
		Print '============================================';
		Print'ERROR OCCURED DURING LOADING BRONZE LAYER';
		Print'ERROR MESSAGE ' +  CAST (ERROR_NUMBER() AS NVARCHAR);
		Print'ERROR MESSAGE ' + ERROR_MESSAGE();
		Print'ERROR MESSAGE ' +  CAST (ERROR_STATE() AS NVARCHAR);
		Print '============================================';
		END CATCH
	END
