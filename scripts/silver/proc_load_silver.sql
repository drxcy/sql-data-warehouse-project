/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME ,@batch_start_time DATETIME, @end_time DATETIME ,@batch_end_time DATETIME;
	BEGIN TRY 
			SET @batch_start_time =GETDATE();
			Print '================================================';
			Print 'Loading Silver Layer';
			Print '================================================';
		
			Print '================================================';
			Print 'Loading CRM Tables';
			Print '================================================';

			-- Loading silver.crm_cust_info
			SET @start_time =GETDATE();

			Print '>> Truncating Tables : silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			Print '>> Inserting Data Into : silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info ( 
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_material_status,
				cst_gndr,
				cst_create_date
			)
			SELECT cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM (cst_lastname) AS cst_lastname ,
			CASE WHEN UPPER(TRIM(cst_material_status)) ='S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) ='M' THEN 'Married'
				ELSE 'n/a'
				END cst_material_status,

			CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
				ELSE 'n/a'
				END cst_gndr,
			cst_create_date
			FROM (
				SELECT 
						*,
						ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
						FROM bronze.crm_cust_info
						WHERE cst_id IS NOT NULL
					)t
					WHERE flag_last = 1;

					SET @end_time =GETDATE();
				Print '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time )AS VARCHAR)+ 'seconds';
				Print '>> -----------------------------';

				-- Loading silver.crm_prd_info
				SET @start_time = GETDATE();
				Print '>> Truncating Tables : silver.crm_prd_info';
				TRUNCATE TABLE silver.crm_prd_info
				Print '>> Inserting Data Into : silver.crm_prd_info';
				INSERT INTO silver.crm_prd_info(
					prd_id ,
					  cat_id ,
					  prd_key ,
					  prd_nm ,
					  prd_cost ,
					  prd_line,
					  prd_start_dt,
					  prd_end_dt
				)
				SELECT prd_id,
				REPLACE(SUBSTRING(prd_key,1,5), '-' ,'_') AS cat_id, --EXTRACT category ID
				SUBSTRING(prd_key ,7,len(prd_key)) AS prd_key, --EXTRACT Product KEY
				prd_nm,
				COALESCE(prd_cost,NULL,0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
						WHEN 'M' THEN 'Mountain'
						WHEN 'R' THEN 'Road'
						WHEN 'S' THEN 'OTHER SALES'
						WHEN 'T' THEN 'Touring'
						ELSE 'n/a'
						END AS prd_line, --Map product line into descriptive Value
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 
				AS DATE
				)AS prd_end_dt --calulate end_date as one day before the next start_date
				FROM bronze.crm_prd_info;
					SET @end_time =GETDATE();
				Print '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time )AS VARCHAR)+ 'seconds';
				Print '>> -----------------------------';

			-- Loading silver.crm_sales_details
			Set @start_time =GETDATE();
			Print '>> Truncating Tables : silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details
			Print '>> Inserting Data Into : silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details 
			( sls_ord_num ,
			  sls_prd_key ,
			  sls_cust_id ,
			  sls_order_dt,
			  sls_ship_dt ,
			  sls_due_dt,
			  sls_sales,
			  sls_quantity,
			  sls_price 
			  )
			SELECT sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,

				CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,

					CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
	
				CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END sls_sales, --Recalculate sales if orginal value is missing or incorrect
				sls_quantity,
				CASE WHEN sls_price IS NULL OR sls_price <=0 
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
				END sls_price -- Derived price if orginal price is missing
				FROM bronze.crm_sales_details;
				SET @end_time =GETDATE();
				Print '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time )AS VARCHAR)+ 'seconds';
				Print '>> -----------------------------';

				Print '================================================';
			Print 'Loading ERP Tables';
			Print '================================================';
			--Loading silver.erp_cust_az12
			Set @start_time =GETDATE();
		 Print '>> Truncating Tables : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12
		Print '>> Inserting Data Into : silver.erp_cust_az12';
		 INSERT INTO silver.erp_cust_az12 (
		 cid,
		 bdate,
		 gen)
		 SELECT 
		 CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove 'NAS' prefix if present
		 ELSE cid
		 END AS cid,
		 CASE WHEN bdate > GETDATE() THEN NULL --Set future date into NULL 
			ELSE bdate
			END AS bdate,
		 CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				Else 'n/a'
				END  AS gen --Normalize gender values and handles unknown values
		 FROM bronze.erp_cust_az12;
		 	SET @end_time =GETDATE();
			Print '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time )AS VARCHAR)+ 'seconds';
			Print '>> -----------------------------';

		--Loading silver.erp_loc_a101
		 SET @start_time = GETDATE();
	  Print '>> Truncating Tables : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101
	Print '>> Inserting Data Into : silver.erp_loc_a101';
	 INSERT INTO silver.erp_loc_a101
	 (cid,cntry)
	 SELECT 
	 REPLACE(cid,'-','') cid,
	  CASE 
		 WHEN UPPER(TRIM(cntry)) ='DE' THEN 'Germany'
		 WHEN UPPER(TRIM(cntry)) IN ('USA','US','United States') THEN 'United States'
		 WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	 END AS cntry -- Normalize and Handle Missing or blank country code
	 FROM bronze.erp_loc_a101;
	 	SET @end_time =GETDATE();
		Print '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time )AS VARCHAR)+ 'seconds';
		Print '>> -----------------------------';
		-- Loading silver.erp_px_cat_g1v2
		SET @start_time =GETDATE();
	  Print '>> Truncating Tables : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	Print '>> Inserting Data Into : silver.erp_px_cat_g1v2';
	 INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
	 SELECT 
			 id,
			 cat,
			 subcat,
			 maintenance 
	 FROM bronze.erp_px_cat_g1v2;
	 	SET @end_time =GETDATE();
Print '>> Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time )AS VARCHAR)+ 'seconds';
Print '>> -----------------------------';
SET @batch_end_time = GETDATE();
Print '==============================================';
Print 'Loading Silver Layer is Completed';
Print '>> Total Time Duration : ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR) + 'seconds';
Print '==============================================';
END TRY 
BEGIN CATCH 
Print '============================================';
		Print'ERROR OCCURED DURING LOADING SILVER LAYER';
		Print'ERROR MESSAGE ' +  CAST (ERROR_NUMBER() AS NVARCHAR);
		Print'ERROR MESSAGE ' + ERROR_MESSAGE();
		Print'ERROR MESSAGE ' +  CAST (ERROR_STATE() AS NVARCHAR);
		Print '============================================';
END CATCH 
END
