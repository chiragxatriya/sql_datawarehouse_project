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
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME, @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================================================';
		PRINT 'Loading the Silver Layer';
		PRINT '====================================================================';
		PRINT '--------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------';
		---(1) silver.crm_cust_info table
		SET @start_time = GETDATE();
		PRINT '>> Truncating the table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting the data into silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr, 
			cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'N/A' END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 ELSE 'N/A' END AS cst_gndr,
			cst_create_date
		FROM
			(SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) Flag
			FROM bronze.crm_cust_info)B 
		WHERE Flag = 1 AND cst_id IS NOT NULL;
		SET @end_time = GETDATE();
		PRINT '--------------------------';
		PRINT 'Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds'
		PRINT '--------------------------------------------------------------------';
		---(2) silver.crm_prd_info table
		SET @start_time = GETDATE();
		PRINT '>> Truncating the table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting the data into silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info 
				(prd_id,
				prd_key,
				dwh_cat_id,
				dwh_prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt)
		SELECT 
			prd_id,
			prd_key,
			REPLACE(SUBSTRING(TRIM(prd_key),1,5),'-','_') AS 'dwh_cat_id',
			SUBSTRING(TRIM(prd_key),7,DATALENGTH(prd_key)) AS 'dwh_prd_key',
			TRIM(prd_nm) prd_nm,
			ISNULL(prd_cost,0) prd_cost,
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'N/A'
			END AS prd_line,
			CAST(prd_start_dt AS DATE),
			CAST((LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1) AS DATE) prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '--------------------------';
		PRINT 'Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds'
		PRINT '--------------------------------------------------------------------';
		---(3) silver.crm_sales_details table
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------------------';
		PRINT '>> Truncating the table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting the data into silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details
				(sls_ord_num,
				 sls_prd_key,
				 sls_cust_id,
				 sls_order_dt,
				 sls_ship_dt,
				 sls_due_dt,
				 sls_sales,
				 sls_quantity,
				 sls_price)
		SELECT
			TRIM(sls_ord_num) sls_ord_num,
			TRIM(sls_prd_key) sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt <= 0 OR DATALENGTH(CAST(sls_order_dt AS VARCHAR)) <> 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
			CASE WHEN sls_ship_dt <= 0 OR DATALENGTH(CAST(sls_ship_dt AS VARCHAR)) <> 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
			CASE WHEN sls_due_dt <= 0 OR DATALENGTH(CAST(sls_due_dt AS VARCHAR)) <> 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL THEN ABS(sls_price) * sls_quantity
			 ELSE sls_sales END AS sls_price,
			sls_quantity,
			CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN ABS(sls_sales) / sls_quantity
			 ELSE sls_price END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '--------------------------';
		PRINT 'Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds'
		PRINT '--------------------------------------------------------------------';
		---(4) silver.erp_cust_az12 table
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------------------';
		PRINT '>> Truncating the table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting the data into silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,DATALENGTH(cid))
				 ELSE cid END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate,		 
			CASE UPPER(gen)
			WHEN '' THEN NULL
			WHEN 'F' THEN 'Female'
			WHEN 'M' THEN 'Male'
			ELSE gen END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '--------------------------';
		PRINT 'Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds'
		PRINT '--------------------------------------------------------------------';
		---(5) silver.erp_loc_a101 table
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------------------';
		PRINT '>> Truncating the table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting the data into silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			REPLACE(cid,'-','') cid,
			CASE WHEN LOWER(TRIM(CNTRY)) IN ('usa' , 'us') THEN 'United States'
				 WHEN LOWER(TRIM(CNTRY)) = 'de' THEN 'Germany'
				 WHEN CNTRY = '' OR CNTRY IS NULL THEN 'N/A'
				 ELSE TRIM(CNTRY)
			END AS CNTRY
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '--------------------------';
		PRINT 'Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds'
		PRINT '--------------------------------------------------------------------';
		---(6) silver.erp_px_cat_g1v1 table
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------------------';
		PRINT '>> Truncating the table silver.erp_px_cat_g1v1';
		TRUNCATE TABLE silver.erp_px_cat_g1v1;
		PRINT '>> Inserting the data into silver.erp_px_cat_g1v1';
		INSERT INTO silver.erp_px_cat_g1v1 (id, cat, subcat, maintenance)
		SELECT 
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM bronze.erp_px_cat_g1v1;
		SET @end_time = GETDATE();
		PRINT '--------------------------';
		PRINT 'Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds'
		PRINT '--------------------------------------------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT 'Whole Batch Loading Duration :' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) AS VARCHAR) + ' Seconds'
		PRINT '--------------------------------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
