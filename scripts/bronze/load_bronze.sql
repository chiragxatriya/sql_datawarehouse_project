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
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'Loading the Bronze Layer';
		PRINT '=======================================================';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------';

		PRINT '>> Truncating table bronze.crm_cust_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting data into table bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'G:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '------------------';

		PRINT '>> Truncating table bronze.crm_prd_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting data into table bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'G:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '------------------';

		PRINT '>> Truncating table bronze.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting data into table bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'G:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '------------------';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting data into table bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'G:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '------------------';

		PRINT '>> Truncating table bronze.erp_loc_a101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting data into table bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'G:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '------------------';

		PRINT '>> Truncating table bronze.erp_px_cat_g1v1';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v1;
		PRINT '>> Inserting data into table bronze.erp_px_cat_g1v1';
		BULK INSERT bronze.erp_px_cat_g1v1
		FROM 'G:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '------------------';
		SET @batch_end_time = GETDATE();
		PRINT '>>Whole batch loading duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds';
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
