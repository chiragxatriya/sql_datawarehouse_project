/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
USE DataWareHouse;

-----Creating tables from the source (Bronze layer) (DDL Commands)------------------------------------
-----(1) Table crm_cust_info
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key	NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),	
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
-----(2) Table crm_prd_info
IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
-----(3) Table crm_sales_details
IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key	NVARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt INT,
	sls_ship_dt	INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
-----(4) Table erp_cust_az12
IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	CID	NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50)
);
-----(5) Table erp_loc_a101
IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	CID	VARCHAR(50),
	CNTRY NVARCHAR(50)
);
-----(6) Table erp_px_cat_g1v1
IF OBJECT_ID ('bronze.erp_px_cat_g1v1','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v1;
CREATE TABLE bronze.erp_px_cat_g1v1 (
	ID VARCHAR(50),
	CAT	VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50)
);
