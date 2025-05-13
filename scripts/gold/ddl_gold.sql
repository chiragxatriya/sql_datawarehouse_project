/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO
CREATE VIEW gold.dim_customer AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY A.cst_id) customer_key,
	A.cst_id customer_id,
	A.cst_key customer_number,
	A.cst_firstname firstname,
	A.cst_lastname lastname,
	CASE WHEN A.cst_gndr = 'N/A' THEN B.gen
			ELSE A.cst_gndr
	END AS gender,
	C.cntry country,
	B.bdate birthdate,
	A.cst_marital_status marital_status,
	A.cst_create_date createdate
	
FROM silver.crm_cust_info A
LEFT JOIN silver.erp_cust_az12 B
ON	 A.cst_key = B.cid
LEFT JOIN silver.erp_loc_a101 C
ON	 C.cid = A.cst_key;
GO
-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO
CREATE VIEW gold.dim_product AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY A.prd_start_dt, A.dwh_prd_key) product_key,
	A.prd_id product_id,
	A.dwh_cat_id category_id,
	B.cat category,
	B.subcat subcategory,
	A.dwh_prd_key product_number,
	A.prd_nm product_name,
	A.prd_line product_line,
	B.maintenance,
	A.prd_cost product_cost,
	A.prd_start_dt product_startdate
	
FROM silver.crm_prd_info A
LEFT JOIN silver.erp_px_cat_g1v1 B
ON	 B.id = A.dwh_cat_id
WHERE A.prd_end_dt IS NULL --Filtering historical data;
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID ('gold.fact_sales' , 'V') IS NOT NULL
DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
	A.sls_ord_num order_number,
	C.product_key,
	B.customer_key,
	A.sls_order_dt order_date,
	A.sls_ship_dt shipping_date,
	A.sls_due_dt due_date,
	A.sls_sales sales_amount,
	A.sls_quantity quantity,
	A.sls_price price
FROM silver.crm_sales_details A
LEFT JOIN gold.dim_customer B
ON		  B.customer_id = A.sls_cust_id
LEFT JOIN gold.dim_product C
ON		  C.product_number = A.sls_prd_key;
GO
