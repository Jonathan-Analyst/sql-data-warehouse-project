
/*
   ===============================================================================================
   DDL Scripts: Create Gold Views
   ===============================================================================================
   Script Purpose:
         This scripts creates views for the Gold Layer in the Data warehouse
		 The Gold Layer represents the final dimension and fact tables (Star Schema)

		 Each view performs transformations and and combines data from the silver layer
		 to produce a clean, enriched, and business ready datasets
	Usage: 
	   These views can be queried directly for analytics and reporting
   ===============================================================================================
*/

-- ===============================================================================================
-- Create dimension: gold.dim_customers
-- ===============================================================================================

CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	lo.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr   --- CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'N/A')
	END gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 lo
	ON ci.cst_key = lo.cid;


-- ===============================================================================================
-- Create dimension: gold.dim_products
-- ===============================================================================================

CREATE OR ALTER VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_num AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; --- Filter out all historical data


-- ===============================================================================================
-- Create Fact: gold.fact_sales
-- ===============================================================================================

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
	s.sls_ord_num AS order_number,
	pr.product_key,
	c.customer_key,
	s.sls_order_dt AS order_date,
	s.sls_ship_dt AS ship_date,
	s.sls_due_dt AS due_date,
	s.sls_sales AS sales_amount,
	s.sls_quantity AS quantity,
	s.sls_price AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_products pr
	ON s.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers c
	ON s.sls_cust_id = c.customer_id;

