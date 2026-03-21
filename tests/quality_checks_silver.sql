/*
==================================================================
   Quality Checks
==================================================================
Purpose of Scripts:
          This scripts performs various quality checks for Data consistency, accuracy, and standardization across both
		    'bronze' and 'silver' schema. It includes checks for:
		  - Null or duplicate primary keys
		  - Unwanted spaces in string fields
		  - Data standardization and consistency
		  - Invalid date range and orders
		  - Data consistency between related fields

Usage:
    - Run these checks before and after loading data to the silver layer
	- Investigate and retrieve any discrepancies found during the check

NOTE:
   - Do this check for all tables in the 'Bronze' layer, if results found clean and manupilate data where neccessary before inserting into 'silver' layer
   - Do thesame check for all tables in the 'Silver' layer. Expectations in the 'Silver' is "No Results"
*/


--- Checking for Duplicates or Null in the primary key
--- Expectations: No Results

SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


---- Quality Check on the 'silver' table after cleaning and inserting from the 'bronze' table

SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;




-- Checking for unwanted space
-- Expectations: No Results


SELECT 
	cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);


---- Quality Check on the 'silver' table after cleaning and inserting from the 'bronze' table

SELECT 
	cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);



SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


---- Quality Check on the 'silver' table after cleaning and inserting from the 'bronze' table


SELECT 
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


--- Data Standardization and consistency

SELECT DISTINCT
	cst_gndr
FROM bronze.crm_cust_info;


--- Bronze.crm_prd_info Cleaning and Manipulation

--- Checking for Null and Duplicate Values in Primary key

SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--- Check for Null or Negative Numbers
--- Expectations: No Results

SELECT
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


--- Data Standardization and Consistency

SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

--- Check for invalid Date Orders
--- Expectations: No Results

SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


--- Check for invalid date on sales Table

SELECT 
	NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
	OR LEN(sls_order_dt) != 8 
	OR sls_order_dt > 20500101;

--- Check Data Consistency: Between Sales, Quantity, and Price
--- Sales = Quantity * Price
--- Sales must not be NULL, Zero or Negative

SELECT DISTINCT
	sls_sales sls_sales_old,
	sls_quantity,
	sls_price,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END sls_sales_new,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
	     ELSE sls_price
    END sls_price_new
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


--- Identify out-of-range Dates

SELECT DISTINCT 
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()








