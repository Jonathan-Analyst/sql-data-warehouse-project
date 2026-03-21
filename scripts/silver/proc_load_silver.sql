/* 
   ===========================================================
   Stored Procedure: Loading silver Layer.  Source -> silver
   ===========================================================

   Purpose of Script: 
   This Stored procedure performs the ETL (Extract, Transform, Load) process to insert the 'silver' schema tables from the 'bronze' schema
   What it does:
   - It truncates silver tables
   - It inserts transformed and cleansed data from the bronze into the silver table
   
   
   Parameters: This stored procedure does not accept any parameter or return any values

   Usage Example: (How to execute the procedure)
         EXEC silver.load_silver;
  ==============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================';
		PRINT ' Loading the Silver Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading the CRM Tables';
		PRINT '---------------------------------------------';

		----- Loading crm_cust_info Table
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table silver.crm_cust_info'; 
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>>> Inserting into Table silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'N/A'
			END cst_marital_status,  -- Normalized Marrital status values to readable format
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'N/A'
			END cst_gndr, --- Normalized Gender status values to readable format
			cst_create_date
		FROM(
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) Flag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL)t
		WHERE Flag = 1;  -- Select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> ------------------------------------------';

		----- Loading crm_prd_info Table
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;


		PRINT '>>> Inserting into Table silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_num,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,  --- Extract Category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key,         --- Extract Product Key
			prd_num,
			ISNULL(prd_cost, 0) prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END prd_line,                          ---Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) prd_end_dt  -- Calculate end date as one day before the next start date 
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> ------------------------------------------';

		----- Loading crm_sales_details Table
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;


		PRINT '>>> Inserting into Table silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			TRIM(sls_ord_num) sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) 
			END sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) 
			END sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) 
			END sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales 
			END sls_sales,   --- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END sls_price    --- Derive price if original value is incorrect
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> ------------------------------------------';

		PRINT '---------------------------------------------';
		PRINT 'Loading the ERP Tables';
		PRINT '---------------------------------------------';

		----- Loading erp_cust_az12 Table
		SET @end_time = GETDATE();
		PRINT '>>> Truncating Table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;


		PRINT '>>> Inserting into Table silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))   -- Remove 'NAS' Prefix if present
				ELSE cid
			END cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL    
				ELSE bdate
			END bdate,         --- Set future birthdates to NULL
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
				ELSE 'N/A' 
			END gen            --- Normalize Gender and Handle unknown and NULL cases
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> ------------------------------------------';


		----- Loading erp_loc_a101 Table
		SET @end_time = GETDATE();
		PRINT '>>> Truncating Table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>>> Inserting into Table silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT 
			REPLACE(cid, '-', '') cid,
			CASE
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ( 'USA', 'US') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) = '' OR UPPER(TRIM(cntry)) IS NULL THEN 'N/A'
				ELSE cntry
			END cntry     --- Normalise and handle missing or blank country codes
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> ------------------------------------------';


		----- Loading erp_px_cat_g1v2 Table
		SET @end_time = GETDATE();
		PRINT '>>> Truncating Table silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>>> Inserting into Table silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			TRIM(id) id,
			TRIM(cat) cat,
			TRIM(subcat) subcat,
			TRIM(maintenance) maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> ------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '>> ==========================================';
		PRINT '>> Loading the Silver Layer is completed';
		PRINT '>>> Total Load Duration ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> ==========================================';
	END TRY
	BEGIN CATCH
	PRINT '================================================';
	PRINT 'ERROR OCCURED WHILE LOADING SILVER LAYER'
	PRINT 'Error Message' + ERROR_NUMBER()
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR)
	PRINT '================================================';
	END CATCH
END



 















