/* 
   ===========================================================
   Stored Procedure: Loading bronze Layer.  Source -> Bronze
   ===========================================================

   Purpose of Script: 
   This Stored procedure loads data into the 'bronze' schema from an external CSV files.
   What it does:
   - It truncates the bronze table before loading the data
   - It uses the 'BULK INSERT' command to load data from the external CSV files to bronze tables
   
   
   Parameters: This stored procedure does not accept any parameter or return any values

   Usage Example: (How to execute the procedure)
         EXEC bronze.load_bronze;
  ==============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading the bronze Layer';
		PRINT '===============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>>> Truncating Table: bronze.crm_cust_info';

		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting into Table: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Jonathan\Desktop\sql_ultimate\data_warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Duration of Load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '=================================================';

		SET @start_time = GETDATE();

		PRINT '>>> Truncating Table: bronze.crm_prd_info';

		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting into Table: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Jonathan\Desktop\sql_ultimate\data_warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration of Load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '=============================================';

		SET @start_time = GETDATE();

		PRINT '>>> Truncating Table: bronze.crm_sales_details';

		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Jonathan\Desktop\sql_ultimate\data_warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration of Load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '===================================================';

		SET @start_time = GETDATE();

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		PRINT '>>> Truncating Table: bronze.erp_cust_az12';

		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting into Table: bronze.erp_cust_az12';

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Jonathan\Desktop\sql_ultimate\data_warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration of Load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '==============================================';

		SET @start_time = GETDATE();

		PRINT '>>> Truncating Table: bronze.erp_loc_a101';

		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting into Table: bronze.erp_loc_a101';

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Jonathan\Desktop\sql_ultimate\data_warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR =  ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration of Load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '======================================================';

		SET @start_time = GETDATE();

		PRINT '>>> Truncating Table: bronze.erp_px_cat_g1v2';

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Jonathan\Desktop\sql_ultimate\data_warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration of Load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '================================================';

		SET @batch_end_time = GETDATE();
		PRINT '====================================================';
		PRINT 'Loading Bronze Layer is completed';
		PRINT 'Over all Load Time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds'; 
		PRINT '====================================================';

	END TRY
	BEGIN CATCH
		PRINT '====================================================';
		PRINT 'ERROR OCCURED DURING BRONZE LAYER LOADING';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '====================================================';
	END CATCH
END;


EXEC bronze.load_bronze;
