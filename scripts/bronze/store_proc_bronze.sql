/*
======================================================================
Stored Procedure: Load the layer (SOURCE -----> BRONZE)
======================================================================
Script Purpose:
  - Load bronze layer from csv file
  - Truncate bronze tables before loading files
  - Use bulk insert to load the csv file
Usage:

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME ;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '****************************************************';
		PRINT '******************S T A R T*************************';
		PRINT '****************************************************';
		PRINT 'Clear Table First';
		PRINT ' LOAD bronze.crm_cust_info Files';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\nfons\Documents(LOCAL)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	
		PRINT '****************************************************';
		PRINT '******************E N D******************************';
		PRINT '****************************************************';


		PRINT 'Clear the Table First';
		PRINT ' LOAD bronze.crm_prd_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
	

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\nfons\Documents(LOCAL)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	

		PRINT '****************************************************';
		PRINT '******************E N D******************************';
		PRINT '****************************************************';


		PRINT 'Clear the Table First';
		PRINT ' LOAD bronze.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
	

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\nfons\Documents(LOCAL)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	

		PRINT '****************************************************';
		PRINT '******************E N D******************************';
		PRINT '****************************************************';

		PRINT 'Clear the Table First';
		PRINT ' LOAD bronze.erp_CUST_AZ12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
	

		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\nfons\Documents(LOCAL)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	

		PRINT '****************************************************';
		PRINT '******************E N D******************************';
		PRINT '****************************************************';

		PRINT 'Clear the Table First';
		PRINT ' LOAD bronze.erp_LOC_A101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101;
	

		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\nfons\Documents(LOCAL)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	

		PRINT '****************************************************';
		PRINT '******************E N D******************************';
		PRINT '****************************************************';

		PRINT 'Clear the Table First';
		PRINT ' LOAD bronze.erp_PX_CAT_G1V2';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
	

		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\nfons\Documents(LOCAL)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	SET @batch_end_time = GETDATE();
	PRINT '======================================================';
	PRINT ' LOADING Bronaze Layer Complete';
	PRINT ' Total load time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '======================================================';
	
	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'SOME SHIT BROKE WHILE LOADING BRONZE LAYER';
		PRINT 'OH SHIT'+ ERROR_MESSAGE();
		PRINT 'OH SHIT'+ CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'OH SHIT'+ CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================';
	END CATCH



END
