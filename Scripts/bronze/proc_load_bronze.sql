--  Data Inserintg to the tables 'Using bulk insert'
/*
==============================================================================================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
==============================================================================================================================

Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performes the following actions:
    -Truncate the bronze tables before loading 
    -Uses 'Bulk Insert' to load data from CSV files to bronze tables.

Parameters:
  None.
  This Stored Procedure does not accept any parameters or return any values.

Usage Exapmle: 
  execute bronze.load_bronze;
*/


create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime , @batch_end_time datetime;
	begin try 
		set @batch_start_time = getdate();
		print('===========================================================================');
		print('Loading Bronze Layer.......');
		print('===========================================================================');

		print('----------------------------------------------------------------------------');
		print('Loading CRM Tables')
		print('----------------------------------------------------------------------------');

		set @start_time = getdate();
		print('>> Truncating Table : bronze.crm_cust_info');
		truncate table bronze.crm_cust_info;
		print('>> Inserting Data Into : bronze.crm_cust_info');
		bulk insert bronze.crm_cust_info
		from 'C:\Users\User\Desktop\My Learning\SQL\Data Warehouse Project\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2 ,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------------'


		set @start_time = getdate();
		print('>> Truncating Table : bronze.crm_prd_info');
		truncate table bronze.crm_prd_info;
		print('>> Inserting Data Into : bronze.crm_prd_info');
		bulk insert bronze.crm_prd_info
		from 'C:\Users\User\Desktop\My Learning\SQL\Data Warehouse Project\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------------'


		set @start_time = getdate();
		print('>> Truncating Table : bronze.crm_sales_details');
		truncate table bronze.crm_sales_details;
		print('>> Inserting Data Into : bronze.crm_sales_details');
		bulk insert bronze.crm_sales_details
		from 'C:\Users\User\Desktop\My Learning\SQL\Data Warehouse Project\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------------'


		set @start_time = getdate();
		print('>> Truncating Table : bronze.erp_CUST_AZ12');
		truncate table bronze.erp_CUST_AZ12;
		print('>> Inserting Data Into : bronze.erp_CUST_AZ12');
		bulk insert bronze.erp_CUST_AZ12
		from 'C:\Users\User\Desktop\My Learning\SQL\Data Warehouse Project\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------------'


		set @start_time = getdate();
		print('>> Truncating Table : bronze.erp_LOC_A101');
		truncate table bronze.erp_LOC_A101;
		print('>> Inserting Data Into : bronze.erp_LOC_A101');
		bulk insert bronze.erp_LOC_A101
		from 'C:\Users\User\Desktop\My Learning\SQL\Data Warehouse Project\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------------'


		set @start_time = getdate();
		print('>> Truncating Table : bronze.erp_PX_CAT_G1V2');
		truncate table bronze.erp_PX_CAT_G1V2;
		print('>> Inserting Data Into : bronze.erp_PX_CAT_G1V2');
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\Users\User\Desktop\My Learning\SQL\Data Warehouse Project\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------------'

		set @batch_end_time = getdate();
		print '>> Loading Bronze Layer is Completed';
		print '>> Total Duraton ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds.'
	end try 
	begin catch
		print '-------------------------------------------------------';
		print 'Error Occured During Loading Bronze Layer';
		print 'Error Message' + Error_Message();
		print 'Error Number' + cast (Error_number() as nvarchar);
		print 'Error Number' + cast (Error_state() as nvarchar);
		print('-------------------------------------------------------');
	end catch 
end 
