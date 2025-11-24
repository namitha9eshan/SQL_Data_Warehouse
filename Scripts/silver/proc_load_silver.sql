/*
========================================================================================================================================
Stored Procedure : Load Silver Layer (Bronze -> Silver)
========================================================================================================================================
Script Purpose : This Stored Procedure performs the ETL process to populate the silver schema tables from the 'bronze' schema.
Action performed : 
  - Truncate silver tables 
  - Insert transformed and cleansed data from Bronze into Silver Tables

Parameters : None. This Stored Procedure does not accept any parameters or return any values.

Usage Example : Exec silver.load_silver;
========================================================================================================================================
*/


create or alter procedure silver.load_silver as 
begin 
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try 
		set @batch_start_time = getdate();
		print'================================================================================================'
		print 'Loading Silver Layer'
		print '================================================================================================'
	-- Insering Data into the silver Layer 
		print'-------------------------------------------------------------------------------------------------'
		print 'Loading CRM tables'
		print '-------------------------------------------------------------------------------------------------'

	-- Inserting Data Into The silver.crm_cst_info 
		set @start_time = GETDATE();
		print '>> Trancating table silver.crm_cst_info'
		truncate table silver.crm_cust_info;
		print '>> Inserting data into silver.crm_cst_info'
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select 
		cst_id,
		cst_key,

		-- trimming the unwanted spaces 
		trim(cust_firstname) as cust_firstname,
		trim(cst_lastname) as cst_lastname ,

		-- Data Consistency and Standardization 
		case when upper(trim(cst_marital_status)) = 'M' then 'Married'
			 when upper(trim(cst_marital_status)) = 'S' then 'Single'
			 else 'n/a'
		end cst_marital_status,
		case when cst_gndr = 'F' then 'Female'
			 when cst_gndr = 'M' then 'Male'
			 else 'n/a'
		end cst_gndr,

		cst_create_date

		-- Getting rid of duplicates and null values in the cst_id column
		from(
		select *,
			row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
			from 
			bronze.crm_cust_info
			where cst_id is not null 
		)t where flag_last = 1;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time , @end_time) as nvarchar) + ' seconds'
		print '>>-------------------------------'






	-- Inserting Data Into The silver.crm_prd_info
		set @start_time = GETDATE();
		print '>> Trancating table silver.crm_prd_info'
		truncate table silver.crm_prd_info;
		print '>> Inserting data into silver.crm_prd_info'
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select
		prd_id, 

		replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		substring(prd_key, 7, len(prd_key)) as prd_key,  
		prd_nm,
		isnull(prd_cost,0) as prd_cost,

		case upper(trim(prd_line))
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other'
			 when 'T' then 'Touring'
			 else 'n/a'
		end prd_line,
		--This is data enrichment : adding new values to your data
		prd_start_dt,
		dateadd(day, -1 ,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt_test 
		from bronze.crm_prd_info;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time , @end_time) as nvarchar) + ' seconds'
		print '>>-------------------------------'









	-- inserting data into silver.crm_sales_details 
		set @start_time = GETDATE();
		print '>> Trancating table silver.crm_sales_details'
		truncate table silver.crm_sales_details;
		print '>> Inserting data into silver.crm_sales_details'
		insert into silver.crm_sales_details(
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
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			 else cast(cast(sls_order_dt  as varchar) as date) 
		end as sls_order_dt,
	
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			 else cast(cast(sls_ship_dt  as varchar) as date) 
		end as sls_ship_dt,

		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			 else cast(cast(sls_due_dt  as varchar) as date) 
		end as sls_due_dt,

		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
			 then sls_quantity * abs(sls_price)
			 else sls_sales
		end as sls_sales,	
		sls_quantity,
		case when sls_price is null or sls_price <= 0 
			 then sls_sales / nullif(sls_quantity, 0)
			 else sls_price 
		end as sls_price 
		from bronze.crm_sales_details;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time , @end_time) as nvarchar) + ' seconds'
		print '>>-------------------------------'











	-- Inserting cleaned data into silver.erp_CUST_AZ12
		set @start_time = GETDATE();
		print '>> Trancating table silver.erp_CUST_AZ12'
		truncate table silver.erp_CUST_AZ12;
		print '>> Inserting data into silver.erp_CUST_AZ12'
		insert into silver.erp_CUST_AZ12(
		cid,
		bdate,
		gen
		)
		select
		case when cid like 'NAS%' then substring(cid, 4, len(cid))
			else cid
		end cid, 

		case when bdate > getdate() then null
			 else bdate
		end bdate, 

		case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
			 when upper(trim(gen)) in ('M', 'MALE') then 'Male'
			 else 'n/a'
		end as gen 
		from bronze.erp_CUST_AZ12;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time , @end_time) as nvarchar) + ' seconds'
		print '>>-------------------------------'






	-- Inserting  clean data into the silver.erp_LOC_A101
		set @start_time = GETDATE();
		print '>> Trancating table silver.erp_LOC_A101'
		truncate table silver.erp_LOC_A101;
		print '>> Inserting data into silver.erp_LOC_A101'
		insert into silver.erp_LOC_A101(
		cid,
		cntry
		)
		select 
		replace(cid,'-', '') cid,

		case when trim(CNTRY) = 'DE' then 'Germany'
			 when trim(CNTRY) in ('US', 'USA') then 'United States'
			 when trim(CNTRY) = '' or CNTRY is null then 'n/a'
			 else trim(CNTRY)
		end as CNTRY 
		from bronze.erp_LOC_A101;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time , @end_time) as nvarchar) + ' seconds'
		print '>>-------------------------------'




	-- Inserting  clean data into the silver.erp_PX_CAT_G1V2
		set @start_time = GETDATE();
		print '>> Trancating table silver.erp_PX_CAT_G1V2'
		truncate table silver.erp_PX_CAT_G1V2;
		print '>> Inserting data into silver.erp_PX_CAT_G1V2'
		insert into silver.erp_PX_CAT_G1V2(
		id,
		cat,
		SUBCAT,
		MAINTENANCE
		)
		select 
		id, 
		cat,
		subcat,
		maintenance
		from bronze.erp_PX_CAT_G1V2;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time , @end_time) as nvarchar) + ' seconds'
		print '>>-------------------------------'


		set @batch_end_time = getdate();
		print '===================================================================================================='
		print 'Loading Silver Layer is Completed';
		print ' - Total Load Duration : ' + cast(datediff(second,@batch_start_time, @batch_end_time) as nvarchar)  + ' seconds'
		print '===================================================================================================='

	end try 
	begin catch
		print '========================================================================================================='
		print 'Error Occured During Loading Silver Layer'
		print 'Error Message' + error_message();
		print 'Error Message' + cast (error_number() as nvarchar);
		print 'Error Message' + cast (error_state() as nvarchar);
		print '========================================================================================================='
	end catch
end 
