/*
========================================================================================================================================
Quality Checks 
=========================================================================================================================================
Script Purpose : 
This scripts performs various quality checks for data consistency , accuracy , and standardization across 'Silver' schemas. It includes checks for ;
- Null or Duplicate Primary Keys 
- Unwanted Spaces in String fields 
- Data Standardization and Consistency 
- Invalid Date Ranges or Orders
- Data Consistency Between Related Fields 

Usage Notes:
  - Run these checks after data loading Silver Layer
  - Investigate and resolve any discrepencies found during the checks 

=========================================================================================================================================

*/




-- Quality checking from the bronze layer(crm_cst_info)

select *
from silver.crm_cust_info
;

-- Checking for duplicates and null values 
select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

--Check for unwanted spaces in String values 
select cst_firstname 
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

select cst_lastname 
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);

select cst_marital_status 
from silver.crm_cust_info
where cst_marital_status != trim(cst_marital_status);

select cst_gndr
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr);

-- Data Consistency and Standardozaion 

select distinct cst_marital_status
from silver.crm_cust_info;

select distinct cst_gndr
from silver.crm_cust_info;





-- Quality checking from the bronze layer(crm_prd_info)

select * 
from silver.crm_prd_info;

-- Checking for duplicates in prd_id

select prd_id, count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or  prd_id is null ;

--Product key is split into 2 extra columns (cat_id and) : it is in the insert clean data into silver layer query

-- checking the unwanted spaces in the prd_nm column
select prd_nm from
bronze.crm_prd_info
where prd_nm != trim(prd_nm);

-- Checking the null or negative numbers from prd_cost 
select prd_cost  from 
silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Checking prd_line 
select distinct prd_line
from silver.crm_prd_info;

-- Fixing the date 
select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
dateadd(day, -1 ,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt_test 
from silver.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R' , 'AC-HE-HL-U509')





-- Quality checking from the bronze layer(crm_sales_details)




-- negative values since the date is in int 
select 
nullif(sls_order_dt, 0) sls_order_dt 
from bronze.crm_sales_details 
where sls_order_dt <= 0 or len(sls_order_dt) != 8;

-- check for invalid date orders 
select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- Check for data consistency 
-- >> Sales = Quantity * Price 
-- >> Values must not be null, zero or negative
-- >> Please follow the rules below
-- >>>>>>>>>If sales are negative,zero or null, derive it using Quantity and Price.
-- >>>>>>>>>If price is zero or null, calculate it using sales and quantity.
-- >>>>>>>>>If price is negative convert it to a positive value.

select distinct
sls_sales as old_sls_sales ,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	 then sls_quantity * abs(sls_price)
	 else sls_sales
end as sls_sales,

case when sls_price is null or sls_price <= 0 
	 then sls_sales / nullif(sls_quantity, 0)
	 else sls_price 
end as sls_price 

from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity  is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0 
order by sls_sales,sls_quantity,sls_price;





-- Checking the quality of the bronze.erp_cust_az12

select * 
from bronze.erp_CUST_AZ12;

-- Idnetify dates that are out of range

select bdate    -- Handled invalid values 
from silver.erp_CUST_AZ12
where bdate < '1925-01-01' or BDATE > getdate();  

select distinct gen -- Handled missing valiues and data normalization
from silver.erp_CUST_AZ12;

select * 
from silver.erp_CUST_AZ12;

-- What I have done :  handled invalid values, 









-- Checking the quality of the bronze.erp_loc_a101

select distinct cntry as 'old cntry',
case when trim(CNTRY) = 'DE' then 'Germany'
	 when trim(CNTRY) in ('US', 'USA') then 'United States'
	 when trim(CNTRY) = '' or CNTRY is null then 'n/a'
	 else trim(CNTRY)
end as CNTRY     -- Checking data normalization and consistency 
from silver.erp_LOC_A101
order by CNTRY;

select * 
from silver.erp_LOC_A101;



-- Quality checking bronze.erp_PX_CAT_G1V2
select CAT
from bronze.erp_PX_CAT_G1V2
where cat != trim(cat);

select SUBCAT
from bronze.erp_PX_CAT_G1V2
where SUBCAT != trim(SUBCAT);

select MAINTENANCE
from bronze.erp_PX_CAT_G1V2
where MAINTENANCE != trim(MAINTENANCE);

-- Data Standardization & Consistency 
select distinct 
cat 
from bronze.erp_PX_CAT_G1V2;

select distinct 
subcat
from bronze.erp_PX_CAT_G1V2;

select distinct 
MAINTENANCE
from bronze.erp_PX_CAT_G1V2;
