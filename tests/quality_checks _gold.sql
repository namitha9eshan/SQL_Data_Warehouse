/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- Checking the quality of gold.dim_customers object 

select distinct gender
from gold.dim_customer

-- Checking the quality of  the gold.dim_product object 
-- 1. Check the prd_key is unique with count(*)
-- 2. Check if we have the same information twice(Same Column)

select prd_key, count(*)
from (
	select 
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost,
		pn.prd_start_dt,
		pc.CAT,
		pc.SUBCAT,
		pc.MAINTENANCE
	from silver.crm_prd_info pn
	left join silver.erp_px_cat_g1v2 pc
	on pn.cat_id = pc.id 
	where prd_end_dt is null
)t group by prd_key 
having count(*) > 1;   

-- checking the gold.dim_product is good 

select * from gold.dim_products;


-- Checking the uality of the gold.fact_sales 
select *
from gold.fact_sales;

-- Connect the whole data model inorder to find issues 

select * 
from gold.fact_sales f 
left join gold.dim_customer c 
on c.customer_key = f.customer_key
left join gold.dim_products p  
on p.product_key  = f.product_key 
where p.product_key is null;
