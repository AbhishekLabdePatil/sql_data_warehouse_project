/*--========================================================================
--Quality check:
--=======================================================================
Script pupose:
    This script performs various quality check for for data consistancy, accuracy, standardization across 
  the 'Silver' schema. It includes the checks for:
    -Null or duplicate primary keys.
    -Unwanted spaces from string fields.
    -Data standardzation and consistancy.
    -Invalid date range order.
    -Date consistancy between related fields
Usage:
    -Run these check after data load  in silver layer.
  - Investigate and reslove  any discrepancies found during the checks.
----------------------------------------------------------------------- 

-----------------------------------
--Data Quality check for table bronze.CRM_cust_info
------------------------------------
--check for nulls or duplicates in primary key:
--Expectaion: no result*/
select cst_id,count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null



--Check for unwanted spaces:
--Expectation: no result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != LTRIM(RTRIM(cst_firstname))

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != LTRIM(RTRIM(cst_lastname))


--Data standarzation & consistency:
select distinct(cst_gndr)
from bronze.crm_cust_info

--Data standarzation & consistency:
select distinct(cst_material_status)
from bronze.crm_cust_info

-------------------------------------------------------------------------------------------

--check for nulls or duplicates in primary key:
--Expectaion: no result
select cst_id,count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null



--Check for unwanted spaces:
--Expectation: no result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != LTRIM(RTRIM(cst_firstname))

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != LTRIM(RTRIM(cst_lastname))


--Data standarzation & consistency:
select distinct(cst_gndr)
from silver.crm_cust_info

--Data standarzation & consistency:
select distinct(cst_material_status)
from silver.crm_cust_info


select count(cst_id) 
from silver.crm_cust_info

truncate table silver.crm_cust_info


-----------------------------------
--Data Quality check for table bronze.CRM_prod_info
------------------------------------

select prd_id ,count(*)
from bronze.crm_prod_info
group by prd_id
having count(*)>1
or  prd_id is null

--Check for unwanted spaces:
--Expectation: no result
SELECT prd_name
FROM bronze.crm_prod_info
WHERE prd_name != LTRIM(RTRIM(prd_name))

--check for null or negative number:
--expectation: no result
select prd_cost
from bronze.crm_prod_info
where prd_cost <0 or prd_cost is null

--Data standardization and consistancy:
select distinct(prd_line)
from bronze.crm_prod_info

------------------------------------------------


select prd_id ,count(*)
from silver.crm_prod_info
group by prd_id
having count(*)>1
or  prd_id is null

--Check for unwanted spaces:
--Expectation: no result
SELECT prd_name
FROM silver.crm_prod_info
WHERE prd_name != LTRIM(RTRIM(prd_name))

--check for null or negative number:
--expectation: no result
select prd_cost
from silver.crm_prod_info
where prd_cost <0 or prd_cost is null

--Data standardization and consistancy:
select distinct(prd_line)
from silver.crm_prod_info

select count(*)
from silver.crm_prod_info


-----------------------------------
--Data Quality check for table bronze.crm_sales_details
------------------------------------

--check invalid order dates:
select 
	nullif(sale_order_date,0)sale_order_date
from bronze.crm_sales_details
where sale_order_date <=0 
or len(sale_order_date)!=8
or sale_order_date >20500101
or sale_order_date <19000101

--check invalid ship dates:
select 
	nullif(sale_ship_date,0)sale_ship_date
from bronze.crm_sales_details
where sale_ship_date <=0 
or len(sale_ship_date)!=8
or sale_ship_date >20500101
or sale_ship_date <19000101

--check invalid due dates:
select 
	nullif(sale_due_date,0)sale_due_date
from bronze.crm_sales_details
where sale_due_date <=0 
or len(sale_due_date)!=8
or sale_due_date >20500101
or sale_due_date <19000101


--check for is it orde date> ship date or order date> due date:
select *
from bronze.crm_sales_details
where sale_order_date > sale_ship_date 
or sale_order_date > sale_due_date

--Check data consitancy between sales,quantity and price
-->>sales=quantity*price
-->>values must not be null,zero,negative
select sale_sales,sale_quantity,sale_price
from bronze.crm_Sales_details
where sale_sales!= sale_quantity*sale_price
or sale_sales is null or sale_quantity is null or sale_price is null
or sale_sales <=0 or sale_quantity <=0 or sale_price<=0

--------------------------------------------------------------------
--check invalid order dates:
select nullif(sale_order_date,0)sale_order_date
from silver.crm_sales_details
where sale_order_date <=0 
or len(sale_order_date)!=8
or sale_order_date >20500101
or sale_order_date <19000101

--check invalid ship dates:
select 
	nullif(sale_ship_date,0)sale_ship_date
from silver.crm_sales_details
where sale_ship_date <=0 
or len(sale_ship_date)!=8
or sale_ship_date >20500101
or sale_ship_date <19000101

--check invalid due dates:
select 
	nullif(sale_due_date,0)sale_due_date
from silver.crm_sales_details
where sale_due_date <=0 
or len(sale_due_date)!=8
or sale_due_date >20500101
or sale_due_date <19000101


--check for is it orde date> ship date or order date> due date:
select *
from silver.crm_sales_details
where sale_order_date > sale_ship_date 
or sale_order_date > sale_due_date

--Check data consitancy between sales,quantity and price
-->>sales=quantity*price
-->>values must not be null,zero,negative
select sale_sales,sale_quantity,sale_price
from silver.crm_Sales_details
where sale_sales!= sale_quantity*sale_price
or sale_sales is null or sale_quantity is null or sale_price is null
or sale_sales <=0 or sale_quantity <=0 or sale_price<=0


select *
from silver.crm_sales_details

-----------------------------------
--Data Quality check for table bronze.erp_cust_az12
------------------------------------
select 
	case when cid like'NAS%'then substring(cid,4,len(cid))
		else cid
	end cid,
	bdate,
	gen

FROM bronze.erp_cust_az12
where cid not in (select cst_key
				from silver.crm_cust_info
			   )

			   select cst_key
				from silver.crm_cust_info

--Identify out of range date
select distinct bdate
from bronze.erp_cust_az12
where bdate <'1924-01-01' or bdate>getdate()

--data standarzation and consistancy:
select distinct gen,
case when upper(LTRIM(RTRIM(gen))) in ('F','FEMALE')then 'Female'
	 when (LTRIM(RTRIM(gen))) in('M','Male')then 'Male'
	 else 'na'
end gen
from bronze.erp_cust_az12

------------------------------------------------------------------------------

select 
	case when cid like'NAS%'then substring(cid,4,len(cid))
		else cid
	end cid,
	bdate,
	gen

FROM silver.erp_cust_az12
where cid not in (select cst_key
				from silver.crm_cust_info
			   )

			   select cst_key
				from silver.crm_cust_info

--Identify out of range date
select distinct bdate
from silver.erp_cust_az12
where bdate <'1924-01-01' or bdate>getdate()

--data standarzation and consistancy:
select distinct gen,
case when upper(LTRIM(RTRIM(gen))) in ('F','FEMALE')then 'Female'
	 when (LTRIM(RTRIM(gen))) in('M','Male')then 'Male'
	 else 'na'
end gen
from silver.erp_cust_az12

select *
from silver.erp_cust_az12


-----------------------------------
--Data Quality check for table bronze.erp_loc_a101
------------------------------------
select replace(cid,'-','') as cid,
	   cntry
from bronze.erp_loc_a101 
where replace(cid,'-','') not in (select cst_key
								  from silver.crm_cust_info
								 )
----------------------------------------------------------

--Data Standarization and consistancy:
select distinct(cntry)
from bronze.erp_loc_a101
order by cntry

----------------------------------------------------
select replace(cid,'-','') as cid,
	   cntry
from silver.erp_loc_a101 
where replace(cid,'-','') not in (select cst_key
								  from silver.crm_cust_info
								 )
----------------------------------------------------------

--Data Standarization and consistancy:
select distinct(cntry)
from silver.erp_loc_a101
order by cntry


-----------------------------------
--Data Quality check for table bronze.erp_px_cat_g1v2
------------------------------------
select id,cat,subcat,maintenance
from bronze.erp_px_cat_g1v2
where  cat!=(LTRIM(RTRIM(cat))) or  subcat!=(LTRIM(RTRIM(subcat))) or maintenance!=(LTRIM(RTRIM(maintenance)))


--Data Standarization and consistancy:
select distinct(subcat)
from bronze.erp_px_cat_g1v2

-------------------------------------------------------------------------------

-----------------------------------
--Data Quality check for table bronze.erp_px_cat_g1v2
------------------------------------
select id,cat,subcat,maintenance
from silver.erp_px_cat_g1v2
where  cat!=(LTRIM(RTRIM(cat))) or  subcat!=(LTRIM(RTRIM(subcat))) or maintenance!=(LTRIM(RTRIM(maintenance)))


--Data Standarization and consistancy:
select distinct(subcat)
from silver.erp_px_cat_g1v2
