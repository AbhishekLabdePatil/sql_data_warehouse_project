
/*
-------------------------------------------------
DDL Script:Create Bronze Tables  
-------------------------------------------------
Script purpose:
  This script creates tables in the 'bronze',schema,dropping existing tables 
if they already exist.
Run this script to re-define the DDL structure of Bronze table
---------------------------------------------------------------------------------
*/
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);

create table bronze.crm_prod_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_name nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime
);


create table bronze.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sale_cust_id int,
	sale_order_date int,
	sale_ship_date int,
	sale_due_date int,
	sale_sales int,
	sale_quantity int,
	sale_price int
);

create table bronze.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50)
);

create table bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);

create table bronze.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50)
);
