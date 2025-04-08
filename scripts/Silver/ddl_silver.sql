
	/*
	===============================================================================
	DDL Script: Create Silver Tables
	===============================================================================
	Script Purpose:
		This script creates tables in the 'Silver' schema, dropping existing tables 
		if they already exist.
		  Run this script to re-define the DDL structure of 'Silver' Tables
	===============================================================================
	*/
	if OBJECT_ID(' silver.crm_cust_info','u') is not null
		drop table  silver.crm_cust_info;
	create table silver.crm_cust_info(
		cst_id int,
		cst_key nvarchar(50),
		cst_firstname nvarchar(50),
		cst_lastname nvarchar(50),
		cst_material_status nvarchar(50),
		cst_gndr nvarchar(50),
		cst_create_date date,
		DWH_create_date datetime2 default getdate()
	);

	if OBJECT_ID('silver.crm_prod_info','u') is not null
		drop table silver.crm_prod_info;
	create table silver.crm_prod_info(
		prd_id int,
		cat_id nvarchar(50),
		prd_key nvarchar(50),
		prd_name nvarchar(50),
		prd_cost int,
		prd_line nvarchar(50),
		prd_start_dt date,
		prd_end_dt date,
		DWH_create_date datetime2 default getdate()
	);


	if OBJECT_ID('silver.crm_sales_details','u') is not null
		drop table silver.crm_sales_details;
	create table silver.crm_sales_details(
		sls_ord_num nvarchar(50),
		sls_prd_key nvarchar(50),
		sale_cust_id int,
		sale_order_date date,
		sale_ship_date date,
		sale_due_date date,
		sale_sales int,
		sale_quantity int,
		sale_price int,
		DWH_create_date datetime2 default getdate()
	);


	if OBJECT_ID('silver.erp_loc_a101','u') is not null
		drop table silver.erp_loc_a101;
	create table silver.erp_loc_a101(
		cid nvarchar(50),
		cntry nvarchar(50),
		DWH_create_date datetime2 default getdate()
	);


	if OBJECT_ID('silver.erp_cust_az12','u') is not null
		drop table silver.erp_cust_az12;
	create table silver.erp_cust_az12(
		cid nvarchar(50),
		bdate date,
		gen nvarchar(50),
		DWH_create_date datetime2 default getdate()
	);


	if OBJECT_ID('silver.erp_px_cat_g1v2','u') is not null
		drop table  silver.erp_px_cat_g1v2;
	create table silver.erp_px_cat_g1v2(
		id nvarchar(50),
		cat nvarchar(50),
		subcat nvarchar(50),
		maintenance nvarchar(50),
		DWH_create_date datetime2 default getdate()
	);

