
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


exec bronze.load_bronze
 create or alter procedure bronze.load_bronze as
begin 
	declare @start_time datetime ,@end_time datetime
	begin try
	print'========================================================================'
	print'Loading bronze layer'
	print'========================================================================'
	print'------------------------------------------------------------------------'
	print'Loading CRM tables ' 
	print'------------------------------------------------------------------------'

	set @start_time =GETDATE();
	print'>> Truncating table: bronze.crm_cust_info'
	truncate table bronze.crm_cust_info
	print'>> Inserting data into: bronze.crm_cust_info'
	bulk insert  bronze.crm_cust_info
	from 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
			firstrow=2,
			fieldterminator=',',
			tablock		
	);
	set @end_time =GETDATE();
	print'>>Load Duration:'+cast(datediff(second,@start_time,@end_time)as nvarchar)+' second'
	print'>>-----------------'

	set @start_time =GETDATE();
    print'>> Truncating table: bronze.crm_prod_info'
	truncate table bronze.crm_prod_info
	print'>> Inserting data into: bronze.crm_prod_info'
	bulk insert bronze.crm_prod_info
	from 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
			firstrow=2,
			fieldterminator=',',
			tablock		
	);
    set @end_time =GETDATE();
	print'>>Load Duration:'+cast(datediff(second,@start_time,@end_time)as nvarchar)+' second'
	print'>>-----------------'

	set @start_time =GETDATE();
	  print'>> Truncating table: bronze.crm_sales_details'
	  truncate table bronze.crm_sales_details
	  print'>> Inserting data into: bronze.crm_sales_details'	
	  bulk insert bronze.crm_sales_details
		from 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
				firstrow=2,
				fieldterminator=',',
				tablock		
		);
		 set @end_time =GETDATE();
	print'>>Load Duration:'+cast(datediff(second,@start_time,@end_time)as nvarchar)+' second'
	print'>>-----------------'


	print'------------------------------------------------------------------------'
	print'Loading ERP tables ' 
	print'------------------------------------------------------------------------'
	
	set @start_time =GETDATE();
	print'>> Truncating table: bronze.erp_cust_az12'
	truncate table bronze.erp_cust_az12

	print'>> Inserting data into: bronze.erp_cust_az12'
	bulk insert bronze.erp_cust_az12
	from 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
			firstrow=2,
			fieldterminator=',',
			tablock		
	);
	set @end_time =GETDATE();
	print'>>Load Duration:'+cast(datediff(second,@start_time,@end_time)as nvarchar)+' second'
	print'>>-----------------'

	set @start_time =GETDATE();
	print'>> Truncating table: bronze.erp_loc_a101'
	truncate table bronze.erp_loc_a101
	
	print'>> Inserting data into: bronze.erp_loc_a101'
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
			firstrow=2,
			fieldterminator=',',
			tablock		
	);
	set @end_time =GETDATE();
	print'>>Load Duration:'+cast(datediff(second,@start_time,@end_time)as nvarchar)+' second'
	print'>>-----------------'

	set @start_time =GETDATE();
	print'>> Truncating table: bronze.erp_px_cat_g1v2'
	 truncate table bronze.erp_px_cat_g1v2
	
	print'>> Inserting data into: bronze.erp_px_cat_g1v2'
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with (
			firstrow=2,
			fieldterminator=',',
			tablock		
	);
	set @end_time =GETDATE();
	print'>>Load Duration:'+cast(datediff(second,@start_time,@end_time)as nvarchar)+' second'
	print'>>-----------------'

	end try
	begin catch
		print'========================================================================'
		print'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print'error message'+error_message();
		print'error message'+cast(error_number()as nvarchar)
		print'error message'+cast(error_state()as nvarchar)

		print'========================================================================'
	end catch
end
