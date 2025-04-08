	 /*
	===============================================================================
	Stored Procedure: Load Silver Layer (Bronze -> silver)
	===============================================================================
	Script Purpose:
		This stored procedure performs the ETL (Extract,Transform,Load) process to populate the 'Silver' schema tables form the
		'Bronze' schema

	 action performed:
		- Truncates Silver tables.
		-Inserted transofrmed and cleaned data from bronze to silver tables.

	Parameters:
		None. 
		  This stored procedure does not accept any parameters or return any values.

	Usage Example:
		EXEC silver.load_silver;
	===============================================================================
	*/
 
 exec silver.load_silver
 
  CREATE OR ALTER PROCEDURE silver.load_silver AS
 begin
	 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
		BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '================================================';
			PRINT 'inserting data into silver layer';
			PRINT '================================================';

			
			SET @start_time = GETDATE();

	print'>>truncating table:silver.crm_cust_info '
	truncate table silver.crm_cust_info
	print'>>Inserting data into:silver.crm_cust_info'
	insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date
	)
	select 
		cst_id,
		cst_key,
		ltrim(rtrim(cst_firstname)) as cst_firstname,
		ltrim(rtrim(cst_lastname)) as cst_lastname,
		case 
			when upper(ltrim(rtrim(cst_material_status))) = 'S' then 'single'
			when upper(ltrim(rtrim(cst_material_status))) = 'M' then 'married'
			else 'na'
		end as cst_material_status,
		case 
			when upper(ltrim(rtrim(cst_gndr))) = 'F' then 'female'
			when upper(ltrim(rtrim(cst_gndr))) = 'M' then 'male'
			else 'na'
		end as cst_gndr,
		cst_create_date
	from (	
		select *,
			   row_number() over (partition by cst_id order by cst_create_date desc) as last_flag
		from bronze.crm_cust_info
		where cst_id is not null
	) as t
	where last_flag = 1;
	
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



        SET @start_time = GETDATE();

	print'>>truncating table:silver.crm_prod_info '
	truncate table silver.crm_prod_info
	print'>>Inserting data into:silver.crm_prod_info'
	insert into silver.crm_prod_info(
		prd_id,
		cat_id,
		prd_key,
		prd_name,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_')as cat_id,
		SUBSTRING(prd_key,7,len(prd_key))as prd_key,
		prd_name,
		isnull(prd_cost,0) prd_cost,
		case  upper(LTRIM(RTRIM(prd_line)))
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other sales'
			 when 'T'then  'Touring'
			 else 'na'
		end prd_line,
		cast(prd_start_dt as date)prd_start_dt,
		cast (LEAD (prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date)as prd_end_dt
	from bronze.crm_prod_info;
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



        SET @start_time = GETDATE();
	print'>>truncating table:silver.crm_sales_details '
	truncate table silver.crm_sales_details
	print'>>Inserting data into:silver.crm_sales_details'
	insert into silver.crm_sales_details(
			sls_ord_num,
		sls_prd_key,
		sale_cust_id,
		sale_order_date,
		sale_ship_date ,
		sale_due_date ,
		sale_sales ,
		sale_quantity ,
		sale_price 
	)
	select	
			 sls_ord_num,
			 sls_prd_key,
			 sale_cust_id,
		case when sale_order_date= 0 or len(sale_order_date)!=8 then null
			 else cast(cast(sale_order_date as nvarchar) as date)
			end sale_order_date,

		case when sale_order_date= 0 or len(sale_order_date)!=8 then null
			 else cast(cast(sale_order_date as nvarchar) as date)
			end sale_ship_date,
		case when sale_due_date= 0 or len(sale_due_date)!=8 then null
			 else cast(cast(sale_due_date as nvarchar) as date)
			end sale_due_date,
		case when sale_sales is null or sale_sales<=0 or sale_sales!=sale_quantity*abs(sale_price) then sale_quantity*abs(sale_price)
			 else sale_sales
		end sale_sales,
			sale_quantity,
	
		case when sale_price is null or sale_price<=0 then 	sale_sales/nullif(sale_quantity,0)
			 else sale_price 
		end sale_price
	from bronze.crm_sales_details

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



        SET @start_time = GETDATE();

	print'>>truncating table:silver.erp_cust_az12 '
	truncate table silver.erp_cust_az12
	print'>>Inserting data into:silver.erp_cust_az12'
	insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)

	select 
		case when cid like'NAS%'then substring(cid,4,len(cid))
			 else cid
		end cid,
		case when bdate>GETDATE() then NULL
	   		 ELSE bdate
		end  bdate,
		case when upper(LTRIM(RTRIM(gen))) in ('F','FEMALE')then 'Female'
			 when (LTRIM(RTRIM(gen))) in('M','Male')then 'Male'
			 else 'na'
		end gen

	FROM bronze.erp_cust_az12

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


        SET @start_time = GETDATE();


	print'>>truncating table:silver.erp_loc_a101 '
	truncate table silver.erp_loc_a101
	print'>>Inserting data into:silver.erp_loc_a101'
	insert into silver.erp_loc_a101(
			cid,
			cntry
	)
	select 
		   replace(cid,'-','') as cid,
		   case when (LTRIM(RTRIM(cntry)))='DE' then 'Germany'
				when (LTRIM(RTRIM(cntry))) in ('US','USA') then 'United States'
				when (LTRIM(RTRIM(cntry)))=' 'or cntry is null then 'na'
				else cntry
		   end cntry

	from   bronze.erp_loc_a101 

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



        SET @start_time = GETDATE();

	print'>>truncating table:silver.erp_px_cat_g1v2 '
	truncate table silver.erp_px_cat_g1v2
	print'>>Inserting data into:silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2
		(
			id,
			cat,
			subcat,
			maintenance
		)
	select 
			id,
			cat,
			subcat,
			maintenance

	from bronze.erp_px_cat_g1v2
	
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Inserted data into silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING inserting data into silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
