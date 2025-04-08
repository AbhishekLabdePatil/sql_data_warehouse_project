/*
---------------------------------------------------------
Stored procedure: Load Bronze layer (source-> bronze)
---------------------------------------------------------
Script purpose:
this stored procedure load data into the bornze schema from external scv file.
it perform the following actions:
- Truncate the bronze tables before loading data.
- Use the 'bulk insert' command to loading data from csv file to bronze tables.
*/

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
bulk insert bronze.crm_cust_info
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
