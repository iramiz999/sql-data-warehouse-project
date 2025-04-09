/*
========================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================
Script Purpose:

This stored procedure loads data into the ‘silver’ schema from external CSV files.
It performs the following actions:

- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT` command to load data from CSV files to silver tables.

Parameters:
None.

This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC silver.load_bronze;
===================================================================================


*/
create or alter procedure silver.load_silver as
begin

declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime
begin try
set @batch_start_time = GETDATE()

print '########################';
print 'Loading Silver Layer';
print '########################';



print '============================'
print 'Loading CRM Tables'
print '============================'
	set @start_time = GETDATE()




	print '>> Truncating Table silver.crm_cust_info  '
	truncate table silver.crm_cust_info

	print '>> Inserting Data Into : silver.crm_cust_info '
	insert into silver.crm_cust_info (
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
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,


	case when  upper (trim(cst_material_status)) = 'S' then 'Single'
	when  upper(trim(cst_material_status)) = 'M' then 'Married'
	else 'n/a'

	end cst_material_status,

	case when  upper (trim(cst_gndr)) = 'F' then 'Female'
	when  upper(trim(cst_gndr)) = 'M' then 'Male'
	else 'n/a'

	end cst_gndr,
	cst_create_date
	from(


	select
	* ,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	)t where flag_last = 1 

	set @end_time = GETDATE()
	print '>> Load Duration : ' + cast(Datediff(second , @start_time , @end_time) as nvarchar) + 'seconds'
	print '>> -----------------------'




	set @start_time = GETDATE()
	print '>> Truncating Table silver.crm_prd_info '
	truncate table silver.crm_prd_info
	print '>> Inserting Data Into : silver.crm_prd_info '
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

	replace(substring(prd_key , 1, 5) , '-' , '_') as cat_id,
	substring(prd_key , 7 ,len(prd_key)) as prd_key,
	prd_nm,

	 isnull(prd_cost, 0 ) as prd_cost ,

	case upper(trim(prd_line))
		when  'M' then 'Mountain'
		when  'R' then 'Road'
		when  'S' then 'Other Sales'
		when  'T' then 'Touring'
		else 'n/a' 
		end as prd_line,

	cast (prd_start_dt as date) as prd_start_dt,
	DATEADD(MONTH, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt

	from bronze.crm_prd_info

	set @start_time = GETDATE()
	set @end_time = GETDATE()
	print '>> Load Duration : ' + cast(Datediff(second , @start_time , @end_time) as nvarchar) + 'seconds'
	print '>> -----------------------'





	set @start_time = GETDATE()
	print '>> Truncating Table silver.crm_sales_details'
	truncate table silver.crm_sales_details
	print '>> Inserting Data Into : silver.crm_sales_details '
	insert into silver.crm_sales_details (
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity ,
	sls_price 

	)

	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,

	case when sls_order_dt =0 or len(sls_order_dt) != 8 then null
		else cast( cast(sls_order_dt as varchar) as date)
		end as sls_order_dt ,

	case when sls_ship_dt =0 or len(sls_ship_dt) != 8 then null
		else cast( cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt ,


	case when sls_due_dt =0 or len(sls_due_dt) != 8 then null
		else cast( cast(sls_due_dt as varchar) as date)
		end as sls_due_dt ,

	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales
	,

	sls_quantity,
	case when sls_price is null or sls_price <=0
	then sls_sales / nullif(sls_quantity,0)
	else sls_price
	end as sls_price


	from bronze.crm_sales_details
	set @end_time = GETDATE()
	set @end_time = GETDATE()
	print '>> Load Duration : ' + cast(Datediff(second , @start_time , @end_time) as nvarchar) + 'seconds'
	print '>> -----------------------'


print '----------------------------'
print 'Loading ERP Tables'
print '----------------------------'

	set @start_time = GETDATE()
	print '>> Truncating Table silver.erp_cust_az12'
	truncate table silver.erp_cust_az12
	print '>> Inserting Data Into : silver.erp_cust_az12'
	insert into silver.erp_cust_az12(cid,bdate,gen)

	select 
	case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
	else cid
	end cid,
	case when bdate > getdate() then null
	else bdate
	end as bdate,

	case when upper(trim(gen)) in ('F' , 'Female') then 'Female'
	when upper(trim(gen)) in ('M' , 'Make') then 'Male'
	 else 'n/a'
	 end as gen
	from bronze.erp_cust_az12



	print '>> Truncating Table silver.erp_loc_a101'
	truncate table silver.erp_loc_a101
	print '>> Inserting Data Into : silver.erp_loc_a101'
	insert into silver.erp_loc_a101(cid, cntry)
	select 
	replace(cid, '-','') cid,
	cntry

	from bronze.erp_loc_a101 
	set @end_time = GETDATE()
	set @end_time = GETDATE()
	print '>> Load Duration : ' + cast(Datediff(second , @start_time , @end_time) as nvarchar) + 'seconds'
	print '>> -----------------------'




	set @start_time = GETDATE()

	print '>> Truncating Table silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2
	print '>> Inserting Data Into : silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2
	(id , cat , subcat, maintenance)
	select 

	id,
	cat,
	subcat,
	maintenance 
	from bronze.erp_px_cat_g1v2
	set @end_time = GETDATE()
	set @end_time = GETDATE()
	print '>> Load Duration : ' + cast(Datediff(second , @start_time , @end_time) as nvarchar) + 'seconds'
	print '>> -----------------------'
	



	set @batch_end_time = GETDATE()

	print '=============================='
	print 'Loading Silver Layer is Completed'
	print '- Total Load Duration  : ' + cast(datediff(second ,  @batch_start_time , @batch_end_time) as nvarchar) + 'seconds'
	print '=============================='

	
	end try
	begin catch

	print '================================================'
	print 'Error Occured During Loading Bronze Layer'
	print 'Error Message' + Error_Message()
	print 'Error Message' + Cast (ERROR_NUMBER() as nvarchar)
	print 'Error Message' + Cast (ERROR_STATE() as nvarchar)
	print '================================================'



	end catch

end

