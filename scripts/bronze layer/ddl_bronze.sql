===================================================================

THIS IS to create a file, the T sql logic redefines the file to clear all data and start again

The inpute of data may differ as per the organisational file's location 

the measurement of speed of the etl process in the code 


====================================================================



if object_id ('bronze.crm_cust_info' , 'U') is not null 
	drop table bronze.crm_cust_info;
Create table bronze.crm_cust_info(
cst_id int,
 cst_key varchar(50),
 cst_firstname varchar(50), 
 cst_lastname varchar(50),
 cst_material_status varchar (50),
cst_gndr varchar (50),
cst_create_date Date
);
 if object_id ('bronze.crm_prd_info' , 'U') is not null 
	drop table bronze.crm_prd_info;
 Create table bronze.crm_prd_info( 
 prd_id int,
 prd_key varchar(50),
 prd_nm varchar(50),
 prd_cost varchar(50),
 prd_line varchar(50),
 prd_start_dt date,
 prd_end_dt date
 );
  if object_id ('bronze.crm_sales_details' , 'U') is not null 
	drop table bronze.crm_sales_details;
 Create table bronze.crm_sales_details(
 sls_ord_num varchar(50),
 sls_prd_key varchar(50),
 sls_cust_id varchar(50),
 sls_order_dt date,
 sls_ship_dt date,
 sls_due_dt date,
 sls_sale varchar(50),
 sls_quantity varchar(50),
 sls_price varchar(50)
 );
 if object_id ('bronze.erp_Cust_AZ12' , 'U') is not null 
	drop table bronze.erp_Cust_AZ12;
 create table bronze.erp_Cust_AZ12(
CID varchar (50),
BDATE date, 
GEN varchar (50)
);
 if object_id ('bronze.erp_LOC_A101' , 'U') is not null 
	drop table bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101(
CID varchar(50),
CNTRY varchar(50)
);
if object_id ('bronze.erp_PX_CAT_G1V2' , 'U') is not null 
	drop table bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50)
);

Create or alter procedure bronze.load_bronze As
Begin 
		Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
		Begin Try
			set @batch_start_time = getdate();
			print '=====================================================';
			print 'Loading Bronze layer';
			print '=====================================================';

			print '-----------------------------------------------------';
			print 'loading CRM Tables';
			print '-----------------------------------------------------';
			
			set @start_time = getdate();
			print '>> Truncating Table:bronze.crm_cust_info';
			
			Truncate table bronze.crm_cust_info;
			print '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Lenovo\Desktop\data portfolio\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		set @end_time= getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'-----------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table:bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Lenovo\Desktop\data portfolio\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with ( 
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time= getdate();
		print '>> Load duration; ' + cast (datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print'-----------------------------------------';


		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details
		print '>> Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Lenovo\Desktop\data portfolio\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time= getdate();
		print '>> Load duration; ' + cast (datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print'-----------------------------------------';


		print '-----------------------------------------------------';
			print 'loading ERP Tables';
		print '-----------------------------------------------------';


		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_Cust_AZ12';
		truncate table bronze.erp_Cust_AZ12
		print '>> Inserting Data Into: bronze.erp_Cust_AZ12';
		bulk insert bronze.erp_Cust_AZ12
		from 'C:\Users\Lenovo\Desktop\data portfolio\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with ( 
		firstrow = 2, 
		fieldterminator = ',',
		tablock
		);
		set @end_time= getdate();
		print '>> Load duration; ' + cast (datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print'-----------------------------------------';


		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_LOC_A101';
		truncate table bronze.erp_LOC_A101;
		print '>> Inserting Data Into: bronze.erp_LOC_A101';
		bulk insert bronze.erp_LOC_A101
		from 'C:\Users\Lenovo\Desktop\data portfolio\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with( 
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time= getdate();
		print '>> Load duration; ' + cast (datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print'-----------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_LOC_A101';
		truncate table bronze.erp_PX_CAT_G1V2;
		print '>> Inserting Data Into:bronze.erp_PX_CAT_G1V2';
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\Users\Lenovo\Desktop\data portfolio\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with( 
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time= getdate();
		print '>> Load duration; ' + cast (datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print'-----------------------------------------';
		set @batch_end_time = getdate();
		print '++++++++++++++++++++++++++++++++++++'
		Print 'Loading bronze layer is completed';
		print ' Total load duration; ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '++++++++++++++++++++++++++++++++++++'
	End try
	begin catch
		Print'=============================================================';
		print' error occured during loading bronze layer'
		print' error message' + error_message();
		print 'error message' + Cast (error_number() as nvarchar);
		print 'error message' + Cast (error_state() as nvarchar);
		Print'=============================================================';		
	end catch
end 
