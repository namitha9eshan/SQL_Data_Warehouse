/*
===========================================================================================
Create Databases & Schemas
===========================================================================================
Script Purpose : 
  This script creates a new database called 'DataWarehouse' after checking if it is already exists.
  If database exist the database will drop and recreated. Additionally the script sets up 3 scehmas withing the database : 'bronze', 'silver', 'gold'

Warning : 
  Running this script will drop the entire 'DataWarehouse' database if its exists.
  All data in the database will be permanatly deleted. Proceed with caution and ensure you have proper backup before running the script.

*/

use master;

-- drop and recreate the 'Datawarehouse' database
if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin 
  alter database DataWarehouse set SINGLE_USER with rollback immediate;
  drop database Datawarehouse;
end; 
go 

-- Create	Database 'Datawarehouse'

create Database DataWarehouse;

use DataWarehouse;

-- Creating the schemas 
create schema bronze;
go
create schema silver;
go
create schema gold;
go 
