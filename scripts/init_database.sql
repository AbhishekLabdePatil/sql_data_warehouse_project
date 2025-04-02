use master;

create database datawarehouse;      #creating database
use datawarehouse;                  #using created database
go
create schema bronze;              #creating bronze schema
go
create schema silver;              #creating sliver schema
go
create schema gold;                #creating gold schema
