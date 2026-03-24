/* 
============================================
Create Database and Schemas
============================================
Script Purpose:
	This script create a new Database named 'Datawarehouse' after checking if it already exist.
	If the Database Exists, it is dropped and recreated. Also the scripts created three schemas in the 
	Database: 'bronze', 'silver', 'gold'.

NOTE THIS: Running this script will drop the entire 'Datawarehouse' Database if it Exist.
		 All Data in the Database will be permanently deleted, proceed with caution
		 
*/

USE master;
GO


-- Drop and recreate the 'Datawarehouse' database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse
END;
GO

--Create the 'Data warehouse' Database

CREATE DATABASE Datawarehouse;
GO


USE Datawarehouse;
GO

-- Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


