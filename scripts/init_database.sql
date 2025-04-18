/*

==========================================================================
CREATE DB and SCHEMAS
==========================================================================

Script:
The script will check if the db name already exists. 
Then it will create DataWaredouse DB additionally 
it will drop the table if the name exists. It will be
dropped and recreated. Then the bronze, silver, and gold
Schemas will be created.

!!!!!!!!!!!!WARNING!!!!!!!!!

DO NOT RUN IF YOU PLAN ON KEEPING THE DATA ALREADY EXISTING!!

!!!!!!!!!!!!WARNING!!!!!!!!!

*/

USE master;
GO

-- Drop and recreate DataWarehouse DB
IF EXISTS (SELECT 1 FROM sys.databases 
  WHERE name = 'DataWarehouse')

BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas

CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
