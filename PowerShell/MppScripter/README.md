#MppScripter Module
The MppScripter module was created to fill a gap in the ability to create database object scripts in mass against
Azure SQL Data Warehouse or an Analytics Platform System (APS - aka Parallel Data Warehouse (PDW)) database.

##Supported Features - v.1.0.0
All objects will include the database name in the script; this is currently not optional

###Tables, Secondary Indexes, & Statistics
Table scripts include:
*DISTRIBUTION
*Storage Type: CLUSTERED INDEX, CLUSTERED COLUMNSTORE INDEX, HEAP
*Partition Information
*Column collation when appropriate
*Column nullability
*Secondary Indexes
*User Defined Statistics

###Stored Procedures, Views, & User Defined Functions (UDFs)
Each of these programmability objects are scripted using the sys.sql_modules DMV.  It currently supports up to 80K
characters, which is 800 lines of 100 characters.  This should support scripts that exceed the 1000 lines, but may
truncate extremely large scripts.  Please validate script of any large stored procedures in your environment.

##vNext (proposed)
*Add support for DEFAULT constraints in table script
*Get-MppLoginScript: returns scripts for instance-level logins, including server role membership and login permissions
*Get-MppDatabaseUserScript: returns scripts for database-level users, including database role membership and user permissions.
*Get-MppDatabaseRoleScript: returns user-defined database roles and respective permissions.  Membership to be handled by Get-MppUserScript
*Ability to modify

###Contributing
We look forward to making this scrpiting tool better for everyone!  Please contribute where you can - code, documentation, and more samples are welcome!