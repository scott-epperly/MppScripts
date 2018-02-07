# MppScripter Module
The MppScripter module was created to fill a gap in the ability to create database object scripts in mass againstAzure SQL Data Warehouse or an Analytics Platform System (APS - aka Parallel Data Warehouse (PDW)) database.

## Release Log
### v1.0.1
* Improved performance when scripting a large number of objects
* Corrected a number of initial release bugs
### v1.0.0
* Initial Release
## Supported Features
All objects will include the database name in the script; this is currently not optional

### Tables, Secondary Indexes, & Statistics
Table scripts include:
* DISTRIBUTION
* Storage Type: CLUSTERED INDEX, CLUSTERED COLUMNSTORE INDEX, HEAP
* Partition Information
* Column collation when appropriate
* Column nullability
* Secondary Indexes
* User Defined Statistics

### Stored Procedures, Views, & User Defined Functions (UDFs)
Each of these programmability objects are scripted using the sys.sql_modules DMV.  It currently supports up to 80K
characters, which is 800 lines of 100 characters.  This should support scripts that exceed the 1000 lines, but may truncate extremely large scripts.  Please validate script of any large stored procedures in your environment.

## Known Issues
* Scripter takes a long time when a large number of objects are scripted (beta fix in place)

## vNext (proposed)
* Add support for DEFAULT constraints in table script
* Get-MppSchemaScript: returns CREATE SCHEMA scripts
* Get-MppDatabaseScript: returns a CREATE DATABASE script
* Get-MppLoginScript: returns scripts for instance-level logins, including server role membership and login permissions
* Get-MppDatabaseUserScript: returns scripts for database-level users, including database role membership and user permissions.
* Get-MppDatabaseRoleScript: returns user-defined database roles and respective permissions.  Membership to be handled by Get-MppUserScript
* Scripting options:
    * Include/Exclude database name
    * Include/Exclude schema name
    * Include/Exclude collation
    * Include/Exclude secondary indexes
    * Include/Exclude user defined statistics
    * Script USE <database>
    * Check for object existence


## Contributing
We look forward to making this scrpiting tool better for everyone!  Please contribute where you can - code, documentation, and more samples are welcome!