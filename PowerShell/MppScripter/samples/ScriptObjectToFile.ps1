Import-Module ..\MppScripter.psd1 -Force;

#--=============================--
#-- Create DW Connection object --
#--=============================--
#-- Active Directory Integrated authentication for Azure SQL DW --
$DBConnection = Get-MppConnection -ServerInstance "<logical_server_name>.database.windows.net" -DatabaseName "<database_name>" -AzureAuthenticationMethod "Active Directory Integrated";

#-- Active Directory Integrated authentication for Azure SQL DW: untested --
#$cred = Get-Credential;
#$DBConnection = Get-MppConnection -ServerInstance "<logical_server_name>.database.windows.net" -DatabaseName "<database_name>" -Credential $cred -AzureAuthenticationMethod "Active Directory Password";

#-- SQL Authentication for Azure SQL DW --
#$cred = Get-Credential;
#$DBConnection = Get-MppConnection -ServerInstance "<logical_server_name>.database.windows.net" -DatabaseName "<database_name>" -Credential $cred -AzureAuthenticationMethod "Sql Password";

#-- Windows Authentication for APS: untested --
#$DBConnection = Get-MppConnection -ServerInstance "<aps_ip_address>,17001" -DatabaseName "<database_name>";

#-- SQL Authention for APS --
#$cred = Get-Credential;
#$DBConnection = Get-MppConnection -ServerInstance "<aps_ip_address>,17001" -DatabaseName "<database_name>" -Credential $cred;


#--==================================--
#-- Script objects to separate files --
#--==================================--
$x = Get-MppObjectScript -MppConnection $DBConnection -ObjectName "dbo.DimProduct", "dbo.FactSales"
$x | ForEach-Object{
    $path = "C:\temp\scripts\$($_.SchemaName).$($_.ObjectName).sql";
    $_.Script | Out-File $path -Force -Confirm:$false;
}


#--=====================--
#-- Clean up connection --
#--=====================--
$DBConnection.Close();