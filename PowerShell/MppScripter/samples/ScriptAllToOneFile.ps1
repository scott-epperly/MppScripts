Import-Module ..\MppScripter.psd1 -Force;


#--=============================--
#-- Create DW Connection object --
#--=============================--
#-- Active Directory Integrated authentication for Azure SQL DW --
#$DBConnection = Get-MppConnection -ServerInstance "<logical_server_name>.database.windows.net" -DatabaseName "<database_name>" -AzureAuthenticationMethod "Active Directory Integrated";

#-- Active Directory Integrated authentication for Azure SQL DW: untested --
#$cred = Get-Credential;
#$DBConnection = Get-MppConnection -ServerInstance "<logical_server_name>.database.windows.net" -DatabaseName "<database_name>" -Credential $cred -AzureAuthenticationMethod "Active Directory Password";

#-- SQL Authentication for Azure SQL DW --
#$cred = Get-Credential;
#$DBConnection = Get-MppConnection -ServerInstance "<logical_server_name>.database.windows.net" -DatabaseName "<database_name>" -Credential $cred -AzureAuthenticationMethod "Sql Password";

#-- Windows Authentication for APS: untested --
#$DBConnection = Get-MppConnection -ServerInstance "<aps_ip_address>,17001" -DatabaseName "<database_name>";

#-- SQL Authention for APS --
$cred = Get-Credential;
$DBConnection = Get-MppConnection -ServerInstance "<aps_ip_address>,17001" -DatabaseName "<database_name>" -Credential $cred;


#--============================--
#-- Script all objects to file --
#--============================--
$path = "C:\temp\scripts\MyTestDatabase.sql";
if (Test-Path $path) {
    Remove-Item $path -Force -Confirm:$false -ErrorAction:Continue;
}

# Script out Schemas First
$s = Get-MppSchemaScript -MppConnection $DBConnection
$s | ForEach-Object{
    $_.Script | Out-File $path -Append -Force -Confirm:$false;
}

# Script out everything else (this could be filtered by object Type on the ForEach-Object if you want to force the object order)
$x = Get-MppObjectScript -MppConnection $DBConnection
$x | ForEach-Object{
    $_.Script | Out-File $path -Append -Force -Confirm:$false;
}


#--=====================--
#-- Clean up connection --
#--=====================--
$DBConnection.Close();
