##############################
#.SYNOPSIS
#Gets scripts for database object in Azure SQL Data Warehouse or an Analytics Platform System appliance.
#
#.DESCRIPTION
#The Get-MppObjectScript cmdlet will return object(s) with a Script property that contains the object script.
#
#Supported Object Types:
#Tables
#    Secondary Indexes
#    Statistics
#    Default Constraints
#Views
#Stored Procedures
#User Defined Functions
#
#
#.PARAMETER MppConnection
#.Net SqlConnection object that is opened against a DW database.  This is commonly
#
#.PARAMETER ObjectName
#Object name(s) for which scripts will be created and returned.  If not specified, all objects in the database will be returned.
#
#.EXAMPLE
#$conn = Get-MppConnection -ServerInstance "myserver.database.windows.net" -Databasename "MyDatabase" -Credential (Get-Credential);
#Get-MppObjectScript -MppConnection $conn
#$conn.Close()
#
#.EXAMPLE
#$conn = Get-MppConnection -ServerInstance "myserver.database.windows.net" -Databasename "MyDatabase" -Credential (Get-Credential);
#Get-MppObjectScript -MppConnection $conn -ObjectName "dbo.MyObjectName"
#$conn.Close()
##############################
function Get-MppObjectScript
{
    [CmdletBinding(DefaultParametersetName="Command")]
    Param (
        [Parameter(Mandatory=$true)]
        [object]$MppConnection
        ,[string[]]$ObjectName
    )

    begin{
        $DatabaseName = $MppConnection.Database;
    }

    Process {
        
        $qryScriptInfo = @"
    if (object_id('tempdb..#ObjectList') is not null)
        drop table #ObjectList;
    
    create table #ObjectList
    with(distribution=round_robin, clustered index(object_id))
    as
    <ObjectSelect>
    
    -- Object Type Info
    select
        o.object_id
        ,schema_name(o.schema_id) as schema_name
        ,o.name
        ,o.type
    from #ObjectList ol
        join sys.objects o
            on ol.object_id = o.object_id;
            
    -- Table Base
    select
        t.object_id
        ,schema_name(t.schema_id) as schema_name
        ,t.name as table_name
        ,i.name as index_name
        ,i.type_desc as index_type
        ,tdp.distribution_policy_desc
        ,(
            select c.name
            from sys.pdw_column_distribution_properties cdp
                join sys.columns c
                    on cdp.object_id = c.object_id
                    and cdp.column_id = c.column_id
            where cdp.object_id = t.object_id
                and cdp.distribution_ordinal = 1
        ) as hash_distribution_column_name
    from #ObjectList ol
        join sys.tables t
            on ol.object_id = t.object_id
        join sys.indexes i
            on t.object_id = i.object_id
            and i.index_id <= 1
        join sys.pdw_table_distribution_properties tdp
            on t.object_id = tdp.object_id
    where
        t.type = 'U'
        and t.is_ms_shipped = 0;
            
    -- Clustered Index Columns
    select
        ic.object_id
        ,ic.index_id
        ,ic.index_column_id
        ,ic.column_id
        ,ic.key_ordinal
        ,ic.is_descending_key
        ,c.name as column_name
    from #ObjectList ol
        join sys.tables t
            on ol.object_id = t.object_id
        join sys.indexes i
            on t.object_id = i.object_id
        join sys.index_columns ic
            on i.object_id = ic.object_id
            and i.index_id = ic.index_id
        join sys.columns c
            on ic.object_id = c.object_id
            and ic.column_id = c.column_id
    where i.index_id <= 1
        and i.type = 1
    order by ic.object_id, ic.index_column_id;
            
    -- Column Info
    select
        c.object_id
        ,c.column_id
        ,c.name as column_name
        ,c.max_length
        ,c.precision
        ,c.scale
        ,c.collation_name
        ,c.is_nullable
        ,c.is_computed
        ,typ.user_type_id
        ,typ.name as data_type_name
        ,case
            when typ.precision >= 38 and typ.scale > 4 then '(' + cast(c.precision as varchar(16)) + ', ' + cast(c.scale as varchar(16)) + ')'
            when typ.scale > 4 then '(' + cast(c.scale as varchar(16)) + ')'
            when typ.precision > 38 then '(' + cast(c.precision as varchar(16)) + ')'
            when typ.max_length = 8000 then '(' + 
                case
                    when c.max_length = -1 then 'max'
                    when typ.name like 'n%' then cast((c.max_length / 2) as varchar(16))
                    else cast(c.max_length as varchar(16))
                end
                + ')'
        end as size
        ,df.definition as default_definition
    from #ObjectList ol
        join sys.tables t
            on ol.object_id = t.object_id
        join sys.columns c
            on t.object_id = c.object_id
        join sys.types typ
            on c.user_type_id = typ.user_type_id
        left join sys.default_constraints df
			on t.object_id = df.parent_object_id
			and c.column_id = df.parent_column_id
    order by t.object_id, c.column_id;
            
            
    -- Partition definition
    if (object_id('tempdb..#PartInfo') is not null)
        drop table #PartInfo;
    
    create table #PartInfo
    with (distribution=round_robin, clustered index(object_id, boundary_value))
    as
    with param_data_type as
    (
        select
            pp.function_id
            ,typ.name as data_type_name
            ,cast(case
                when typ.collation_name is not null then 1
                when typ.name like '%date%' then 1
                when typ.name = 'uniqueidentifier' then 1
                else 0
            end as bit) as use_quotes_on_values_flag
        from sys.partition_parameters pp
            join sys.types typ
                on pp.user_type_id = typ.user_type_id
    )
    select
        t.object_id
        ,c.name as partition_column_name
        ,pf.boundary_value_on_right
        ,prv.boundary_id
        ,case
            when pdt.use_quotes_on_values_flag = 1 then '''' + cast(
                case pdt.data_type_name
                    when 'date' then convert(char(10), prv.value, 120)
                    when 'smalldatetime' then convert(varchar, prv.value, 120)
                    when 'datetime' then convert(varchar, prv.value, 121)
                    when 'datetime2' then convert(varchar, prv.value, 121)
                    else prv.value
                end	
                as varchar(32)) + ''''
            else cast(prv.value as varchar(32))
        end as boundary_value
    from sys.tables t
        join sys.indexes i
            on t.object_id = i.object_id
            and i.index_id <= 1
        join sys.index_columns ic
            on i.object_id = ic.object_id
            and i.index_id = ic.index_id
            and ic.partition_ordinal = 1
        join sys.columns c
            on ic.object_id = c.object_id
            and ic.column_id = c.column_id
        join sys.partition_schemes ps
            on i.data_space_id = ps.data_space_id
        join sys.partition_functions pf
            on ps.function_id = pf.function_id
        join param_data_type pdt
            on pf.function_id = pdt.function_id
        join sys.partition_range_values prv
            on pf.function_id = prv.function_id;
    
    select
        pinfo.*
    from #PartInfo pinfo
        join #ObjectList ol
            on pinfo.object_id = ol.object_id
    order by pinfo.object_id, pinfo.boundary_value;
    
    -- Statistics Info
	select
        s.object_id
        ,s.stats_id
        ,s.name as stats_name
        ,s.has_filter
		,s.filter_definition
		,sc.stats_column_id
		,c.name as column_name
	from #ObjectList ol
		join sys.stats s
			on ol.object_id = s.object_id
		join sys.stats_columns sc
			on s.object_id = sc.object_id
			and s.stats_id = sc.stats_id
		join sys.columns c
			on sc.object_id = c.object_id
			and sc.column_id = c.column_id
    where s.user_created = 1
        and s.stats_id > 1
	order by s.object_id, s.stats_id, sc.stats_column_id
	
	-- Secondary Index Info
	select
        i.object_id
        ,i.index_id
		,i.name as index_name
        ,i.type_desc
        ,ic.index_column_id
		,ic.is_descending_key
		,c.name as column_name
	from #ObjectList ol
		join sys.indexes i
			on ol.object_id = i.object_id
		join sys.index_columns ic
			on i.object_id = ic.object_id
			and i.index_id = ic.index_id
		join sys.columns c
			on ic.object_id = c.object_id
			and ic.column_id = c.column_id
	where i.index_id > 1
    order by i.object_id, index_id, ic.index_column_id
    
    -- Procedure/View scripted
    select
            o.object_id
        ,substring(sm.definition, 1, 4000) as def1
        ,case when len(sm.definition) > 4000 then substring(sm.definition, 4001, 4000) else '' end as def2 
        ,case when len(sm.definition) > 8000 then substring(sm.definition, 8001, 4000) else '' end as def3 
        ,case when len(sm.definition) > 12000 then substring(sm.definition, 12001, 4000) else '' end as def4 
        ,case when len(sm.definition) > 16000 then substring(sm.definition, 16001, 4000) else '' end as def5 
        ,case when len(sm.definition) > 20000 then substring(sm.definition, 20001, 4000) else '' end as def6 
        ,case when len(sm.definition) > 24000 then substring(sm.definition, 24001, 4000) else '' end as def7 
        ,case when len(sm.definition) > 28000 then substring(sm.definition, 28001, 4000) else '' end as def8 
        ,case when len(sm.definition) > 32000 then substring(sm.definition, 32001, 4000) else '' end as def9 
        ,case when len(sm.definition) > 36000 then substring(sm.definition, 36001, 4000) else '' end as def10
        ,case when len(sm.definition) > 40000 then substring(sm.definition, 40001, 4000) else '' end as def11 
        ,case when len(sm.definition) > 44000 then substring(sm.definition, 44001, 4000) else '' end as def12 
        ,case when len(sm.definition) > 48000 then substring(sm.definition, 48001, 4000) else '' end as def13 
        ,case when len(sm.definition) > 52000 then substring(sm.definition, 52001, 4000) else '' end as def14 
        ,case when len(sm.definition) > 56000 then substring(sm.definition, 56001, 4000) else '' end as def15 
        ,case when len(sm.definition) > 60000 then substring(sm.definition, 60001, 4000) else '' end as def16 
        ,case when len(sm.definition) > 64000 then substring(sm.definition, 64001, 4000) else '' end as def17 
        ,case when len(sm.definition) > 68000 then substring(sm.definition, 68001, 4000) else '' end as def18 
        ,case when len(sm.definition) > 72000 then substring(sm.definition, 72001, 4000) else '' end as def19
        ,case when len(sm.definition) > 76000 then substring(sm.definition, 76001, 4000) else '' end as def20
    from
        #ObjectList ol
        join sys.objects o
            on ol.object_id = o.object_id
        join sys.sql_modules sm
            on o.object_id = sm.object_id
    where o.type in ('P', 'V', 'FN');
"@;
        # Get list of all objects if none were specified
        if(!$ObjectName) {
            $strObjectSelect = "select object_id from sys.objects;";
        }
        else {
            $ObjectSelect = $ObjectName | ForEach-Object{"`r`nunion all select object_id('$_') as object_id where object_id('$_') is not null"}
            $strObjectSelect = ([string]::Concat($ObjectSelect)).substring(12);
        }

        # Retrieve metadata from database
        $qryScriptInfoMod = $qryScriptInfo -replace "<ObjectSelect>", $strObjectSelect;
        $params=@{
            "DBConnection"=$MppConnection;
            "Query"=$qryScriptInfoMod;
        }
        Write-Progress -Activity "Retrieving metadata from database . . .";
        $ds = runsql @params;

        # Script the objects
        $cntr = 0
        $ds.Tables[0] | ForEach-Object{
            $cntr++;
            Write-Progress -Activity "Scripting Objects" -Status "$($_.name)" -PercentComplete ($cntr/$ds.Tables[0].Rows.Count*100)
            $object_id = $_.object_id;

            # Filter tables for current object
            $tblTableBase = $ds.Tables[1].select("object_id='$object_id'");
            $tblClusteredIndexCols = $ds.Tables[2].select("object_id='$object_id'");
            $tblColumnInfo = $ds.Tables[3].select("object_id='$object_id'");
            $tblPartitionInfo = $ds.Tables[4].select("object_id='$object_id'");
            $tblStatsInfo = $ds.Tables[5].select("object_id='$object_id'");
            $tblSecondaryIndexInfo = $ds.Tables[6].select("object_id='$object_id'");
            $tblProgrammability = $ds.Tables[7].select("object_id='$object_id'");

            # Create Script output - User Tables
            if ($_.type.Trim() -eq "U") {
                # Column List
                $column_list = $tblColumnInfo | ForEach-Object{"`r`n`t,[$($_.column_name)] $($_.data_type_name)$($_.size)$(if($_.collation_name.length -gt 1) {" COLLATE $($_.collation_name)"})$(if ($_.is_nullable) {" NOT"}) NULL$(if ($_.default_definition) {" DEFAULT " + $_.default_definition})"};
                $str_column_list = ([string]::Concat($column_list)).substring(4);


                # Distribution Type
                if($tblTableBase.distribution_policy_desc -eq "HASH") {
                    $distribution_type = "HASH($($tblTableBase.hash_distribution_column_name))";
                }
                else {
                    $distribution_type = $tblTableBase.distribution_policy_desc;
                };


                # Storage Type
                if($tblTableBase.index_type -eq "CLUSTERED COLUMNSTORE") {
                    $storage_type = "$($tblTableBase.index_type) INDEX"
                }
                elseif($tblTableBase.index_type -eq "CLUSTERED") {
                    $clust_column_list = $tblClusteredIndexCols | ForEach-Object{", [$($_.column_name)] $(if ($_.is_descending_key){"DESC"} else {"ASC"})"};
                    $str_clust_column_list = ([string]::Concat($clust_column_list)).substring(1);
                    $storage_type = "$($tblTableBase.index_type) INDEX ($str_clust_column_list )"
                }
                else {
                    $storage_type = $tblTableBase.index_type
                }


                # Partition Info
                $partition_info = "";
                $part_range_vals = $tblPartitionInfo | ForEach-Object{"`r`n`t,$($_.boundary_value)"};
                if ($part_range_vals.Length -gt 0) {
                    $str_part_range_vals = ([string]::Concat($part_range_vals)).substring(4);
                    $partition_info = ", PARTITION ($($tblPartitionInfo[0].partition_column_name) RANGE $(if ($tblPartitionInfo[0].boundary_value_on_right) {"RIGHT"} else {"LEFT"} ) FOR VALUES($str_part_range_vals))"
                }


                # Secondary Index Info
                $index_id = 0;
                $index_stub = "";
                [System.Collections.ArrayList]$index_cols = @();
                $index_info = "";
                $lst_index_info = $tblSecondaryIndexInfo | ForEach-Object{
                    if($index_id -ne $_.index_id) {
                        # Output CREATE INDEX string
                        if($index_stub -ne "") {
                            $str_index_cols = [string]::Concat($index_cols.ToArray()).substring(1);
                            $index_cols.Clear();
                            $index_out = $index_stub -replace "<index_cols>", $str_index_cols;
                            $index_out;
                        }
                        
                        $index_stub = "`r`nCREATE INDEX [$($_.index_name)] on [$($tblTableBase.schema_name)].[$($tblTableBase.table_name)] (<index_cols> );`r`nGO";
                        [void]$index_cols.Add(", $($_.column_name) $(if($_.is_descending_key) {"DESC"} else {"ASC"})");
                    }
                    else {
                        [void]$index_cols.Add(", $($_.column_name) $(if($_.is_descending_key) {"DESC"} else {"ASC"})");
                    }
                    $index_id = $_.index_id;
                }
                # Gather info from the last record
                if ($index_cols.Count -gt 0) {
                    $str_index_cols = [string]::Concat($index_cols.ToArray()).substring(1);
                    $index_cols.Clear();
                    $lst_index_info += $index_stub -replace "<index_cols>", $str_index_cols;
                }
                # Write out generated statement if there is anything to write out
                if ($lst_index_info.count -gt 0) {
                    $index_info = ([string]::Concat($lst_index_info));
                }
                

                # Stats Info
                $stats_id = 0;
                $stats_stub = "";
                [System.Collections.ArrayList]$stats_cols = @();
                $stats_info = "";
                $lst_stats_info = $tblStatsInfo | ForEach-Object{
                    if($stats_id -ne $_.stats_id) {
                        # Output CREATE STATISTICS string
                        if($stats_stub -ne "") {
                            $str_stats_cols = [string]::Concat($stats_cols.ToArray()).substring(1);
                            $stats_cols.Clear();
                            $stats_out = $stats_stub -replace "<stats_cols>", $str_stats_cols;
                            $stats_out;
                        }
                        
                        $stats_stub = "`r`nCREATE STATISTICS [$($_.stats_name)] on [$($tblTableBase.schema_name)].[$($tblTableBase.table_name)] (<stats_cols> )$(if ($_.has_filter) {" WHERE $($_.filter_definition)"});`r`nGO";
                        [void]$stats_cols.Add(", $($_.column_name)");
                    }
                    else {
                        [void]$stats_cols.Add(", $($_.column_name)");
                    }
                    $stats_id = $_.stats_id;
                }
                if ($stats_cols.Length -gt 0) {
                    $str_stats_cols = [string]::Concat($stats_cols.ToArray()).substring(1);
                    $stats_cols.Clear();
                    $lst_stats_info += $stats_stub -replace "<stats_cols>", $str_stats_cols;
                }
                if ($lst_stats_info.count -gt 0) {
                    $stats_info = ([string]::Concat($lst_stats_info));
                }


                # Compile the script
                $script = "CREATE TABLE [$($tblTableBase.schema_name)].[$($tblTableBase.table_name)]`r`n(`r`n`t$str_column_list`r`n)`r`nWITH ( DISTRIBUTION=$distribution_type, $storage_type $partition_info);`r`nGO`r`n$index_info`r`n$stats_info`r`n";
                    
            }
            elseif ($_.type.Trim() -in "P","V","FN") {
                #$script = $tblProgrammability.def1;
                $script = $tblProgrammability.def1 + $tblProgrammability.def2 + $tblProgrammability.def3 + $tblProgrammability.def4 + $tblProgrammability.def5 + 
                    $tblProgrammability.def6 + $tblProgrammability.def7 + $tblProgrammability.def8 + $tblProgrammability.def9 + $tblProgrammability.def10 +
                    $tblProgrammability.def11 + $tblProgrammability.def12 + $tblProgrammability.def13 + $tblProgrammability.def14 + $tblProgrammability.def15 +
                    $tblProgrammability.def16 + $tblProgrammability.def17 + $tblProgrammability.def18 + $tblProgrammability.def19 + $tblProgrammability.def20 +
                    "`r`nGO";
            }
            else {
                $script = "Database object not found; Unable to generate script."
            }

            $objProp = @{
                "ObjectId"=$_.object_id;
                "SchemaName"=$_.schema_name;
                "ObjectName"=$_.name;
                "ObjectType"=$_.type;
                "Script"=$script;
            }
            New-Object -TypeName psobject -Property $objProp;
        }
    }
}

##############################
#.SYNOPSIS
#Gets scripts for non-default database schemas in Azure SQL Data Warehouse or an Analytics Platform System appliance.
#
#.DESCRIPTION
#The Get-MppSchemaScript cmdlet will return object(s) with a Script property that contains the schema script.
#
#
#.PARAMETER MppConnection
#.Net SqlConnection object that is opened against a DW database.  This is commonly
#
#.PARAMETER ObjectName
#Object name(s) for which scripts will be created and returned.  If not specified, all objects in the database will be returned.
#
#.EXAMPLE
#$conn = Get-MppConnection -ServerInstance "myserver.database.windows.net" -Databasename "MyDatabase" -Credential (Get-Credential);
#Get-MppSchemaScript -MppConnection $conn
#$conn.Close()
#
#.EXAMPLE
#$conn = Get-MppConnection -ServerInstance "myserver.database.windows.net" -Databasename "MyDatabase" -Credential (Get-Credential);
#Get-MppSchemaScript -MppConnection $conn -SchemaName "stg"
#$conn.Close()
##############################
function Get-MppSchemaScript
{
    [CmdletBinding(DefaultParametersetName="Command")]
    Param (
        [Parameter(Mandatory=$true)]
        [object]$MppConnection
        ,[string[]]$SchemaName
    )

    begin{
        $DatabaseName = $MppConnection.Database;
    }

    Process {
        

        # Get list of all objects if none were specified
        if(!$SchemaName) {
            $qryScriptInfo = "select schema_id, name as schema_name, 'Schema' as type from sys.schemas where name not in ('dbo', 'INFORMATION_SCHEMA', 'sys')";
        }
        else {
            $ObjectSelect = $SchemaName | ForEach-Object{",'$_'"}
            $qryScriptInfo = "select schema_id, name as schema_name, 'Schema' as type from sys.schemas where name in (" + ([string]::Concat($ObjectSelect)).substring(1) + ");";
        }

        # Retrieve metadata from database
        $params=@{
            "DBConnection"=$MppConnection;
            "Query"=$qryScriptInfo;
        }
        Write-Progress -Activity "Retrieving metadata from database . . .";
        $ds = runsql @params;

        # Script the objects
        $cntr = 0
        $ds.Tables[0] | ForEach-Object{
            $cntr++;
            Write-Progress -Activity "Scripting Schemas" -Status "$($_.schema_name)" -PercentComplete ($cntr/$ds.Tables[0].Rows.Count*100)

            $script = "CREATE SCHEMA [$($_.schema_name)];`r`nGO"
            
            $objProp = @{
                "SchemaId"=$_.schema_id;
                "SchemaName"=$_.schema_name;
                "ObjectType"=$_.type;
                "Script"=$script;
            }
            New-Object -TypeName psobject -Property $objProp;
        }
    }
}

#--==================--
#-- Helper Functions --
#--==================--
function Get-MppConnection {
<#
.SYNOPSIS
Gets SqlConnection object for database in Azure SQL Data Warehouse or an Analytics Platform System appliance.

.DESCRIPTION
The Get-MppConnection cmdlet returns the necessary connection object required for Get-MppScript.  Use this
cmdlet to prepare the object and open the connection.  Alternatively, you can build the .NEt SqlConnection
object yourself to pass to the Get-MppScript object.

.PARAMETER ServerInstance
Azure SQL Data Warehouse logical server name or server name/IP of Analytics Platform System control node and 17001 port

.PARAMETER Databasename
Name of database for which to connect

.EXAMPLE
$conn = Get-MppConnection -ServerInstance "myserver.database.windows.net" -Databasename "MyDatabase" -Credential (Get-Credential);
Get-MppScript -MppConnection $conn
$conn.Close()

.EXAMPLE
$conn = Get-MppConnection -ServerInstance "myserver.database.windows.net" -Databasename "MyDatabase" -Credential (Get-Credential);
Get-MppScript -MppConnection $conn -ObjectName "dbo.MyObjectName"
$conn.Close()
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [string]$ServerInstance,

        [string]$DatabaseName = "master",
        
        [PSCredential] [System.Management.Automation.Credential()]$Credential,
        
        [ValidateSet("Active Directory Integrated", "Active Directory Password", "Sql Password")]
        [string]$AzureAuthenticationMethod = "Sql Password"
    )
    begin {
        try {
            
                if ($Credential) {
                    if ($ServerInstance -match "database.windows.net") {
                        $DBConnection = New-Object System.Data.SqlClient.SqlConnection("Server=$($ServerInstance); Database=$($DatabaseName);User ID=$($Credential.Username);Password=$($Credential.GetNetworkCredential().Password);Authentication=$($AzureAuthenticationMethod);")
                    }
                    else {
                        $DBConnection = New-Object System.Data.SqlClient.SqlConnection("Server=$($ServerInstance); Database=$($DatabaseName);User ID=$($Credential.Username);Password=$($Credential.GetNetworkCredential().Password);")
                    }
                }
                else {
                    if ($ServerInstance -match "database.windows.net") {
                        $DBConnection = New-Object System.Data.SqlClient.SqlConnection("Server=$($ServerInstance); Database=$($DatabaseName);Authentication=$($AzureAuthenticationMethod);")
                    }
                    else {
                        $DBConnection = New-Object System.Data.SqlClient.SqlConnection("Server=$($ServerInstance); Database=$($DatabaseName);Trusted_Connection=yes;")
                    }
                }
                $DBConnection.Open()
        }    
        catch {
            Write-Error -Message "Error connecting to SQL."
        }
    }
    
    process {
        return $DBConnection;
    }

    end {}
}

function runSql {
    [CmdletBinding()]
    param (
        [ValidateScript({$_.State -eq "Open"})]
        [object]$DBConnection,
        [String]$Query,
        $Variable,        
        [Switch]$NonQuery,
        [Switch]$AutoClose
    )
    begin {}

    process {
        $sql = $Query               
        
        if ($NonQuery) {
            foreach ($statement in [regex]::Split($sql, "GO")) {
                $DBCommand = New-Object System.Data.SqlClient.SqlCommand($statement, $DBConnection)
                $DBCommand.ExecuteNonQuery()
            }
        }
        else {
            $DBCommand = New-Object System.Data.SqlClient.SqlCommand($sql, $DBConnection)
            $DBCommand.CommandTimeout = 300; #Setting timeout to 5 minutes.
            if($Variable.count -gt 0) {
                $Variable.Keys | ForEach-Object {
                    $DBCommand.Parameters.AddWithValue($_, [string]$Variable.Item($_)) | Out-Null;
                }
            }

            $DBAdapter = New-Object -TypeName System.Data.SqlClient.SqlDataAdapter
            $DBDataSet = New-Object -TypeName System.Data.DataSet
            $DBAdapter.SelectCommand = $DBCommand

            $DBAdapter.Fill($DBDataSet) | Out-Null
            $DBDataSet
        }
    }
    end {
        if ($AutoClose) {
            $DBConnection.Close();
            $DBConnection.Dispose();
        }
    }
}
