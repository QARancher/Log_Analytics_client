# Log_Analytics_client
Use this client in order to push csv records to Azure Log Analytics Workspace. 
Usage:
python --workspace_id='xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' 
--primary_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' 
--blob=<file1 file2 or url1 url2>  
--table_name=<table_name> 
--use_types=True

--workspace_id: Mandatory - The unique identifier for the Log Analytics workspace.

--primary_key: Mandatory - Primary key for authentication. Can be retrived from Log Analytics workspace then Advanced 
Settings and then Connected Sources.

--blob: Mandatory - CSV file or url to a blob containing events
May be used as list with space delimiter,i.e 'url1 url2 url3 .. urln'

--table_name: Mandatory - The target table name to publish the data to in Log Analytics.

--use_types: Optional - when used, the special types as number and timestamp preserve their type 
while inserted to the Log Analytics workspace.
