/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 11/7/2017

Proc Info: This proc will make a backup file of the log table and maintain a six month history on the log table ...

		
		
Input Parameters:


	
Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xx.xx.AM_LogBackup (text, text, text)
Language NZPLSQL Returns  boolean
as
begin_proc


	declare
	
	FileName alias for $1;
	OprPath alias for $2;
	LogPath alias for $3;

	ExecString varchar(4000);

	begin
	
	
	
	ExecString := '	
		create external table ''' || OprPath || FileName || '''
		using (
			RemoteSource ODBC
			IncludeHeader
			Delim ''|''
			LogDir ''' || LogPath || '''
		)
		as
		
		select *
		from AM_Log';


		execute immediate ExecString;
		
		Return True;

	end;


end_proc;