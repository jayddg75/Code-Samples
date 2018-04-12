/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 11/21/2017

Proc Info: This proc generates a sql script success file for the main powershell script to know that the sql ran successfully

		
		
Input Parameters:


	
Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xx.xx.AM_SQLFinish (text, text, text)
Language NZPLSQL Returns  boolean
as
begin_proc


	declare
	
		FileName alias for $1;
		FilePath alias for $2;
		LogPath alias for $3;
		
		FileExecString varchar(2000);

	begin
	
		FileExecString := '
			create external table ''' || FilePath ||  FileName ||  '''
			using (
				RemoteSource ODBC
				LogDir ''' || LogPath || '''
			)
			as
			
			select ''The sql script was successfully ran''';


		execute immediate FileExecString;
		
		Return True;

	end;


end_proc;