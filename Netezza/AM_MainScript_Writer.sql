/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by:
Create Date:

Proc Info:

		
		
Input Parameters:


	
Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xx.xx.AM_MainScript_Writer (text, text, text, text, text, text, text)
Language NZPLSQL Returns boolean
as
begin_proc



declare

	SQLScriptName alias for $1;
	TempPath alias for $2;
	LogPath alias for $3;
	NZOprPath alias for $4;
	SchedRunFileName alias for $5;
	LogBackUpName alias for $6;
	NoMainSQLName alias for $7;
	
	
	i record;
	j record;
	
	ParmString varchar(4000);
	ParmStringDouble varchar(4000);
	ProcString varchar(2000);
	ExecString varchar(6000);
	ExecStringDouble varchar(6000);
	ProcHeader varchar(500);
	
	FileExecString varchar(3000);
	
	NextLogId int;
	NextBatchId int;
	
	SeqCheck int;
	
	RecCnt int;
	

begin



	-- get the batch id
	select max(Batch_Id)
	into NextBatchId
	from AM_Batch;



	-- check to see if there is anything to run
	select count(*)
	into RecCnt
	from AM_Batch
	where Batch_Status = 'A';
	
	
	-- if there are none insert a dummy record into the log,  write the no main sql file,  and exit
	If RecCnt = 0 then
	
		insert into AM_Log values (next value for AM_Log_Seq, NextBatchId, 'P', null, 'S', 'Nothing to Run', current_timestamp, current_timestamp, current_timestamp, current_user);

		FileExecString := '
			create external table ''' || TempPath ||  NoMainSQLName ||  '''
			using (
				RemoteSource ODBC
				LogDir ''' || LogPath || '''
			)
			as
			
			select ''No main sql to run''';
			
			
		execute immediate FileExecString;
		
		exit;
		
	end if;
	

	


	-- create the sequence ...
	
	select count(*)
	into SeqCheck
	from _v_Sequence
	where SeqName = 'AM_SQLSCRIPT_SEQ';
	
	if SeqCheck > 0 then
		drop sequence AM_SQLScript_Seq;
	end if;
	
	create sequence AM_SQLScript_Seq;

	
	-- create the table that holds the scipt ...
	drop table AM_SQL_Scripts if exists;

	create table AM_SQL_Scripts (
	Script_Line int,
	Script_Value varchar(3000),
	CreateDate datetime,
	CreateBy varchar(100)
	);
	


	-- insert the header for the SQL script file ...
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '------------------------------------------------------------------------------------------------', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '-- This sql scrip is for batch id: ' || NextBatchId ||',     created at : ' || current_timestamp, current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '------------------------------------------------------------------------------------------------', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '--', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '--', current_timestamp, current_user);
	


	
	for i in 

		select Proc_Id, Schedule_Id
		from AM_Batch
		where Batch_Status = 'A'
		order by Proc_Order

	loop

		raise notice '%', i.Proc_Id;

		----------------------------------------------------------------------------------------------------
		-- put the strings together to execute and update the log
	
		-- set the proc string minus the parameters ...
		select 'set catalog ' || Proc_Database || '; exec ' || Proc_Schema || '.' || Proc_Text
		into ProcString
		from AM_Proc
		where Proc_Id =i.Proc_Id;
		
		
		ParmString := '';
		ParmStringDouble := '';
		
		
		-- put the parameters into a string ...
		for j in 
		
			select case when Parm_Type = 'D' then dp.DynParm_Value else Parm_Value end as Parm_Value,
				Parm_String_Ind
			from AM_Proc pr
			join AM_Parameter p on pr.Proc_Id = p.Proc_Id
			left join AM_Dynamic_Parameter dp on p.Parm_Value = dp.DynParm_Code
			where pr.Proc_Id = i.Proc_Id
			order by Parm_Order
		
		loop
		
			ParmString := ParmString || ', ' || case when j.Parm_String_Ind is True then '''' || j.Parm_Value || '''' else j.Parm_Value end;
			ParmStringDouble := ParmStringDouble || ', ' || case when j.Parm_String_Ind is True then '''''' || j.Parm_Value || '''''' else j.Parm_Value end;
			
		end loop;
			
			
		-- get the proc header info to put into the script ...
		select '-- Report Name: ' || r.Report_Name || '  ; Proc Code: ' || pr.Proc_Code || '  ; Proc_Id : ' || pr.Proc_Id || '  ; Schedule Description : ' || s.Schedule_Desc
		into ProcHeader
		from AM_Proc pr
		join AM_Report r on pr.Report_Id = r.Report_Id
		join AM_Proc_Sched ps on pr.Proc_Id = ps.Proc_Id
		join AM_Schedule s on ps.Schedule_Id = s.Schedule_Id
		where pr.Proc_Id = i.Proc_Id
			and s.Schedule_Id	= i.Schedule_Id;
	
	
	
		ExecString := ProcString || ' ( ' || substr(ParmString, 3, length(ParmString)) || ' );';
		ExecStringDouble := ProcString || ' ( ' || substr(ParmStringDouble, 3, length(ParmStringDouble)) || ' );';
		
		raise notice '%',   ExecString;
		
		
			
		-- write out the sql script records ...
		
		-- proc header
		insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, ProcHeader, current_timestamp, current_user);
		
		-- call the proc to write out the initial log record ...
		insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, 'set Catalog xx; exec AM_Log_Writer (''P'', ''I'', ' || i.Proc_Id || ', ''' || ExecStringDouble || ''');', current_timestamp, current_user);
		insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, 'commit;', current_timestamp, current_user);
		
		-- insert the executable
		insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, ExecString, current_timestamp, current_user);
		insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, 'commit;', current_timestamp, current_user);
		
		-- call the proc to update the log record upon successfull completion and generate the PowerShell scripts for tha proc ...
		insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, 'set Catalog xx; exec AM_Log_Writer (''P'', ''U'', ' || i.Proc_Id || ', '''');', current_timestamp, current_user);
		insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, 'commit;', current_timestamp, current_user);
		
		-- insert a blank row for formatting ...
		insert into AM_SQL_Scripts values(next value for AM_SQLScript_Seq, '', current_timestamp, current_user);
		
		
		commit;
			
			
	end loop;
			
			
			
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '', current_timestamp, current_user);		
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '------------------------------------------------------------------------------------------------------------------', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '-- The section below is post-sql operations that need to run after the sql has run every time ...', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '------------------------------------------------------------------------------------------------------------------', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '', current_timestamp, current_user);

																												
	-- insert the proc to update the schedule ...
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '-- update the schedule ...', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, 'set Catalog xx;  exec AM_Schedule_Run (current_date, ''' || SchedRunFileName || ''', ''' || NZOprPath || ''', ''' || LogPath || ''');', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, 'commit;', current_timestamp, current_user);
	insert into AM_SQL_Scripts values (next value for AM_SQLScript_Seq, '', current_timestamp, current_user);
	
	
	commit;


		
		------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		

		-- write out the sql script ...
		FileExecString := '
			create external table ''' || TempPath ||  SQLScriptName ||  '''
			using (
				RemoteSource ODBC
				LogDir ''' || LogPath || '''
			)
			as
			
			select Script_Value
			from AM_SQL_Scripts
			order by Script_Line';
			
			
		execute immediate FileExecString;
	
	
		drop table AM_SQL_Scripts if exists;
	
		
	Return True;
					
		
	

end;


end_proc;