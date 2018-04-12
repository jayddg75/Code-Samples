	/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 12/8/2017

Proc Info: This proc will generate all the dynamic parameter values and check to make sure that all the dynamic parameters defined
in AM_Parameter are in the table once it has been updated.  If there are some missing write out a problem file and exit;

		
		
Input Parameters:


	
Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xx.xx.AM_Dynamic_Parameter (text, text, text, text, text)
Language NZPLSQL Returns int
as
begin_proc


declare
	
	
	TempPath alias for $1;
	DropoffPath alias for $2;
	ImportPath alias for $3;
	LogPath alias for $4;
	ParmErrorFile alias for $5;
	
	ExecString varchar(4000);
	
	RunDate date;
	BatchId int;
	
	ParmChkCnt int;
	ParmString varchar(300);
	
	LogEntry varchar(4000);
	
	i record;


begin
	
	
	-- get the run date and batch id
	select max(Run_Date)
	into RunDate
	from AM_Batch;
	
	select max(Batch_Id)
	into BatchId
	from AM_Batch;
	
	

	
	-- create the table of dynamic run time parameters to pass into the procs ...
		
	drop table Dyn_Parm if exists;

	create table Dyn_Parm (
	Code varchar(20),
	Code_Descrption varchar(500),
	Code_Value varchar(1000)
	);

	insert into Dyn_Parm values ('PMM', 'Previous month''s month', date_part('month', add_months(RunDate, -1)));
	insert into Dyn_Parm values ('PMY', 'Previous month''s year', date_part('year', add_months(RunDate, -1)));
	insert into Dyn_Parm values ('TempPath', 'The temp folder path', TempPath);
	insert into Dyn_Parm values ('DropPath', 'The drop off folder path', DropoffPath);
	insert into Dyn_Parm values ('ImpPath', 'The import folder path', ImportPath);
	insert into Dyn_Parm values ('FileLogPath', 'The log folder path', LogPath);
	insert into Dyn_Parm values ('PD', 'The previous day', RunDate - 1);
	insert into Dyn_Parm values ('PWS', 'The last sunday', next_day(RunDate - 14, 'SUN'));
	insert into Dyn_Parm values ('EPM', 'End of the previous month', last_day(add_months(RunDate, -1)));
	insert into Dyn_Parm values ('RD', 'The run date', RunDate);



	---------------------------------------------------------------------------------------------------------------------------------------------
	-- update the AM_Dynamic_Parameter table

	-- insert new records	
	insert into AM_Dynamic_Parameter
		
		select next value for AM_DynParm_Seq
			,Code
			,Code_Descrption
			,Code_Value
			,current_timestamp
			,current_timestamp
		from Dyn_Parm
		where Code  in (
		
			select Code
			from Dyn_Parm
			
			except

			 select DynParm_Code
			 from AM_Dynamic_Parameter
			 
		) ;
	
		
	-- update any changes
	update AM_Dynamic_Parameter t set
		t.DynParm_Description = Code_Descrption
		,t.DynParm_Value = Code_Value
		,t.UpdateDate = current_timestamp
	from Dyn_Parm s
	where s.Code = t.DynParm_Code
		and s.Code in (
	
		select Code
		from (
		
			select Code
				,Code_Descrption
				,Code_Value
			from Dyn_Parm
			
			except

			 select DynParm_Code
			 	,DynParm_Description
				,DynParm_Value
			 from AM_Dynamic_Parameter
			 
		 ) a
		  
	);
	

	-- get rid of old records
	delete from AM_Dynamic_Parameter
	where DynParm_Code in (

		select DynParm_Code
		 from AM_Dynamic_Parameter
		
		except
		
		select Code
		from Dyn_Parm
		
	) ;
		
		
		
		

	---------------------------------------------------------------------------------------------------------------------------------------------
	-- check to make sure all the of dynamic parameters defined in the Parameter table have an entry here
		
		
	drop table ParmCheck if exists;
	
	create table ParmCheck (
	Parm_Code varchar(20),
	Parm_Error varchar(500)
	);
		
		
	insert into ParmCheck
	
		select Parm_Value
			,'Missing value dynamic parameter definition'
		from (
			
			select Parm_Value
			from AM_Parameter
			where Parm_Type = 'D'
			
			except
			
			select DynParm_Code
			from AM_Dynamic_Parameter
			
		) a;
		
		
		select count(*)
		into ParmChkCnt
		from ParmCheck;
			
				
		if ParmChkCnt > 0 then
		
			ParmString := '';
		
			for i in 
			
				select Parm_Code
				from ParmCheck
				
			loop
			
				ParmString := ParmString || ',' || i.Parm_Code;
				
			end loop;
		
				
			select Log_Executable
			into LogEntry 
			from AM_Log
			where Batch_Id = BatchId
				and Rec_Id = 'I';
				
				
			-- update the log record to show there was a problem
			call AM_Log_Writer( 'I', 'U', null, LogEntry || ' ; Parameter Problem : ' || substring(ParmString, 2, length(ParmString)) );
		
				
		
			-- write out the problem file ...
			ExecString := '
				create external table ''' || TempPath || ParmErrorFile || '''
				using (
					RemoteSource ODBC
					Delim '',''
					IncludeHeader
					LogDir ''' || LogPath || '''
				)
				as
				
				select *
				from ParmCheck';
				
			execute immediate ExecString;
			
			return 0;
			
			exit;
			
		end if;
		

	drop table Dyn_Parm if exists;
	drop table ParmCheck if exists;
		
	return 1;



end;


end_proc;