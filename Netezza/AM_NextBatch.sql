/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 11/9/2017

Proc Info: This proc will pull the next batch id from the log table and create a reference table that houses the current value.
This will also create the today's run table.  If there is nothing to run output the "No Run" file.

		
		
Input Parameters:


	
Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xx.xx.AM_NextBatch(text, text, text, date)
Language NZPLSQL Returns  boolean
as
begin_proc


	declare

		BatchFile alias for $1;
		TempPath alias for $2;
		LogPath alias for $3;
		RunDate alias for $4;

		ChkCnt int;
		NextBatchId int;
		
		FileExecString varchar(2000);
		
		LogId int;
		
		i record;
		ProcCodes varchar(300);

	begin
	
	
	
		---------------------------------------------------------------------------------------------------
		-- maintain a six month window on the log table
		
		delete from AM_Log
		where CreateDate < add_months(current_date, -6);
		
		
		
		---------------------------------------------------------------------------------------------------
		-- set the batch
		
		
		truncate table AM_Batch;
		
		-- get the next batch id from the log ...
		select nvl(max(Batch_Id), 0) + 1
		into NextBatchId
		from AM_Log;
			 
		commit;

	
	
		----------------------------------------------------------------------------------
		-- get the list of stuff to run
		
		create temp table TodaysRun as
		
			select Proc_Id
				,Proc_Code
				,Schedule_Id
				,Proc_Order
			from (
					
				select sr.Proc_Id
					,pr.Proc_Code
					,sr.Schedule_Id
					,pr.Proc_Order 
					,row_number() over (partition by sr.Proc_Id order by sr.Run_Date desc) as Last_Needed_Run
				from AM_Schedule_Run sr
				join AM_Proc pr on sr.Proc_Id = pr.Proc_Id
				where Run_Date <= RunDate		   		                            	  -- the ones that need to be run
					and Run_Date <= current_date							  	   	  -- don't run them before their day
				 	and Actual_Run_Date is null 						 			      -- make sure it hasn't ran already
				 	and Proc_Status = 'A'												  -- only active procs
					and sr.Schedule_Type = 'P'										  -- only the procs, not the reminders
					and Run_Date > nvl(Last_Run_Date, '2000-01-01')		 -- only run ones that are past the last time it actually ran, if it never ran put some ridiculous date in the past
					
				) a
				where Last_Needed_Run = 1;
			
			
			
			select count(*)
			into ChkCnt
			from TodaysRun;
		
		
		
		
		
			if ChkCnt = 0 then 
			
			
				-- insert the nothing to run record
				insert into AM_Batch values (NextBatchId, 'A', RunDate, 0, null, 0, 0, current_timestamp);
				
		
			else
		
				-- If there is stuff to run, insert the procs
				insert into AM_Batch
		
					select NextBatchId
						,'A'
						,RunDate
						,Proc_Id
						,Proc_Code
						,Schedule_Id
						,Proc_Order
						,current_timestamp
					from TodaysRun;
					
					
			end if;
		
		
			FileExecString := '
					create external table ''' || TempPath ||  BatchFile || '''
					using (
						RemoteSource ODBC
						Delim '',''
						IncludeHeader
						LogDir ''' || LogPath || '''
					)
					as
					
					select b.Batch_Id
						,b.Run_Date
						,r.Report_Name
						,pr.Proc_Text
						,s.Schedule_Desc
					from AM_Batch b
					left join AM_Proc pr on b.Proc_Id = pr.Proc_Id
					left join AM_Report r on pr.Report_Id = r.Report_Id
					left join AM_Schedule s on b.Schedule_Id = s.Schedule_Id;';
					
					
				execute immediate FileExecString;
				
				
				
				
				----------------------------------------------------------------------------------
				-- update the log
				
				
			if ChkCnt = 0 then 
			
			
				call AM_Log_Writer ('I', 'I', null, 'RunDate: ' || to_char(RunDate, 'YYYY-MM-DD') || ' ;  Nothing To Run Today');
			
			else
				
				ProcCodes := '';
				
				for i in 	
				
					select Proc_Code
					from TodaysRun
				
				loop
			
					ProcCodes := ProcCodes || ', ' || i.Proc_Code;
					
				end loop;
				
				call AM_Log_Writer ('I', 'I', null, 'RunDate: ' || to_char(RunDate, 'YYYY-MM-DD') || ' ;  ProcCodes: ' || trim(substring(ProcCodes, 2, length(ProcCodes))) );
				
				
			end if;
				
				
			Return True;

			
end;


end_proc;