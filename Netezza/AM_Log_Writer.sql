/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 10/30/2017

Proc Info: This proc will generate the script records for maintaining the logs and will geneate the PowerShell scripts for the Proc_Id when the proc gets called to mark
a successfull completion of the Proc_Id that gets passed in.

		
		
Input Parameters:
	1. Action: I for intial or U for completion updated
	2. NextLogId: The log id from the parent proc
	3. ProcId: The Proc_Id from the parent proc
	4. ExecString: The executable string from the parent proc


Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xx.xx.AM_Log_Writer (text, text, int, text)
Language NZPLSQL Returns  boolean
as
begin_proc


declare

	PType alias for $1;
	PAction alias for $2;
	ProcId alias for $3;
	ExecString alias for $4;
	
	j record;
	
	BatchId int;
	NextLogId int;
	

begin


		select max(Batch_Id)
		into BatchId
		from AM_Batch;


	-- for proc log records ...
	if PType = 'P' then 

		-- write out the scripts to generate the log record and the executable ...
		if PAction = 'I' then
		
			NextLogId := next value for AM_Log_Seq;
		
			insert into AM_Log (Log_Id, Batch_Id, Rec_Id, Proc_Id, Log_Status, Log_Executable, StartTime, CreateDate, CreateBy) values ( NextLogId, BatchId, PType, ProcId, 'R',  ExecString, current_timestamp, current_timestamp, current_user);
			
			commit;
				
		else

			-- if the proc ran sucesssfully then update the log and generate the powershell scripts for that proc
			update AM_Log set 
				Log_Status = 'S',
				EndTime = current_timestamp
			where Batch_Id = BatchId
				and Proc_Id = ProcId
				and Rec_Id = 'P';
				
			commit;
					
		end if;
		
	end if;
			
			
	If PType <>  'P' then
	
	
		If PAction = 'I' then
		
			NextLogId := next value for AM_Log_Seq;
		
			insert into AM_Log (Log_Id, Batch_Id, Rec_Id, Log_Status, Log_Executable, StartTime, CreateDate, CreateBy) values ( NextLogId, BatchId, PType, 'R',  ExecString, current_timestamp, current_timestamp, current_user);
			
			commit;
			
		else
		
		
			if ExecString  is null then
	
				update AM_Log s set
					Log_Status = 'S',
					EndTime = current_timestamp
				where s.Batch_Id = BatchId
					and s.Rec_Id = PType;
					
				commit;
				
			else
			
				update AM_Log s set
					Log_Status = 'S',
					EndTime = current_timestamp,
					Log_Executable = ExecString
				where s.Batch_Id = BatchId
					and s.Rec_Id = PType;
					
					
				commit;
			
			end if;

		
		end if;
	
	end if;
		
	
	Return True;


end;


end_proc;