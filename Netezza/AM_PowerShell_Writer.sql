/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 10/30/2017

Proc Info:  This proc will write out the PowerShell script at the location and with the file name given.  Must use as ".ps1" file type; ex: PowerShell Script.ps1.

		
		
Input Parameters:
	FileName: The name of the file with a "ps1" extension
	FilePath: The path to write the file to


	
Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xx.xx.AM_PowerShell_Writer (text, text, text, text, text, text, boolean, text, text)
Language NZPLSQL Returns boolean
as
begin_proc


	declare

		FilePath alias for $1;
		PostFileName alias for $2;
		PreFileName alias for $3;
		LogPath alias for $4;
		NoPostPSName alias for $5;
		NoPrePSName alias for $6;
		PDebug alias for $7;
		PrePost alias for $8;
		VarDefName alias for $9;
		
		ExecString varchar(4000);
		
		NextBatchId int;
		
		SeqCheck int;
		
		i record;
		j record;
		
		PSCnt int;
		FileNameOut varchar(50);
		PSFileName varchar(50);
		PSFiles varchar(500);


	begin

		-- get the batch id
		select max(Batch_Id)
		into NextBatchId
		from AM_Batch;

		
		
		drop table RunProcs if exists;
		drop table Scripts if exists;
		
		create table RunProcs as 
				
			select distinct b.Proc_Id
				,pr.Proc_Code
				,r.Report_name
			from AM_Batch b
			join AM_Proc pr on b.Proc_Id = pr.Proc_Id
			join AM_Report r on pr.Report_Id = r.Report_Id
			join AM_Script s on b.Proc_Id = s.Proc_Id   				-- join to get if any have pre-sql commands
				and Pre_Post = PrePost	  						        	   -- any pre-sql commands
			where b.Batch_Status = 'A';
				
				
		create table Scripts as
		
			select b.Proc_Id 
				,pr.Proc_Code
				,s.Pre_Post
				,s.Script_Order
			from AM_Script s
			join AM_Batch b on b.Proc_Id = s.Proc_Id
			join AM_Proc pr on b.Proc_Id = pr.Proc_Id
			where Pre_Post = PrePost
				and b.Batch_Status = 'A';
			
			
			select count(*)
			into PSCnt
			from RunProcs;	
		

		if PrePost = 'B' then 
		
			-- set the output file name
			FileNameOut := PreFileName;
			
		
			if PSCnt = 0 then
			
				ExecString := '
					create external table ''' || FilePath || NoPrePSName || '''
					using (
						RemoteSource ODBC
						LogDir ''' || LogPath || '''
					)
					as
					
					select ''No pre PowerShell scripts to run today''';
					
					
				execute immediate ExecString;
			
			
				call AM_Log_Writer( 'B', 'I', null, 'No Pre-sql PowerShell Commands To Run Today');
				
			
				exit;
				
			end if;
			
			
		else
		
			-- set the output file name
			FileNameOut := PostFileName;
			
				
			if PSCnt = 0 then
			
				ExecString := '
					create external table ''' || FilePath || NoPostPSName || '''
					using (
						RemoteSource ODBC
						LogDir ''' || LogPath || '''
					)
					as
					
					select ''No post PowerShell scripts to run today''';
				
	
				execute immediate ExecString;
				
				
				call AM_Log_Writer( 'A', 'I', null, 'No Post-sql PowerShell Commands To Run Today');
	
	
				exit;
				
			end if;
		
		
		end if;
		
	
	
			-- restart the sequence ...
			select count(*)
			into SeqCheck
			from _v_Sequence
			where SeqName = 'AM_PWRSCRIPT_SEQ';
			
			if SeqCheck > 0 then
				drop sequence AM_PwrScript_Seq;
			end if;
		
			create sequence AM_PwrScript_Seq;
	
	
	
			drop table AM_PwrShl_Scripts if exists;
			
			create table AM_PwrShl_Scripts (
			Script_Line int,
			Script_Value varchar(6000),
			CreateDate datetime,
			CreateBy varchar(100)
			);

	
	
			-- insert the batch id record to tie it off to the log table ....
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '#----------------------------------------------------------------------------------------------------------------', current_timestamp, current_user);
			
			if PrePost = 'B' then
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '# This pre-sql script is for batch id: ' || NextBatchId ||',     created at : ' || current_timestamp, current_timestamp, current_user);
			else
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '# This post-sql script is for batch id: ' || NextBatchId ||',     created at : ' || current_timestamp, current_timestamp, current_user);
			end if;
			
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '#----------------------------------------------------------------------------------------------------------------', current_timestamp, current_user);
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '# Include the variable definition file ...', current_timestamp, current_user);
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '. "' || VarDefName || '"', current_timestamp, current_user);
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
			insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
			
			PSFiles := '';
				
			for j in 
			
				select Proc_Id
					,Proc_Code
					,Report_Name
				from RunProcs
				
				
			loop
		
				-- insert the proc header  ...
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '#-----------------------------------------------------------------------------------------------------------------------------------------', current_timestamp, current_user);
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '# Report Name: ' || j.Report_Name || '; Proc Code: ' || j.Proc_Code || ';  Proc_Id: ' || j.Proc_Id, current_timestamp, current_user);
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, 'MainLogRecord -Section $global:Section -Type A -Entry "Report Name: ' || j.Report_Name || '; Proc Code: ' || j.Proc_Code || ';  Proc_Id: ' || j.Proc_Id || '"', current_timestamp, current_user);
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
		
				for i in 
				
					select Proc_Code
						,Pre_Post
						,Script_Order
					from Scripts
					where Proc_Id = j.Proc_Id
					order by Script_Order

					
				loop

					PSFileName :=  i.Proc_Code || '_' || i.Pre_Post || i.Script_Order || '.ps1';
					
					PSFiles := PSFiles || ', ' || PSFileName;
					
					insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, 'MainLogRecord -Section $Global:Section -Type A -Suppress T -Entry "' || PSFileName || ' Starting ..."', current_timestamp, current_user);
					insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '& ( $WN_ProcPS_Path + "' || PSFileName || '")', current_timestamp, current_user);
					insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, 'MainLogRecord -Section $Global:Section -Type A -Suppress T -Entry "' || PSFileName || ' Completion"', current_timestamp, current_user);
					
				end loop;
				
				
				-- insert the blank as a seperator between the different script sections ...
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
				insert into AM_PwrShl_Scripts values (next value for AM_PwrScript_Seq, '', current_timestamp, current_user);
				
				
			end loop;
			
			
			
			-- write out the file ...
			ExecString := 'create external table ''' || FilePath || FileNameOut || '''
				using (
					RemoteSource ''ODBC''
					LogDir ''' || LogPath || '''
					Delim ''\r''
				)
				as
				
				select Script_Value
				from AM_PwrShl_Scripts
				order by Script_Line;';
				
				
				If PDebug is True then
					raise notice '%', ExecString;
				end if;
			
			
			execute immediate ExecString;
			
			call AM_Log_Writer( PrePost, 'I', null, trim(substring(PSFiles, 2, length(PSFiles))));


			drop table RunProcs if exists;
			drop table Scripts if exists;
			drop table AM_PwrShl_Scripts if exists;

		
		
	return True;


	end;
	

end_proc;