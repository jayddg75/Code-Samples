/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 11/2/2017

Proc Info: This proc will rerun the scheduled run table and update with any successfull runs and show the schedule for about the next two months.
If a successfull run happen

		
		
Input Parameters:
	RunDate: The date to run as; typically current_date.  You can run for a past day as well.


	
Example call:

	
	
/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create or replace procedure xxx.xxx.AM_Schedule_Run (date, text, text, text)
Language NZPLSQL Returns boolean
as
begin_proc


	declare
	
		RunDate alias for $1;
		FileName alias for $2;
		OprPath alias for $3;
		LogPath alias for $4;
		
		NextMonth date;
		
		RunMonth int;
		RunYear int;
		
		ExecString varchar(4000);
		TableName varchar(200);
	
	

	begin

		NextMonth := add_months(RunDate, 1);
		RunMonth := date_part('month', RunDate);
		RunYear := date_part('year', RunDate);
		
		
		------------------------------------------------------------------------------------------------------
		-- get the list of when the schedules are supposed to run

		create temp table DaysWeek (
		DayNumber int,
		DayText varchar(10)
		);

		insert into DaysWeek values (1, 'SUN');
		insert into DaysWeek values (2, 'MON');
		insert into DaysWeek values (3, 'TUE');
		insert into DaysWeek values (4, 'WED');
		insert into DaysWeek values (5, 'THU');
		insert into DaysWeek values (6, 'FRI');
		insert into DaysWeek values (7, 'SAT');


		drop table SchedRun if exists;

		create table SchedRun (
		Schedule_Id int,
		Run_Date date
		);
			
			
		for i in -2 .. 2 loop	
		
			NextMonth := add_months(RunDate, i);
				
			insert into SchedRun

				select Schedule_Id, to_date(date_part('year', NextMonth) || '-' || date_part('month', NextMonth) || '-' || Schedule_Day, 'YYYY-MM-DD') 
				from AM_Schedule
				where Schedule_Freq = 'M'
					and Schedule_Code <> 'EOM';
					
					
			insert into SchedRun

				select Schedule_Id, last_day(to_date(date_part('year', NextMonth) || '-' || date_part('month', NextMonth) || '-1', 'YYYY-MM-DD') )
				from AM_Schedule
				where Schedule_Freq = 'M'
					and Schedule_Code = 'EOM';
			
		end loop;

		
		for i in -8 .. 8 loop
		
			insert into SchedRun

				select Schedule_Id,  next_day(RunDate + ( 7 * i ), DayText)
				from AM_Schedule s
				join DaysWeek dw on s.Schedule_Day = dw.DayNumber
				where Schedule_Freq = 'W';
				
		end loop;
				

		
		
		------------------------------------------------------------------------------------------------------------
		-- insert into the table 
		
		truncate table AM_Schedule_Run;
		
		insert into AM_Schedule_Run
		
 			select 'P' as Schedule_Type 
				,a.Proc_Id
				,a.Schedule_Id
				,l.Batch_Id
				,a.Run_Date
				,a.Next_Run
				,a.Prev_Run
				,date_trunc('day', l.CreateDate)
				,max(date_trunc('day', l.CreateDate)) over (partition by a.Proc_Id order by l.CreateDate desc) as Last_Run_Date
				,current_timestamp
			from (
			
				select ps.Proc_Id, 
					s.Schedule_Id, 
					s.Run_Date, 
					nvl(lead(Run_Date) over (partition by ps.Proc_Id order by Run_Date), '9999-12-31') as Next_Run,
					nvl(lead(Run_Date) over (partition by ps.Proc_Id order by Run_Date desc), '2000-01-01') as Prev_Run
				from SchedRun s
				join AM_Proc_Sched ps on s.Schedule_Id = ps.Schedule_Id
				join AM_Proc pr on ps.Proc_Id = pr.Proc_Id
				
			) a
			left join AM_Log l on a.Proc_Id = l.Proc_Id
				and date_trunc('day', l.CreateDate) >= Run_Date
				and date_trunc('day', l.CreateDate) < Next_Run
				and l.Log_Status = 'S';
			
			
			update AM_Schedule_Run set
				Last_Run_Date = null
			where Run_Date > current_date;
				
				
			-- insert the reminders
			insert into AM_Schedule_Run
			
				select 'R' as Schedule_Type
					,rm.Proc_Id
					,rm.Schedule_Id
					,null as Batch_Id
					,s.Run_Date
					,null
					,null
					,null
					,null
					,current_timestamp
				from AM_Reminder rm
				join SchedRun s on rm.Schedule_Id = s.Schedule_Id;
			
		
			drop table SchedRun if exists;


			-----------------------------------------------------------------------------------------------------------------
			-- write out the file ...
			
			-- create the external table
			
			TableName := substring(FileName, 0, length(FileName) - 3);
			
			ExecString := '	
				create external table ' || TableName || '
				(
					Type varchar(8),
					Report_Name varchar(500),
					Description varchar(4000),
					Schedule_Desc varchar(500),
					Run_Date date,
					Actual_Run_Date date
				)
				using (
					DataObject ''' || OprPath  || FileName || '''
					RemoteSource ODBC
					Delim '',''
					IncludeHeader
					LogDir ''' || LogPath || '''
				);';
			
				
			execute immediate ExecString;
			
			
			-- write out the file from the view
			ExecString := '
			
				insert into ' || TableName || '
			
					select Type
						,Report_Name
						,Description
						,Schedule_Desc
						,Run_Date
						,Actual_Run_Date
					from AM_Run_Schedule
					where date_part(''month'', Run_Date) = ' || RunMonth || '
						and date_part(''year'', Run_Date) = ' || RunYear || '
					order by Run_Date, Proc_Order';
					
					
			execute immediate ExecString;
			
			-- drop the external table
			ExecString := 'drop table ' || TableName || ';';
			
			execute immediate ExecString;
			
			
			Return True;
			
			
	end;


end_proc;