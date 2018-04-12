/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Created by: Jason Schilli
Create Date: 12/8/2017

This view is used to generate a readable run schedule

	
drop view xx.xx.AM_Run_Schedule;


/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

create view xxxx.xx.AM_Run_Schedule as

	select Type
	,Report_Name
	,Description
	, Schedule_Desc
	,Proc_Order
	,Run_Date
	,Actual_Run_Date
	,Batch_Id
	from (

		select 'Run' as Type 
			,r.Report_Name
			,pr.Proc_Text as Description
			,s.Schedule_Desc
			,pr.Proc_Order
			,sr.Run_Date
			,sr.Actual_Run_Date
			,sr.Batch_Id
		from AM_Schedule_Run sr
		join AM_Proc pr on sr.Proc_Id = pr.Proc_Id
			and pr.Proc_Status = 'A'
		join AM_Schedule s on sr.Schedule_Id = s.Schedule_Id
		join AM_Report r on pr.Report_Id = r.Report_Id
		where sr.Schedule_Type = 'P'

		union all

		select 'Reminder' as Type 
			,r.Report_Name
			,rm.Reminder_Text
			,s.Schedule_Desc
			,null
			,sr.Run_Date
			,null as Actual_Run_Date
			,null as Batch_Id
		from AM_Schedule_Run sr
		join AM_Reminder rm on sr.Proc_Id = rm.Proc_Id
			and sr.Schedule_Id = rm.Schedule_Id
		join AM_Proc pr on sr.Proc_Id = pr.Proc_Id
		join AM_Report r on pr.Report_Id = r.Report_Id
		join AM_Schedule s on sr.Schedule_Id = s.Schedule_Id
		where sr.Schedule_Type = 'R'

	) a;