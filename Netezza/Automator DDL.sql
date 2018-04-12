
set catalog xxxxxxx;


------------------------------------------------------------------------------------------------

drop table AM_Schedule if exists;

create table AM_Schedule (
Schedule_Id int,
Schedule_Code varchar(10),
Schedule_Desc varchar(500),
Schedule_Freq char(1),
Schedule_Day int,
CreateDate datetime,
UpdateDate datetime
);

drop sequence AM_Schedule_Seq;

create sequence AM_Schedule_Seq as integer;




------------------------------------------------------------------------------------------------

drop table AM_Report if exists;

create table AM_Report (
Report_Id int,
Report_Code varchar(10),
Report_Name varchar(500),
CreateDate datetime,
UpdateDate datetime
);


drop sequence AM_Report_Seq;

create sequence AM_Report_Seq as integer;




------------------------------------------------------------------------------------------------

drop table AM_Proc if exists;

create table AM_Proc (
Proc_Id int,
Proc_Code varchar(10),
Proc_Status char(1),
Report_Id int,
Proc_Order int,
Proc_Database varchar(50),
Proc_Schema varchar(50),
Proc_Text varchar(2000),
CreateDate datetime,
UpdateDate datetime
);


drop sequence AM_Proc_Seq;

create sequence AM_Proc_Seq as integer;




------------------------------------------------------------------------------------------------

drop table AM_Proc_Sched if exists;

create table AM_Proc_Sched (
Proc_Sched_Id int,
Proc_Id int,
Schedule_Id int,
CreateDate datetime
);


drop sequence AM_ProcSched_Seq;

create sequence AM_ProcSched_Seq as integer;




------------------------------------------------------------------------------------------------

drop table AM_Parameter if exists;

create table AM_Parameter (
Parm_Id int,
Proc_Id int,
Parm_Type char(1),
Parm_String_Ind boolean,
Parm_Order int,
Parm_Name varchar(500),
Parm_Value varchar(1000),
CreateDate datetime,
UpdateDate datetime
);


drop sequence AM_Parameter_Seq;

create sequence AM_Parameter_Seq as integer;





------------------------------------------------------------------------------------------------

drop table AM_Script if exists;

create table AM_Script (
Script_Id int,
Proc_Id int,
Pre_Post char(1),
Script_Order int,
Script_Text varchar(2000),
CreateDate datetime,
UpdateDate datetime
);

drop sequence AM_Script_Seq;

create sequence AM_Script_Seq as integer;




------------------------------------------------------------------------------------------------

drop table AM_Reminder if exists;

create table AM_Reminder (
Reminder_Id int,
Proc_Id int,
Schedule_Id int,
Reminder_Text varchar(4000),
CreateDate datetime
);


drop sequence AM_Reminder_Seq;

create sequence AM_Reminder_Seq as integer;



------------------------------------------------------------------------------------------------

drop table AM_Log if exists;

create table AM_Log (
Log_Id int,
Batch_Id int,
Rec_Id char(1),
Proc_Id int,
Log_Status char(1),
Log_Executable varchar(2000),
StartTime datetime,
EndTime datetime,
CreateDate datetime,
CreateBy varchar(100)
);

drop sequence AM_Log_Seq;

create sequence AM_Log_Seq as integer;



------------------------------------------------------------------------------------------------

drop table AM_Schedule_Run if exists;

create table AM_Schedule_Run (
Schedule_Type char(1),
Proc_Id int,
Schedule_Id int,
Batch_Id int,
Run_Date date,
Next_Run_Date date,
Prev_Run_Date date,
Actual_Run_Date date,
Last_Run_Date date,
CreateDate datetime
);




------------------------------------------------------------------------------------------------

drop table AM_Batch if exists;

create table AM_Batch (
Batch_Id int,
Batch_Status char(1),
Run_Date date,
Proc_Id int,
Proc_Code varchar(10),
Schedule_Id int,
Proc_Order int,
CreateDate datetime
);



------------------------------------------------------------------------------------------------

drop table AM_Dynamic_Parameter if exists;

create table AM_Dynamic_Parameter (
DynParm_Id int,
DynParm_Code varchar(20),
DynParm_Description varchar(500),
DynParm_Value varchar(2000),
CreateDate datetime,
UpdateDate datetime
);


drop sequence AM_DynParm_Seq;

create sequence AM_DynParm_Seq as integer;