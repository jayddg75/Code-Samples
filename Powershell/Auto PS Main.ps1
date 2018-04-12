#------------------------------------------------------------------------------------------------------------------------------------------
# Created By: Jason Schilli
# Create Date: 11/13/2017
#
# File Name: Rpt Auto PS Main
#  
# This script is the main PS script which drives the automation process.  This gets called on by Windows Task to execute at a scheduled time
# This script is basically divided into eight sections, in order ...
#   1. Run Settings
#   2. Definitions
#   3. Setup
#   4. Initilization
#   5. Pre-sql PowerShell commands
#   6. Generate and run SQL script
#   7. Post-SQL PowerShell
#   8. Wrap up
#
#------------------------------------------------------------------------------------------------------------------------------------------






#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 1. Run Settings



#--------------------------------------------------------------------------
# define the script parameters


param(


[String]$RunDate = (get-date -UFormat '%Y-%m-%d'),


[ValidateSet("T", "F")]
[String] $AutoPilot ="T",


[ValidateSet("File", "Prompt")]
[String] $Creds ="File",


[String] $CredsFileName = "Encrypted.txt",


[ValidateSet("T", "F")]
[String] $RunInitilization = "T",


[ValidateSet("T", "F")]
[String] $RunPrePS ="T",


[ValidateSet("T", "F")]
[String] $RunSQL ="T",


[ValidateSet("T", "F")]
[String] $RunPostPS ="T"


)







try {




$PT = 1
$PI = 0


if ($AutoPilot -eq "T") { $APMsg = "Running in auto pilot mode" } else { $APMsg = "Running in prompted mode" }
if ($Creds -eq "File") { $CredsMsg = "Running stored credentials" } else { $CredsMsg = "Prompting for DB credentials" }

if($RunInitilization -eq "T") { 
    
    $RunInitilizationMsg = "Running" 
    $PT++

    } else { $RunInitilizationMsg = "Skipping" }


if($RunPrePS -eq "T") { 

    $RunPrePSMsg = "Running"
    $PT++ 
    
    } else { $RunPrePSMsg = "Skipping" }


if($RunSQL -eq "T") { 

    $RunSQLMsg = "Running"
    $PT++
    
     } else { $RunSQLMsg = "Skipping" }


if($RunPostPS -eq "T") { 

    $RunPostPSMsg = "Running" 
    $PT++

    } else { $RunPostPSMsg = "Skipping" }




#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 2. Definitions


$Section = "Definitions"




#--------------------------------------------------------------------------
# Email settings

#The notifications is the list that is used to send automation process emails to.
#So if there is an error it can be fixed

$NotificationsEmailList = "some@email.address"

$AutoSubjLine = "Report Automation:"
$ErrSubjLine = ($AutoSubjLine + " Error")
$GoodSubjLine = ($AutoSubjLine + " Completion")
$NoteSubjLine = ($AutoSubjLine + " Notifications")



#--------------------------------------------------------------------------
# Folder structure

# the top level folder path 
$MainPath = "Some Path"


# End file path folders ....
$TmpName = "Temp Files"
$OprName = "Operational Files"
$LogName = "Log Files"
$HistLogName = "Historical Log Files"
$DrpOffName = "Dropoff"
$ImportName = "Import"
$PrcPSName = "Proc PS Scripts"
$CredsName = "Credentials"
$BackUpName = "BackUp"
$ProcPSName = "Proc PS Scripts"




#--------------------------------------------------------------------------
# set folder paths

#Windows paths ...
$TmpFldr = $MainPath + $TmpName + "\"
$OprFldr = $MainPath + $OprName + "\"
$LgFldr = $MainPath + $LogName + "\"
$DrpFldr = $MainPath + $DrpOffName + "\"
$ImpFldr = $MainPath + $ImportName + "\"
$BckUpFldr = $MainPath + $BackUpName + "\"
$PPSFldr = $OprFldr + $PrcPSName + "\"
$CredFldr = $OprFldr + $CredsName + "\"
$HistLogFldr = $LgFldr + $HistLogName + "\"


# Netezza folders
$NZ_TmpFldr = ($TmpFldr -replace "\\", "\\") -replace "\\\\\\\\", "\\"
$NZ_DrpFldr = ($DrpFldr -replace "\\", "\\") -replace "\\\\\\\\", "\\"
$NZ_ImpFldr = ($ImpFldr -replace "\\", "\\") -replace "\\\\\\\\", "\\"
$NZ_LgFldr = ($LgFldr -replace "\\", "\\") -replace "\\\\\\\\", "\\"
$NZ_OprFldr = ($OprFldr -replace "\\", "\\") -replace "\\\\\\\\", "\\"


#if the temporary folders don't exists create them ....
if (-not (Test-Path($TmpFldr))) {New-Item -Path $MainPath -Name $TmpName -ItemType "directory"}
if (-not (Test-Path($LgFldr))) {New-Item -Path $MainPath -Name $LogName -ItemType "directory"}
if (-not (Test-Path($DrpFldr))) {New-Item -Path $MainPath -Name $DrpOffName -ItemType "directory"}
if (-not (Test-Path($ImpFldr))) {New-Item -Path $MainPath -Name $ImportName -ItemType "directory"}
if (-not (Test-Path($HistLogFldr))) {New-Item -Path $LgFldr -Name $HistLogName -ItemType "directory"}



#--------------------------------------------------------------------------
# set file names ...

# sql file names
$SQLScrptName = "MainSQLScript.sql"
$SQLRunFileName = "Run.sql"

# flag and info files
$SQLSuccessName = "SQLSuccess.txt"
$BatchName = "Batch.csv"
$ScheduleName = "RunSchedule.csv"
$NoPostSLQName = "NoPostSQLPS.txt"
$NoPreSQLName = "NoPreSQLPS.txt"
$NoMainSQLName = "NoMainSQL.txt"
$ParmErrorsName = "ParmErrors.csv"
$AddInst = "Additional Instructions.txt"


# PowerShell files
$PSScrptName = "PowerShell_Post.ps1"
$PSPreScrptName = "PowerShell_Pre.ps1"
$CustFuncName = "Functions.ps1"
$VarFileName = "Variables.ps1"

# Log and error files
$LogOutputName = "OutPut.stdout"
$LogErrName = "Errors.stdout"
$LogBackupName = "LogTableBackup.txt"
$LogBackupNameOld = "LogBackupOld.txt"
$MainLogName = "MainLog.txt"
$AuxLogName = "AuxLog.txt"


#--------------------------------------------------------------------------
# set the file paths ...

# sql files
$SQLScript = $TmpFldr + $SQLScrptName
$SQLRunFile = $TmpFldr + $SQLRunFileName  

# PowerShell files
$PostPS = $TmpFldr + $PSScrptName
$PrePS = $TmpFldr + $PSPreScrptName
$CustFunc = $OprFldr + $CustFuncName
$VarFile = $OprFldr + $VarFileName

# flag and info files
$BatchFile = $OprFldr + $BatchName
$NoPS = $TmpFldr + $NoPostSLQName
$NoPrePS = $TmpFldr + $NoPreSQLName
$SchedRunFile = $OprFldr + $ScheduleName
$SQLSuccess = $TmpFldr + $SQLSuccessName  
$ParmError = $TmpFldr + $ParmErrorsName
$AddInstFile = $TmpFldr + $AddInst
$SQLSuccessFile = $TmpFldr + $SQLSuccessFileName
$NoMainSQLFile = $TmpFldr + $NoMainSQLName

# Log and error files
$OutPutFile = $LgFldr + $LogOutputName
$ErrorFile = $LgFldr + $LogErrName
$LogBkpFileOld = $OprFldr + $LogBackupNameOld
$LogBkpFile = $OprFldr + $LogBackupName
$MainLog = $LgFldr + $MainLogName
$AuxLog =  $LgFldr + $AuxLogName



$NZ_RunFile = ( $SQLRunFile -replace "\\", "\\" ) -replace "\\\\\\\\", "\\"
$NZ_OutPutFile = ( $OutPutFile -replace "\\", "\\" ) -replace "\\\\\\\\", "\\"
$NZ_ErrorFile = ( $ErrorFile -replace "\\", "\\" ) -replace "\\\\\\\\", "\\"





#--------------------------------------------------------------------------
# define sql script text


#set the text for the call script write script ...
$CallScrptWrtrText = "

---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- generate the sql script ...

set Catalog xxx; 
exec AM_MainScript_Writer (
    '$SQLScrptName'    -- the name of the sql script 
    ,'$NZ_TmpFldr'     -- the path for the temp files
    ,'$NZ_LgFldr'      -- the path for the log folder
    ,'$NZ_OprFldr'     -- the netezza operational folder
    ,'$ScheduleName'   -- the name of the scheduled run file
    ,'$LogBackupName'  -- the name of the log table backup file
    ,'$NoMainSQLName'  -- the name of the no main sql file
);
commit;

    
"



$LogTableBackupText = "

---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- back up the log table ...

set Catalog xxx;  
exec AM_LogBackup ( 
    '$LogBackupName'
    ,'$NZ_OprFldr'
    ,'$NZ_LgFldr'
);
commit;


"





$CallScheduleRun = "
 
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- run the initilization procs ...


set Catalog xxx; 


-- update the schedule to make sure we have it all ...
exec AM_Schedule_Run (
    '$RunDate'
    ,'$ScheduleName'  -- the name of the schedule file to output 
    ,'$NZ_OprFldr'    -- the netezza operational folder path
    ,'$NZ_LgFldr'     -- the netezza log folder path
);
commit;


-- generate the next batch id
exec AM_NextBatch(
   '$BatchName'       -- the today's procs file name
   ,'$NZ_OprFldr'      -- the netezza temp folder path
   ,'$NZ_LgFldr'       -- the netezza log folder path
   ,'$RunDate'      -- the RunAs date  
);
commit;


-- update the dynamic parameters
exec AM_Dynamic_Parameter ( 
    '$NZ_TmpFldr'
    ,'$NZ_DrpFldr'
    ,'$NZ_ImpFldr'
    ,'$NZ_LgFldr'
    ,'$ParmErrorsName'
);
commit;


-- update the log entry to show a success
exec AM_Log_Writer ('I', 'U', null, null);
commit;


"



$UpdatePrePS = "

---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- update to show successful completion of pre-sql PowerShell commands

set Catalog xxx;
exec AM_Log_Writer ('B', 'U', null, null);
commit;


"



$UpdatePostPS = "

---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- update to show successful completion of post-sql PowerShell commands

set Catalog xxx;
exec AM_Log_Writer ('A', 'U', null, null);
commit;


"



$CallPrePS = "

---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- generate the pre-sql PS script ...

set Catalog xxx;
exec AM_PowerShell_Writer (
    '$NZ_TmpFldr'         -- the netezza temp folder path
    ,'$PSScrptName'        -- the post-sql PS file name
    ,'$PSPreScrptName'     -- the pre-sql PS file name
    ,'$NZ_LgFldr'          -- the netezza log folder path
    ,'$NoPostSLQName'      -- the no post-sql PS file name
    ,'$NoPreSQLName'       -- the no pre-sql PS file name
    ,'False'               -- set debug to false
    ,'B'                   -- pass in the pre-sql PS flag
    ,'$VarFile'            -- the path to the variable definitions file

);
commit;

"




$CallPostPS = "

---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
-- generate the post-sql PS script ...

set Catalog xxx;
exec AM_PowerShell_Writer (
    '$NZ_TmpFldr'          -- the netezza temp folder path
    ,'$PSScrptName'        -- the post-sql PS file name
    ,'$PSPreScrptName'     -- the pre-sql PS file name
    ,'$NZ_LgFldr'          -- the netezza log folder path
    ,'$NoPostSLQName'      -- the no post-sql PS file name
    ,'$NoPreSQLName'       -- the no pre-sql PS file name
    ,'False'               -- set debug to false
    ,'A'                   -- pass in the post-sql PS flag
    ,'$VarFile'            -- the path to the variable definitions file

);
commit;

"




#--------------------------------------------------------------------------
# Variable file which is included on all pre and post PS scripts
# Include the custom functions file as well ...


$Vars = "

# define variables
" + '$WN_ProcPS_Path' + " = ""$PPSFldr""
" + '$WN_DrpOff_Path' + " = ""$DrpFldr""
" + '$WN_Import_Path' + " = ""$ImpFldr""
" + '$WN_VarFile' + " = ""$VarFile""
" + '$NotificationsEmailList' + " = ""$NotificationsEmailList""
" + '$FromEmailAccount' + " = ""$FromEmailAccount""
" + '$PSEmailServer' + " = ""$PSEmailServer""
" + '$AuxLog' + " = ""$AuxLog""
" + '$LgFldr' + " = ""$LgFldr""
" + '$AddInstFile' + " = ""$AddInstFile""
" + '$OutPutFile' + " = ""$OutPutFile""
" + '$MainLog' + " = ""$MainLog""

# include the custom functions
. ""$CustFunc"" 

"




$AppendText = "

commit;
    
set Catalog xxx; 
exec AM_SQLFinish('$SQLSuccessName', '$NZ_TmpFldr', '$NZ_LogFolder'); 

"





$AugLogStart = "
---------------------------------------------------------------------------------------
Auxilary Log
---------------------------------------------------------------------------------------






---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
Variable file ..."



#--------------------------------------------------------------------------
# include the custom functions file

. $CustFunc


#--------------------------------------------------------------------------
# get the password from the encrypted file to pass back to Netezza or 
# prompt for it


if ($Creds -ne "File") {

    $User = Read-Host "Database User Name?"
    $Password = Read-Host "Database Password?"
    Write-Host ""
    Write-Host ""

} else {

    #go to the file and get the encrypted password
    $User = $env:USERNAME

    $SP = Get-Content ($CredFldr + $CredsFileName) | ConvertTo-SecureString
    $CP = New-Object System.Management.Automation.PSCredential -ArgumentList $User, $SP
    $Password = $CP.GetNetworkCredential().Password

}



$NZ_CMDExec = "c:\Program Files\Aginity\Aginity Workbench for PureData System for Analytics\Aginity.NetezzaWorkbench.exe"
$Conn_Strng = "Driver={NetezzaSQL};server=xxx;UserName=$User;Password=$Password;Database=xxx;Query Timeout=120"







#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 3. Setup



$Section = "Setup"


$AutomationStart = "Report Automation: Starting"
$AutomationEnd = "Report Automation: Finished"


if (test-path ($TmpFldr + "*")) {Remove-Item ($TmpFldr + "*")}
if (Test-Path $MainLog) { Remove-Item $MainLog }
if (Test-Path $AuxLog ) { Remove-Item $AuxLog }
if (Test-Path $AddInstFile ) { Remove-Item $AddInstFile }



"LogId|TimeStamp|Section|Type|Status|Entry" > $MainLog
$LogId = 0


MainLogRecord -Section $Section -Entry $AutomationStart -Suppress T
MainLogRecord -Section $Section -Entry ("RunDate: " + $RunDate) -Suppress T



$AugLogStart > $AuxLog 
Add-Content -Path $AuxLog -Value $Vars



# get the batch id in case you are not creating a new batch

if (Test-Path $BatchFile) {

    $Imp = Import-Csv $BatchFile
    $BatchImp = $Imp | Select-Object -First 1 -Property Batch_Id
    $BatchId = $BatchImp[0].BATCH_ID

    MainLogRecord -Section $Section -Entry ("Current Batch File Id: " + $BatchId) -Suppress T

}





Write-Host ""
Write-Host ""
Write-Host ""
Write-Host ""
MainLogRecord -Section $Section -Entry $Section
Write-Host "-----------------------------------------------------------------------"
Write-Host "-----------------------------------------------------------------------"
Write-Host ""
Write-Host ""
Write-Host "RunDate:"
Write-Host $RunDate
Write-Host ""
Write-Host "AutoPilot:"
Write-Host $APMsg
Write-Host ""
Write-Host "Credentials:"
Write-Host $CredsMsg
Write-Host ""
Write-Host "Sections Running For:"
Write-Host "-----------------------"
Write-Host ""
Write-Host ("      Initilization: " + $RunInitilizationMsg)
Write-Host (" Pre-SQL PowerShell: " + $RunPrePSMsg)
Write-Host ("         SQL Script: " + $RunSQLMsg)
Write-Host ("Post-SQL PowerShell: " + $RunPostPSMsg)
Write-Host ""
Write-Host "------------------------------------------------"
Write-Host ""



# Log the setup
MainLogRecord -Section $Section -Entry ("AutoPilot: " + $APMsg)      -Suppress T -Type R
MainLogRecord -Section $Section -Entry ("Credentials: " + $CredsMsg) -Suppress T -Type R

if ($Creds -eq "File") {

    MainLogRecord -Section $Section -Entry ("Credential File Name: " + $CredsFileName) -Suppress T -Type R

}

MainLogRecord -Section $Section -Entry ("Initilization: " + $RunInitilizationMsg)  -Suppress T -Type R
MainLogRecord -Section $Section -Entry ("Pre-SQL PowerShell: " + $RunPrePSMsg)     -Suppress T -Type R
MainLogRecord -Section $Section -Entry ("SQL Script: " + $RunSQLMsg)               -Suppress T -Type R
MainLogRecord -Section $Section -Entry ("Post-SQL PowerShell: " + $RunPostPSMsg)   -Suppress T -Type R




MainLogRecord -Section $Section -Entry "Writing out the variable file ..."

# PowerShell variable file
if (Test-Path $VarFile) { Remove-Item $VarFile }
$Vars > $VarFile 








#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 3. Check for anthing to run



if ($RunInitilization -eq "T") {

    $Section = "Initialization"
    ProgressInd


    Write-Host ""
    Write-Host ""
    Write-Host ""
    Write-Host ""
    MainLogRecord -Section $Section -Entry $Section
    Write-Host "-----------------------------------------------------------------------"
    Write-Host "-----------------------------------------------------------------------"
    Write-Host ""



    MainLogRecord -Section $Section -Entry "Removing old batch file"
    if (Test-Path $BatchFile) { Remove-Item $BatchFile }


    #--------------------------------------------------------------------------
    # generate the list of stuff to run


    RunSQL  -SQLText $CallScheduleRun `
            -NotificationsEmailList $NotificationsEmailList `
            -Description "Updating the schedule, setting the batch, and updating dynamic parameters"




    #--------------------------------------------------------------------------
    # check for missing dynamic parameters


    if (Test-Path $ParmError) {

        MainLogRecord -Section $Section -Status E -Entry "Missing parameters on AM_Dynamic_Parameter, please update the AM_Dynamic_Parameter procedure. Stopping the process."
    
        generate-htmlemail -FilePath $TmpFldr `
            -FileName $ParmErrorsName ` 
            -To $NotificationsEmailList `
            -Subject $ErrSubjLine `
            -FontSize 80 `
            -BodyText "Missing parameters on AM_Dynamic_Parameter, please update the AM_Dynamic_Parameter procedure.<br>Stopping the process.<br><br>"


        CheckLogError

    }




    #--------------------------------------------------------------------------
    # check for either the today's procs files or the no procs file

    if (Test-Path $BatchFile) {


        #import the list of procs to run today and show them in the output ...
        $Imp = Import-Csv $BatchFile
        $BatchImp = $Imp | Select-Object -First 1

        # set the batch id
        $BatchId = $BatchImp[0].BATCH_ID

        # check for no procs to run 
        if ( ($BatchImp[0].REPORT_NAME) -eq 'NULL') { $NoRunCheck = 1 } else { $NoRunCheck = 0 }

    }



    if ($NoRunCheck -eq 0) {



        Write-Host ""
        Write-Host "Today's Run"
        Write-Host "--------------------------------------------------------"

        $Imp | Format-Table

        Write-Host ""
        Write-Host ""


        # write todays run to the log
        $Imp | ForEach-Object { 
            
            MainLogRecord -Section $Section -Type P -Entry $_ -Suppress T

        }


        if ($AutoPilot -eq "T") { $Choice = "Yes" }
        else {

            $Choice = read-host "Enter ""Yes"" and to continue to run, else enter ""No"" to exit ..."
            Write-Host ""

        }

        if ($Choice -eq "No") { 
        
            MainLogRecord -Section $Section -Entry "Chose to exit the script. Exiting."

            EndProcess

            Exit 
        
        }


    }
    else {
        
        MainLogRecord -Section $Section -Entry ("BatchId:" + $BatchId)
        MainLogRecord -Section $Section -Entry "There was nothing to run today."

        ProgressInd -100Pct
        
        EndProcess

        Exit

    }







}






#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 4. Pre-sql PowerShell commands



if ($RunPrePS -eq "T") {




    #--------------------------------------------------------------------------
    # generate and run the pre-sql PowerShell script ...

    $Section = "Pre-sql PowerShell"
    
    ProgressInd


    Write-Host ""
    Write-Host ""
    Write-Host ""
    Write-Host ""
    MainLogRecord -Section $Section -Entry $Section
    Write-Host "-----------------------------------------------------------------------"
    Write-Host "-----------------------------------------------------------------------"
    Write-Host ""



    #--------------------------------------------------------------------------
    # run the script 


    RunSQL  -SQLText $CallPrePS `
            -NotificationsEmailList $NotificationsEmailList `
            -Description "Generating the pre-sql PowerShell commands"



    #--------------------------------------------------------------------------
    # check for either the PowerShell script and run it or 
    # the "No PowerShell commands" file



    if (Test-Path $PrePS) {

        Write-Host ""
        MainLogRecord -Section $Section -Entry "The pre-sql Powershell file has been generated ..."


        if ($AutoPilot -eq "T") { $Choice = "Yes" }
        else { $Choice = read-host "Enter ""Yes"" and to continue to run the script, else enter ""No"" to exit ..."}
        
        Write-Host ""

        if ($Choice -eq "Yes") {


            MainLogRecord -Section $Section -Entry "Running the pre-sql PowerShell script ..."

            & $PrePS

            CheckLogError
       
        
        } else { 
        
            MainLogRecord -Section $Section -Entry "Chose to exit the script. Exiting."

            EndProcess

            Exit 
        
        }


    }
    elseif (Test-Path $NoPrePS) {

        MainLogRecord -Section $Section -Entry "There are no pre-sql PowerShell commands to run, continuing ..."

    }


    


    #--------------------------------------------------------------------------
    # Update the log table



    RunSQL  -SQLText $UpdatePrePS `
            -NotificationsEmailList $NotificationsEmailList `
            -Description "Updating the pre-sql PowerShell log entry"


    
        






}




#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 5. Generate and run SQL script



if ($RunSQL -eq "T") {


    $Section = "Main SQL"

    ProgressInd


    Write-Host ""
    Write-Host ""
    Write-Host ""
    Write-Host ""
    MainLogRecord -Section $Section -Entry $Section
    Write-Host "-----------------------------------------------------------------------"
    Write-Host "-----------------------------------------------------------------------"
    Write-Host ""



    #--------------------------------------------------------------------------
    # generate the main sql script


    RunSQL  -SQLText $CallScrptWrtrText `
            -NotificationsEmailList $NotificationsEmailList `
            -Description "Generating the main sql script"




	 if (Test-Path $NoMainSQLFile) {

        MainLogRecord -Section $Section -Entry "There is no main sql script to run"
        
   } else  {    


    
    MainLogRecord -Section $Section -Entry "The main sql script has been generated ..."



    #--------------------------------------------------------------------------
    # remove old files and run the main sql script


    if ($AutoPilot -eq "T") { $Choice = "Yes" }
    else { $Choice = read-host "Enter ""Yes"" and to continue to run the script, else enter ""No"" to exit ..." }

    if ($Choice -eq "Yes") {
        

        MainLogRecord -Section $Section -Entry "Removing the old scheduled run information ..."
        if (test-path $SchedRunFile) {Remove-Item $SchedRunFile}



        # run the script ...
        RunSQL  -SQLText (Get-Content $SQLScript) `
                -NotificationsEmailList $NotificationsEmailList `
                -Description "Running the main sql script"



        } else { 
        
            MainLogRecord -Section $Section -Entry "Chose to exit the script. Exiting."

            EndProcess

            Exit 
        
        }




     }   











}








#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 6. Post-SQL PowerShell






if ($RunPostPS -eq "T") {

    
    $Section = "Post-sql PowerShell"
    
    ProgressInd

    
    Write-Host ""
    Write-Host ""
    Write-Host ""
    Write-Host ""
    MainLogRecord -Section $Section -Entry $Section
    Write-Host "-----------------------------------------------------------------------"
    Write-Host "-----------------------------------------------------------------------"
    Write-Host ""


    #--------------------------------------------------------------------------
    # generate the post-sql PowerShell file



    RunSQL  -SQLText $CallPostPS `
            -NotificationsEmailList $NotificationsEmailList `
            -Description "Generating the post-sql PowerShell script"




    #--------------------------------------------------------------------------
    # check for either the PowerShell file or the "No PowerShell 
    # commands to run" file


    if (Test-Path $NoPS) {

        MainLogRecord -Section $Section -Entry "There are no post-sql Powershell commands to run ..."

    }
    elseif (Test-Path $PostPS) {

        #if the post-sql file shows up, run it ...
        MainLogRecord -Section $Section -Entry "The post-sql PowerShell file has been generated ..."


        if ($AutoPilot -eq "T") { $Choice = "Yes" }
        else { $Choice = read-host "Enter ""Yes"" and to continue to run the post-sql PowerShell script, else enter ""No"" to exit and run manually ..." }

        if ($Choice -eq "Yes") {

            MainLogRecord -Section $Section -Entry "Running the post-sql PowerShell Script"

            & $PostPS

            CheckLogError

        } else { 
        
            MainLogRecord -Section $Section -Entry "Chose to exit the script. Exiting."

            EndProcess

            Exit 
        
        }

    }







    # Update the log to show successfull completion

    RunSQL  -SQLText $UpdatePostPS `
            -NotificationsEmailList $NotificationsEmailList `
            -Description "Updating the post-sql PowerShell log entry"


    







}






#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------
# 7. Wrap up and completion email

$Section = "Wrap Up"

ProgressInd

Write-Host ""
Write-Host ""
Write-Host ""
Write-Host ""
MainLogRecord -Section $Section -Entry $Section
Write-Host "-----------------------------------------------------------------------"
Write-Host "-----------------------------------------------------------------------"
Write-Host ""




EndProcess



Write-Host ""
Write-Host "---------------------------------------------------------------------------------"
Write-Host "The reports are run for today ... :)"
Write-Host "---------------------------------------------------------------------------------"
Write-Host ""














}






catch {

    MainLogRecord -Section $Section -Type A -Status E -Entry ( $_ )
    CheckLogError

}
