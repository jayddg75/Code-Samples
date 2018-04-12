#----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
# this function does the progress tracking for the main script

function ProgressInd (
$100Pct 
)
{

    $Global:PI ++

    if ($100Pct -eq $null) { $PP = [int](($PI/$PT) * 100) } else { $PP = 100 }
 
    Write-Progress -Id 1 -Activity "Main PowerShell Script" -Status "$PP% Complete" -CurrentOperation $Section -PercentComplete $PP 


}



#----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
# put all the end process steps in one place

function EndProcess (


[ValidateSet("T", "F")]
[String] $NoEmail,

[ValidateSet("T", "F")]
[String] $Error

)
 {


    # check for any notifications and send an email out if there are any ...

    MainLogRecord -Section $Section -Entry "Checking log for notifications"

    $Notes = Import-Csv $MainLog -Delimiter "|" | Where Status -eq 'Notification' 

    if ($Notes -ne $null) {
        

        MainLogRecord -Section $Section -Entry "Sending out notifications"

        generate-htmlemail -FileasHTML ($Notes | ConvertTo-Html -Fragment)  -To $NotificationsEmailList -Subject $NoteSubjLine -FontSize 80


    } else {

        MainLogRecord -Section $Section -Entry "There were no notifications to send out"

    }



    if ($Error -ne "T") { 

        # back up the log table
        RunSQL  -SQLText $LogTableBackupText `
                -NotificationsEmailList $NotificationsEmailList `
                -Description "Backing up log table ..."



        BackUpOprFiles -BackupFldr $BckUpFldr -OprFldrPath $OprFldr -OprFldrName $OprName

    }


    MaintLogFile


    if ($NoEmail -ne "T") {

        generate-htmlemail -FilePath $OprFldr -FileName $ScheduleName -To $NotificationsEmailList -Subject $GoodSubjLine -FontSize 80

    }





}










#----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
# manage the log files

function MaintLogFile {


    $FileTimeStamp = get-date -UFormat "%Y%m%d%H%M"
    $NewMainLog = ($HistLogFldr + ($BatchId + "M_" + $FileTimeStamp + ".txt" ))
    $NewAuxLog = ($HistLogFldr + ($BatchId + "A_" + $FileTimeStamp + ".txt"))


    if (Test-Path $MainLog) { 
    
        Move-Item -Path $MainLog -Destination $NewMainLog
    }
        


    if (Test-Path $AuxLog ) { 
    
        Move-Item -Path $AuxLog -Destination $NewAuxLog 

    }


    # get the lowest number batch Id from the log table backup
    $LBI = Import-Csv -Path $LogBkpFile -Delimiter "|" | Select @{Name = "BatchIds"; Expression = {[int]$_.Batch_Id}} | Where-Object BatchIds -NE 0 | Sort-Object BatchIds | select -First 1

    $LowestBatchId = $LBI[0].BatchIds



    # Only keep log files for batches that are on the log table in Netezza
    Get-ChildItem $HistLogFldr | ForEach-Object {

        $SeperatorPos =  $_.Name.IndexOf("_")

        $HistBatchId =   [int]$_.Name.Substring(0, $SeperatorPos - 1)


       if ($HistBatchId -lt $LowestBatchId ) {

            Remove-Item ($_.FullName)
            
       }

    }



    # Last Log Entries
    MainLogRecord -Section $Section -File $NewMainLog -Entry "Maintaining log table backup files"
    MainLogRecord -Section $Section -File $NewMainLog -Entry $GoodSubjLine -Suppress T
    MainLogRecord -Section $Section -File $NewMainLog -Entry $AutomationEnd -Suppress T


    









}




#----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
# This function will search for log errors and return a flag

function CheckLogError 
{


    MainLogRecord -Section $Section -Entry "Checking log for errors"

    # import any records on the log that have an error status
    $LogErr = Import-Csv $MainLog -Delimiter "|" | Where Status -eq 'Error'


    # if there are any errors, send an email and stop everything
    if ($LogErr -ne $null) {

        MainLogRecord -Section $Section -Entry "Found errors in the log, exiting process"

        generate-emailAttachment -To $NotificationsEmailList -Subject $ErrSubjLine -AttachPath $MainLog, $AuxLog
        
        EndProcess -NoEmail T -Error T

        Exit
    
    } 
    


 }




#----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
# This function will insert records into the main log


function MainLogRecord (
$Section,
$Type,
$Status,
$Entry,
$Suppress,
$File
)
{

    $Global:LogId ++   

    if ($Type -eq "S") { $TypeEntry = "Executed SQL"} 
    elseif ($Type -eq "P") { $TypeEntry = "Batch" }
    elseif ($Type -eq "R") { $TypeEntry = "Run Status" }
    elseif ($Type -eq "A") { $TypeEntry = "PowerShell Script" }
    else { $TypeEntry = "MileStone" }

    if ($Status -eq "E") { $StatusEntry = "Error"} 
    elseif ($Status -eq "N") {$StatusEntry = "Notification"} 
    else { $StatusEntry = "Normal" } 
    
    [String]$TimeStamp = Get-Date -Format G


    $LogEntry = [Convert]::ToString($LogId) + "|" + $TimeStamp + "|" + $Section + "|" + $TypeEntry + "|" + $StatusEntry + "|" + $Entry

    if ($File -eq $null) { $WriteFile = $MainLog } else { $WriteFile = $File }


    # Write the log entry
    Add-Content -Path $WriteFile -Value $LogEntry


    # Write out the message, if it is not suppressed
    if ($Suppress -ne "T") { 

        Write-Host $Entry
        Write-Host ""

    }

}








#----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
# This function will execute a sql script against Netezza

function RunSQL (
$SQLText,
$Description,
$NotificationsEmailList
)
{

    


    # -------------------------------------------------------------------------------------
    # remove old files and write out the script to run


    # remove any old successfull run files and log files ...
    if (test-path $SQLSuccess) {remove-item $SQLSuccess}
    if (test-path $OutPutFile) {Remove-Item $OutPutFile}
    if (test-path $ErrorFile) {Remove-Item $ErrorFile}


    # put together the file to run
    $SQLText + $AppendText > $SQLRunFile

    MainLogRecord -Section $Section -Type S -Entry $Description
    

    # write out the about to be executed sql to the aux log file
    Add-Content -Path $AuxLog  -Value ($SQLText + $AppendText)




    # -------------------------------------------------------------------------------------
    # run the script ...


    & $NZ_CMDExec --unattended --stdout $NZ_OutPutFile --stderr $NZ_ErrorFile --description $Description --action exec --connstr $Conn_Strng --dbtype NetezzaODBC --sqlfile $NZ_RunFile



    # -------------------------------------------------------------------------------------
    # wait for the error file to have an error or the success file

    $x = 0
    $LoopFlag = 0


    while ($LoopFlag -eq 0) {


        Start-Sleep -s 1 
        $x++

        Write-Progress -Activity $Description -Status ("$x seconds")


        $ErrFileFlag = if (Test-Path $ErrorFile) { if ((Get-Item $ErrorFile).length -gt 0) {1}{0} }


        #check to see if the error log showed up with problems ...
        if ($ErrFileFlag -eq 1) {

                MainLogRecord -Section $Section -Type S -Status E -Suppress T -Entry (Get-Content $ErrorFile) 
                MainLogRecord -Section $Section -Type S -Status E -Entry "There was a problem running the sql. Check the log for details"

                CheckLogError

        }
        elseif (Test-Path $SQLSuccess) { 
            
            $LoopFlag = 1
             
            MainLogRecord -Section $Section -Type S -Suppress T -Entry (Get-Content $OutPutFile) 
            MainLogRecord -Section $Section -Type S -Suppress T -Entry "SQL ran successfully"
            Write-Host ""

         }



    }


    # wait five seconds to let Aginity wrap up with its log files
    Start-Sleep -s 5


}






#----------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------
# This function will generate an html email from the contents of a CSV



function generate-htmlemail (
$FilePath,
$FileName,
$FileasHTML,
$To,
$BodyText,
$Subject,
$FontSize,
$HeaderFill,
$HeaderFont,
$RowFont 
) 
{


    #set default coloring if none are passed in
    if ($HeaderFill -eq $null) { $HeaderFill = "CadetBlue" }
    if ($HeaderFont -eq $null) { $HeaderFont = "White" }
    if ($RowFont -eq $null) { $RowFont = "DimGray" }
    if ($FontSize -eq $null) { $FontSize = 80 }


    #define HTML tags for formatting
    $TableOpenTag = "<table style=""font-size:$FontSize%;border:1px solid black;border-collapse:collapse;padding:4px"">"
    $DataElement = "<td style =""border:1px solid black;border-collapse:collapse;padding:4px;color:$RowFont"">"
    $HeaderElement = "<th style =""border:1px solid black;border-collapse:collapse;padding:4px;background-color:$HeaderFill;color:$HeaderFont;font-weight:bold"">"


    if ($FilePath -ne $null -and $FileName -ne $null) {

    $CSVFile = $FilePath + $FileName
    $ImportAsHTML = Import-Csv -Path $CSVFile | ConvertTo-Html -Fragment

    } else {

      $ImportAsHTML = $FileasHTML

    }


    $a = $ImportAsHTML -replace "<table>", $TableOpenTag
    $b = $a -replace "<td>", $DataElement
    $c = $b -replace "<th>", $HeaderElement


$Body = @"
$BodyText
$c
"@

    $Outlook = New-Object -com Outlook.Application
    $mail = $Outlook.CreateItem(0)

    $mail.subject = $Subject
    $mail.HTMLBody = $Body
    $mail.To = $To
    $mail.Send()



    MainLogRecord -Section $Section -Entry "Generated an html email"



}




#----------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------
# This function will generate an email with attachments
# Make sure sequential attachments are seperated by a semi-colon


function generate-emailAttachment (
$To,
$Subject,
$Body,
$AttachPath
) 
{





    $Outlook = New-Object -com Outlook.Application
    $mail = $Outlook.CreateItem(0)

    $mail.subject = $Subject
    $mail.body = $Body
    $mail.To = $To
 

    MainLogRecord -Section $Section -Entry "Generating an email"



    if ($AttachPath -ne $null) {


        $AttachPath | ForEach-Object {

            $mail.Attachments.Add($_)
            MainLogRecord -Section $Section -Entry ( "Email attachment: " + $_ )

        }

    }



    $mail.Send()


}





#----------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------
# This function will generate an email with attachments
# Make sure sequential attachments are seperated by a semi-colon


function BackUpOprFiles (
$BackupFldr,
$OprFldrPath,
$OprFldrName
)
{

    MainLogRecord -Section $Section -Entry "Backing up operational folder"

    if (Test-Path $BackupFldr) { Remove-Item ($BackupFldr + "*") -Recurse }
    Copy-Item $OprFldrPath ( $BackupFldr + $OprFldrName + " " + ( Get-Date -UFormat "%Y%m%d" ) ) -Recurse


}