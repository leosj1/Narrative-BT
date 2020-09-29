call C:/Users/seun/anaconda3/Scripts/activate.bat C:\Users\seun\anaconda3\
cd C:\Narrative-BT
REM call conda activate base

set CUR_YYYY=%date:~10,4%
set CUR_MM=%date:~4,2%
set CUR_DD=%date:~7,2%
REM set CUR_TT=%time:~0,8%
REM set SUBFILENAME=%CUR_MM%-%CUR_DD%-%CUR_YYYY%_%CUR_TT:~0,2%_%CUR_TT:~3,2%_%CUR_TT:~6,2%

set SUBFILENAME=%CUR_MM%-%CUR_DD%-%CUR_YYYY%

echo ------------------------------------START------------------------------------------ >> C:\Narrative-BT\Logs\Log_Tracker_Narratives_%SUBFILENAME%.log
echo %date%-%time% >> C:\Narrative-BT\Logs\Log_Tracker_Narratives_%SUBFILENAME%.log
"C:\Users\seun\anaconda3\python.exe" "C:\Narrative-BT\Narratives.py" echo %DATE% %TIME% >> C:\Narrative-BT\Logs\Log_Tracker_Narratives_%SUBFILENAME%.log 2>&1
echo %date%-%time% >> C:\Narrative-BT\Logs\Log_Tracker_Narratives_%SUBFILENAME%.log
echo ------------------------------------END------------------------------------------ >> C:\Narrative-BT\Logs\Log_Tracker_Narratives_%SUBFILENAME%.log