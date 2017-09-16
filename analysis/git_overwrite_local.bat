cd C:\Users\mbs013300\git\EDGAR-research
ECHO This script will overwrite all local changes
SET /P input=[Would you like to continue(Y/N)]
IF "%input%"=="N" GOTO Exit
git checkout -- .		
git pull
GOTO End
:Exit
ECHO No changes were made.
:End
ECHO Process is complete. Good bye.
pause