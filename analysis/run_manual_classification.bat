REM ***Change character mapping to UNICODE***
chcp 65001

REM ***Copy paragraph files from BoxSync to EDGAR-Research Folder***
xcopy /s/y "C:\Users\mbs013300\Box Sync\EDGAR Team\paragraphs\*.csv" "C:\Users\mbs013300\git\EDGAR-research\data\*.csv"

REM UnREM this line to test whether unicode is working *** python -c "print(chr(254))"

REM ***Run manual_classification.py program***
python manual_classification.py

REM ***pause, important if there is an error so the command window doesn't close***
pause