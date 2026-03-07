@echo off
setlocal EnableDelayedExpansion
cd /d %~dp0
REM Inicia minimizado
start "" /min python watch_tsv_to_json_bg.py
exit /b 0
