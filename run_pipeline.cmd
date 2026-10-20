@echo off
REM Lanzador para Windows: doble clic en este fichero ejecuta run_pipeline.ps1
REM (equivale a: powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1)
powershell -NoProfile -ExecutionPolicy Bypass -NoExit -File "%~dp0run_pipeline.ps1"
