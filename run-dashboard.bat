@echo off
REM Pass 1 power-sector dashboard launcher (Windows).
REM Double-click from File Explorer, or run from any shell.

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo.
    echo ERROR: Virtual environment not found at .venv\Scripts\python.exe
    echo.
    echo Install dependencies first by running from this directory:
    echo     uv sync
    echo.
    pause
    exit /b 1
)

where uv >nul 2>&1
if %errorlevel%==0 (
    uv run streamlit run mvp_pass1_power\dashboard\dashboard.py
) else (
    echo Note: 'uv' not on PATH; launching via the venv directly.
    .venv\Scripts\python.exe -m streamlit run mvp_pass1_power\dashboard\dashboard.py
)

REM Keep the window open after Streamlit exits so any error messages are visible.
pause
