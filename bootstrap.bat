@echo off
setlocal

echo =======================================================
echo NextGen Model - Windows-Native Environment Setup
echo =======================================================

:: Define a flag to track if we need to install dependencies
set REQUIRES_INSTALL=0

IF NOT EXIST ".venv" (
    echo [INFO] Virtual environment not found. Creating .venv...
    python -m venv .venv
    IF %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to create virtual environment.
        echo Please ensure Python is installed and added to your PATH.
        pause
        exit /b 1
    )
    set REQUIRES_INSTALL=1
)

echo [INFO] Activating .venv...
call .venv\Scripts\activate.bat
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to activate virtual environment.
    pause
    exit /b 1
)

:: Only run pip install if we just created the environment
IF "%REQUIRES_INSTALL%"=="1" (
    echo [INFO] Installing project dependencies...
    python -m pip install --upgrade pip >nul
    pip install -e .[solvers]
    IF %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to install core project dependencies.
        pause
        exit /b 1
    )
    
    :: Explicit safety net for the UI, Data I/O, and Asynchronous Task packages
    pip install pandas streamlit plotly pyarrow pydantic gurobipy celery redis >nul
    echo [SUCCESS] Environment is fully prepped and ready.
) ELSE (
    echo [INFO] Environment already prepped. Skipping installation.
)

:MENU
echo.
echo =======================================================
echo NextGen ISPyPSA - Execution Menu
echo =======================================================
echo 1. Run Standard MGA Multi-Horizon Optimization (Phase 4.5)
echo 2. Run Macroeconomic Soft-Linking IAM Loop (Phase 7)
echo 3. Launch Streamlit Visualization Dashboard
echo 4. Clear results_export\ and luto_io\ Caches
echo 5. Exit
echo =======================================================
set /p choice="Enter your choice (1-5): "

IF "%choice%"=="1" (
    echo.
    echo Running Multi-Horizon Optimization...
    set PYTHONPATH=src
    python src\ispypsa\nextgen\runners\phase4_5_runner.py --config ispypsa_config.yaml
    pause
    goto MENU
)

IF "%choice%"=="2" (
    echo.
    echo Running Macroeconomic Price-Elasticity Convergence Loop...
    set PYTHONPATH=src
    python src\ispypsa\nextgen\runners\phase7_soft_link_runner.py --config ispypsa_config.yaml
    pause
    goto MENU
)

IF "%choice%"=="3" (
    echo.
    echo Launching Streamlit Dashboard...
    echo Binding to 0.0.0.0 for remote network access.
    set PYTHONPATH=src
    python -m streamlit run src\ispypsa\nextgen\gui\app.py --server.address 0.0.0.0
    pause
    goto MENU
)

IF "%choice%"=="4" (
    echo.
    echo Clearing Parquet and CSV Caches...
    IF EXIST "results_export\*.parquet" (
        del /Q results_export\*.parquet
        echo Results cache cleared.
    ) ELSE (
        echo Export cache already empty.
    )
    IF EXIST "luto_io\*.csv" (
        del /Q luto_io\*.csv
        echo LUTO2 cache cleared.
    )
    IF EXIST "iam_io\*.csv" (
        del /Q iam_io\*.csv
        echo IAM cache cleared.
    )
    pause
    goto MENU
)

IF "%choice%"=="5" (
    echo Exiting...
    exit /b 0
)

echo Invalid choice. Please try again.
goto MENU