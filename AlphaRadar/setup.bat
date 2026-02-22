@echo off
echo ===================================================
echo AlphaRadar Initial Setup Script
echo ===================================================

echo [1] Creating Python Virtual Environment (venv)...
python -m venv venv

echo [2] Activating Virtual Environment...
call venv\Scripts\activate

echo [3] Upgrading pip...
python -m pip install --upgrade pip

echo [4] Installing Dependencies...
if exist requirements.txt (
    pip install -r requirements.txt
) else (
    pip install .
)

echo ===================================================
echo Setup Complete!
echo You can now run the application by running:
echo     run_alpharadar.bat
echo ===================================================
pause
