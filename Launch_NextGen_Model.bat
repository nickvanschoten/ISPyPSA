@echo off
echo Opening WSL2 and Launching ISPyPSA...
wsl ~ -e bash -c "cd $(wslpath '%cd%') && ./setup_and_run.sh"
pause