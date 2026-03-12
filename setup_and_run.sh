#!/bin/bash

# --- NextGen ISPyPSA Automator ---
echo "🚀 Initializing NextGen Energy Model Environment..."

# 1. Check for Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Installing..."
    sudo apt update && sudo apt install -y python3 python3-pip python3-venv
else
    echo "✅ Python3 detected."
fi

# 2. Setup Virtual Environment
if [ ! -d ".venv_wsl" ]; then
    echo "📦 Creating virtual environment (.venv_wsl)..."
    python3 -m venv .venv_wsl
fi
source .venv_wsl/bin/activate

# 3. Install/Update Dependencies
echo "📥 Installing dependencies (this may take a minute)..."
pip install --upgrade pip
pip install -r requirements.txt
pip install streamlit plotly pyarrow pydantic  # Ensuring MVP specific tools are there

# 4. Gurobi Verification
if ! command -v gurobi_cl &> /dev/null; then
    echo "⚠️  Gurobi CMD not found in PATH. Please ensure Gurobi is installed in WSL."
    echo "👉 If you have a key, run: grbgetkey YOUR-KEY-HERE"
else
    echo "✅ Gurobi detected."
fi

# 5. Data & Run Selection
echo "------------------------------------------------"
echo "Select an action:"
echo "1) Run Full Optimization Pipeline (Phase 4.5)"
echo "2) Launch Visualization Dashboard (Streamlit)"
echo "3) Both (Run Optimizer then Launch GUI)"
echo "4) Exit"
read -p "Enter choice [1-4]: " choice

case $choice in
    1) python3 src/ispypsa/nextgen/runners/phase4_5_runner.py --config tests/data/testbed_config.yaml ;;
    2) streamlit run src/ispypsa/nextgen/gui/app.py ;;
    3) 
       python3 src/ispypsa/nextgen/runners/phase4_5_runner.py --config tests/data/testbed_config.yaml
       streamlit run src/ispypsa/nextgen/gui/app.py 
       ;;
    4) exit ;;
esac