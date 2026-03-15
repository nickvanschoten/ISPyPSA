"""
Asynchronous Task Worker for PyPSA-AUS Optimization.
Uses Celery to decouple heavy solver calls from the Streamlit UI.
"""
import os
import logging
from celery import Celery
import pypsa
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
app = Celery("pypsa_aus", broker=broker_url, backend=broker_url)

logger = logging.getLogger(__name__)

# --- ThreadPool Fallback (for local dev without Redis) ---
_executor = ThreadPoolExecutor(max_workers=2)
_local_tasks = {}

def run_local_async(payload):
    """Run optimization in a background thread without Celery/Redis."""
    task_id = f"local_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
    _local_tasks[task_id] = {"state": "PROGRESS", "info": {"message": "Initializing local thread..."}}
    
    def _run():
        try:
            res = run_optimization_logic(payload, lambda msg: _update_local(task_id, "PROGRESS", msg))
            _local_tasks[task_id] = {"state": "SUCCESS", "info": res}
        except Exception as e:
            _local_tasks[task_id] = {"state": "FAILURE", "info": str(e)}
            
    _executor.submit(_run)
    return task_id

def _update_local(task_id, state, message):
    _local_tasks[task_id] = {"state": state, "info": {"message": message}}

def get_local_status(task_id):
    return _local_tasks.get(task_id, {"state": "PENDING", "info": {"message": "Task not found"}})

# --- Unified Optimization Logic ---

def run_optimization_logic(scenario_payload: dict, update_func=None):
    """
    Core logic shared between Celery and local threads.
    """
    from ispypsa.nextgen.runners.scenario_orchestrator import run_scenario_pipeline
    
    if update_func: update_func("Building network...")
    
    # Run the full pipeline
    network = run_scenario_pipeline(scenario_payload)
    
    if update_func: update_func("Optimizing...")
    
    # solver_options can be part of payload
    network.optimize(solver_name='gurobi')
    
    if update_func: update_func("Exporting results...")
    
    # Export logic using MGAExportManager
    from ispypsa.nextgen.io.high_frequency_export import MGAExportManager
    exporter = MGAExportManager(output_dir="results_export")
    exporter.export_all(network, scenario_payload["scenario_name"])
    
    return {"status": "SUCCESS", "scenario": scenario_payload["scenario_name"]}

# --- Celery Task ---

@app.task(bind=True)
def run_optimization_task(self, scenario_payload: dict):
    """
    Celery task wrapper.
    """
    try:
        return run_optimization_logic(
            scenario_payload, 
            lambda msg: self.update_state(state='PROGRESS', meta={'message': msg})
        )
    except Exception as e:
        logger.error(f"Celery task failed: {e}")
        self.update_state(state='FAILURE', meta={'message': str(e)})
        raise e
