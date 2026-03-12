import sys
import os
import faulthandler

faulthandler.enable()
faulthandler.dump_traceback_later(timeout=15, exit=True)

print("Starting linopy import...", flush=True)
os.environ["NUMEXPR_MAX_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

try:
    from ispypsa.nextgen.config.manager import DeepMergeConfigManager
    print("DeepMergeConfigManager imported successfully!", flush=True)
except Exception as e:
    print(f"Error importing linopy: {e}", flush=True)
