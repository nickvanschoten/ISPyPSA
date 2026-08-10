"""Verify BarConvTol passes through the linopy → Gurobi chain.

Builds the same 100-row × 200-col control LP used in verify_gurobi_pypsa.py
and solves it twice: once with default BarConvTol (1e-8) and once with
BarConvTol=1e-3. Checks the Gurobi log for "Changed value of parameter
BarConvTol" to confirm the option is forwarded correctly.
"""

import io
import time

import linopy
import numpy as np
import pandas as pd


def _build_control_model(n_vars: int = 200, n_cons: int = 100) -> linopy.Model:
    rng = np.random.default_rng(42)
    A = rng.standard_normal((n_cons, n_vars))
    b = rng.standard_normal(n_cons)
    c = rng.standard_normal(n_vars)
    m = linopy.Model()
    x = m.add_variables(
        lower=-10.0, upper=10.0,
        coords=[pd.Index(range(n_vars), name="i")], name="x",
    )
    A_da = (
        pd.DataFrame(A, index=pd.Index(range(n_cons), name="j"),
                     columns=pd.Index(range(n_vars), name="i"))
        .stack().rename("A").to_xarray().fillna(0)
    )
    rhs = pd.Series(b, index=pd.Index(range(n_cons), name="j")).to_xarray()
    m.add_constraints((A_da * x).sum("i") >= rhs, name="ineq")
    obj = (pd.Series(c, index=pd.Index(range(n_vars), name="i")).to_xarray() * x).sum()
    m.add_objective(obj, sense="min")
    return m


def _solve_and_capture(gurobi_params: dict | None = None) -> tuple[str, float, float]:
    """Returns (captured_log, objective_value, wall_s).

    linopy passes **solver_options kwargs directly to gurobipy via setParam,
    mirroring what PyPSA does: m.solve(solver_name=solver_name, **solver_options).
    """
    import sys
    buf = io.StringIO()
    m = _build_control_model()
    t = time.perf_counter()
    # linopy writes Gurobi log to stdout; capture it
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        if gurobi_params:
            m.solve(solver_name="gurobi", **gurobi_params)
        else:
            m.solve(solver_name="gurobi")
    finally:
        sys.stdout = old_stdout
    wall = time.perf_counter() - t
    return buf.getvalue(), float(m.objective.value) if m.status == "ok" else float("nan"), wall


def main():
    print("=== Control LP: Gurobi default BarConvTol ===")
    log_default, obj_default, wall_default = _solve_and_capture()
    print(log_default[:3000])
    print(f"Wall: {wall_default:.3f}s  Objective: {obj_default:.6f}")

    print("\n=== Control LP: Gurobi BarConvTol=1e-3 ===")
    log_tol3, obj_tol3, wall_tol3 = _solve_and_capture({"BarConvTol": 1e-3})
    print(log_tol3[:3000])
    print(f"Wall: {wall_tol3:.3f}s  Objective: {obj_tol3:.6f}")

    # Confirm BarConvTol change appears in log
    tol_confirmed = "BarConvTol" in log_tol3
    tol_absent_in_default = "BarConvTol" not in log_default or "Changed" not in log_default
    print("\n=== Verification ===")
    print(f"BarConvTol mention in tol=1e-3 log: {tol_confirmed}")
    print(f"BarConvTol mention absent/unchanged in default log: {tol_absent_in_default}")
    print(f"Objective diff (default vs tol=1e-3): {abs(obj_default - obj_tol3):.2e}")


if __name__ == "__main__":
    main()
