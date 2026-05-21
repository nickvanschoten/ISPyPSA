"""Verify Gurobi works through the same PyPSA path ISPyPSA uses.

Builds a 100-row x 200-col control LP through linopy + Gurobi, mirroring the
discipline used in prior addenda before running production-scale solves.
"""

import time

import linopy
import numpy as np
import pandas as pd


def main():
    rng = np.random.default_rng(42)
    n_vars = 200
    n_cons = 100
    A = rng.standard_normal((n_cons, n_vars))
    b = rng.standard_normal(n_cons)
    c = rng.standard_normal(n_vars)

    m = linopy.Model()
    x = m.add_variables(lower=-10.0, upper=10.0, coords=[pd.Index(range(n_vars), name="i")], name="x")
    A_da = pd.DataFrame(A, index=pd.Index(range(n_cons), name="j"),
                        columns=pd.Index(range(n_vars), name="i")).stack().rename("A").to_xarray().fillna(0)
    rhs = pd.Series(b, index=pd.Index(range(n_cons), name="j")).to_xarray()
    m.add_constraints((A_da * x).sum("i") >= rhs, name="ineq")
    obj = (pd.Series(c, index=pd.Index(range(n_vars), name="i")).to_xarray() * x).sum()
    m.add_objective(obj, sense="min")
    print(f"Built LP: {n_cons} rows, {n_vars} cols")

    t = time.perf_counter()
    m.solve(solver_name="gurobi")
    wall = time.perf_counter() - t
    print(f"Solve wall: {wall:.2f}s")
    print(f"Status: {m.status}")
    print(f"Objective: {m.objective.value:.6f}")


if __name__ == "__main__":
    main()
