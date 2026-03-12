# ISPyPSA
[![Continuous Integration and Deployment](https://github.com/Open-ISP/ISPyPSA/actions/workflows/cicd.yml/badge.svg)](https://github.com/Open-ISP/ISPyPSA/actions/workflows/cicd.yml)
[![codecov](https://codecov.io/gh/Open-ISP/ISPyPSA/graph/badge.svg?token=rcEXuQgfOJ)](https://codecov.io/gh/Open-ISP/ISPyPSA)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/Open-ISP/ISPyPSA/main.svg)](https://results.pre-commit.ci/latest/github/Open-ISP/ISPyPSA/main)
[![UV](https://camo.githubusercontent.com/4ab8b0cb96c66d58f1763826bbaa0002c7e4aea0c91721bdda3395b986fe30f2/68747470733a2f2f696d672e736869656c64732e696f2f656e64706f696e743f75726c3d68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f61737472616c2d73682f75762f6d61696e2f6173736574732f62616467652f76302e6a736f6e)](https://github.com/astral-sh/uv)

An open-source capacity expansion modelling tool based on the methodology and assumptions used by the Australian Energy Market Operator (AEMO) to produce their Integrated System Plan (ISP). Built on [PyPSA](https://github.com/pypsa/pypsa).

**This README is a quick reference.** For detailed instructions, tutorials, and API documentation, see the [full documentation](https://open-isp.github.io/ISPyPSA/):

- [Getting Started](https://open-isp.github.io/ISPyPSA/getting_started/) - Installation and first model run
- [Configuration Reference](https://open-isp.github.io/ISPyPSA/config/) - All configuration options
- [CLI Guide](https://open-isp.github.io/ISPyPSA/cli/) - Command line interface details
- [API Reference](https://open-isp.github.io/ISPyPSA/api/) - Python API for custom workflows
- [Workflow Overview](https://open-isp.github.io/ISPyPSA/workflow/) - How the modelling pipeline works

## 🚀 NextGen Soft-Linking Architecture [V1.0]

The ISPyPSA capacity expansion model has been completely upgraded with a bi-directional "soft-linking" architecture. It has transitioned from a rigid engineering tool into a true, multi-vector macroeconomic forecasting engine.

### Core Value Propositions
1.  **The AEMO NEM Topology Scale-Up**: The PyPSA solver no longer builds on empty generic nodes. It is algorithmically seeded with the true geographical scaffolding of the Australian National Electricity Market. It natively initializes 10 AEMO sub-regions, explicit interstate HVAC/HVDC links (e.g., VNI, QNI), and geographically mapped baseline legacy generators equipped with rigid retirement schedules.
2.  **MGA Slack-Constraint Engine**: Formal *Modeling to Generate Alternatives*. You can mathematically bound the linear programming solver to discover structurally different infrastructure paradigms (e.g., maximize Hydrogen pipelines or minimize Gas peakers) strictly within a predefined cost slack penalty over the global NPV optimum.
3.  **Macroeconomic Equilibrium (IAM Linkage)**: PyPSA is decoupled from inelastic demand streams. Using `iam_exchange.py`, the solver calculates Load-Weighted System Marginal Prices (SMP) per multi-horizon investment period and passes them outward. A built-in rudimentary model (or an external connected CGE/IAM) returns structural demand downscaling targets. An under-relaxed convergence loop ensures PyPSA iterates until physical supply costs and economic demand responses reach market equilibrium.
4.  **External Spatial Opportunity Costs (LUTO2 Feedback)**: PyPSA supports dynamic inbound penalizations via `luto_bridge.py`. By attaching a `spatial_penalty_cost` ($/MW) to the network configurations, external high-resolution land-use models (e.g., LUTO2) can force PyPSA's solver to economically abandon regions facing heavy agricultural competition. Capacity targets are serialized cleanly outbound (`[region, year, carrier, capacity_mw]`) to guide 1km² micro-siting externally without leaking PyPSA internals.

---

## 💻 Developer Onboarding & Execution

The NextGen multi-horizon solver is entirely driven by Python executing a modular pipeline, followed by a stateless Streamlit interface.

### 1. Installation & Environment Lockdown
It is highly recommended to isolate this scientific project. `pyproject.toml` strictly locks the capacity expansion physics and visualization engine versions.
```bash
# Clone the NextGen branch of the repository
git clone https://github.com/Open-ISP/ISPyPSA.git

# We recommend using 'uv' or initializing a virtual environment safely located locally (e.g., WSL2 or C:\)
uv venv
source .venv/bin/activate
uv pip install -e .
```

### 2. Configuring a Scenario Payload
You control the MGA sweeps and Spatial Penalties via `ispypsa_config.yaml`.
```yaml
# Example: Bounding solver to minimize gas within 5% elasticity tolerance
testbed:
  scenario_name: "Phase6_StepChange_MGA"
  mga_options:
    slack_epsilon: 0.05
    target_component: 'Generator'
    target_carrier: 'gas'
    target_action: 'minimize'
    # Optional constraints
  nodes: 
    # Example LUTO2 Land-use opportunity cost penalizing Australian agriculture
    - name: "NSW"
      type: "Urban"
      spatial_penalty_cost: 30000.0  
```

### 3. Pipeline Execution
Run the isolated NextGen pipeline orchestrators explicitly from the primary `src` directory to initialize the geographical scale-up and fire the optimization loop.

```bash
# Set PYTHONPATH to include the module
export PYTHONPATH="src"

# Run the standard MGA exploration with regional scaling
python src/ispypsa/nextgen/runners/phase4_5_runner.py --config ispypsa_config.yaml

# OR Run the Macroeconomic Convergence elasticity ping-pong
python src/ispypsa/nextgen/runners/phase7_soft_link_runner.py --config ispypsa_config.yaml
```

The solvers will serialize highly structured outputs natively into the `results_export/` or `luto_io/` Parquet and CSV files exactly upon reaching either mathematical optimum or soft-linked market convergence.

### 4. Interactive Visualization
A purely stateless, analytical frontend instantly renders comparative scenario deltas parsing strictly from the underlying Parquet exports (requires zero PyPSA instantiation).
```bash
python -m streamlit run src/ispypsa/nextgen/gui/app.py
```

---

## Environment & Pathing Hardening (WSL2 Readiness)

> [!WARNING]
> **Windows SMB Network Drive Vulnerability**  
> Running ISPyPSA on a mapped Windows SMB network drive (e.g., `T:\`) may trigger deterministic multi-threading and disk-IO deadlocks when initializing heavy data-science C-extensions.
> Python 3.12 exhibits silent crashes (Exit Code 1) during deep component initialization (such as `pandas` or `pypsa`).
> 
> For stability and high computational performance, the environment **must be executed locally**. We strongly advise running the framework natively via WSL2 (Windows Subsystem for Linux), or directly on a native local file system (e.g. `C:\`).

**Windows-Native Local Execution:**  
For users constrained by Enterprise IT policies, we provide a `bootstrap.bat` script in the project root. To ensure remote stability, run this script from a **local system drive**. It automates virtual environment creation with explicit error trapping, safely installs dependencies, and provides an interactive menu to launch the Multi-Vector Optimization and Visualization Dashboard bound to `0.0.0.0`.

## Contributing
Interested in contributing to the source code or adding macroeconomic linkages? Check out the [contributing instructions](./CONTRIBUTING.md).
Please note that this project is released with a [Code of Conduct](./CONDUCT.md). 

## License
`ispypsa` was created as a part of the [OpenISP project](https://github.com/Open-ISP) and belongs to its original authors. It is licensed under the terms of [GNU GPL-3.0-or-later](LICENSE).
