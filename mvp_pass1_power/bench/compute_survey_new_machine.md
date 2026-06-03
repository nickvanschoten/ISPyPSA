# Compute survey — new machine (Optimus-NC)

Date: 2026-05-28
Surveyor: characterisation snapshot for LP/parallelism planning.
Method: Win32 CIM queries, SysInternals coreinfo, py-cpuinfo via the
project venv, live Gurobi licence check-out. No benchmarks beyond a
single sequential disk-write probe.

This document is purely characterisation. It does **not** recommend what
to do with the machine — that is a separate planning conversation
informed by the bottlenecks documented across Phase 1–7.

---

## 1. Headline

| | Previous bench (Dell Precision 5490) | New machine (Dell PowerEdge R940xa, host `Optimus-NC`) |
|---|---|---|
| Class | Mobile workstation | 4-socket rack server |
| CPU | 1× Intel Core Ultra 7 165H (Meteor Lake-H) | 4× Intel Xeon Platinum 8280L (Cascade Lake-SP) |
| Physical cores | 16 (6P + 8E + 2LP-E) | **112** (4 × 28) |
| Logical processors | 22 | **224** (4 × 56, HT on) |
| Base / max turbo | 1.4 GHz P / 4.8 GHz P-turbo | 2.7 GHz base / 4.0 GHz turbo (per Intel ARK) |
| SIMD | AVX2, FMA (no AVX-512) | **AVX-512** F/BW/CD/DQ/VL + VNNI, AVX2, FMA |
| RAM | 64 GiB | **~3.07 TiB** (48× 64 GiB DDR4 ECC RDIMM @ 2933 MT/s) |
| Memory channels | 2 (LPDDR5x) | 6 per socket × 4 sockets = 24 channels, fully populated 2 DIMMs/channel |
| NUMA | UMA (single socket) | **4 NUMA nodes** (one per socket) |
| Storage | 1× NVMe (laptop SSD) | RAID volumes: C: 3.84 TB, D: **19.2 TB** (DELL PERC S140 RAID controller) |
| OS | Windows 11 | Windows Server 2022 Standard (build 20348) |
| Sharing | Single-user laptop | **Multi-user shared workstation** (≥25 CSIRO user homes on D:\) |
| Remote access | local console | RDP (`TermService`) + WinRM running; **no sshd** |
| Gurobi | not installed | **Gurobi 11.0.3** with floating licence to CSIRO token server |

Crude scale ratios (new vs previous):
**7× physical cores, 10× logical, 48× RAM, 300× local disk.** The
machine class is fundamentally different — this is a node from a
shared analytics server, not a workstation.

---

## 2. CPU

### Model

```
Intel(R) Xeon(R) Platinum 8280L CPU @ 2.70GHz
Family 6, Model 85, Stepping 7   (Cascade Lake-SP, Skylake-X derivative)
4 sockets populated, each at slot CPU1..CPU4
```

The 8280L is the high-memory variant of the Platinum 8280 (Cascade
Lake-SP, 2019 release). Per-socket spec: 28 cores, 56 threads, 2.7 GHz
base, 4.0 GHz max turbo, 38.5 MB L3, supports up to 4.5 TB DDR4 per
socket — that headroom is why this machine can carry 3 TB total.

### Counts and topology

- 4 sockets × 28 physical cores = **112 physical cores**
- Hyperthreading **on** → 224 logical processors
- Reported as **4 Windows processor groups** of 56 logical procs each
  (`GetActiveProcessorGroupCount`)
- **4 NUMA nodes** (one per socket; `GetNumaHighestNodeNumber` returned 3)

**Windows-specific caveat:** the four-processor-group layout matters
for parallel Python. A default Python process on Windows only sees the
processor group it was launched into (max 64 logical processors per
group; here 56), so single-process parallelism (`multiprocessing`,
linopy/HiGHS internal threading) is capped at one socket's worth of
threads unless processor affinity is explicitly set per-group. Running
multiple separate processes pinned to different groups is the natural
way to use the whole machine.

### Caches

- L1 per core: 32 KB D + 32 KB I (Skylake-X standard, not directly
  reported by Windows but architecturally fixed for this stepping)
- L2 per core: 1 MB (28 MB per socket as reported)
- L3 per socket: 38.5 MB shared (~1.375 MB/core, non-inclusive)
- **Aggregate L3 across the box: ~154 MB**

### SIMD

py-cpuinfo flag set on this CPU:

| Family | Flags present |
|---|---|
| **AVX-512** | `avx512f`, `avx512bw`, `avx512cd`, `avx512dq`, `avx512vl`, `avx512vnni` |
| AVX/AVX2/FMA | `avx`, `avx2`, `fma` |
| Other | SSE family, BMI, etc. (full set: 78 flags) |
| **Not present** | AMX (Sapphire Rapids+ only), AVX-512 IFMA/VBMI/BF16, no SGX, no CET |

**Relevance:** the previous bench had no AVX-512. HiGHS, Gurobi 11, and
modern NumPy/SciPy LAPACK kernels can use AVX-512. Whether they help on
this workload is empirically unclear (HiGHS simplex is largely
control-flow-bound; PDLP's matrix-multiply loops are the most likely
beneficiary), but the capability is available.

### Live load at survey time

- All 4 sockets present, healthy
- `Win32_PageFileUsage`: 437 GB pagefile allocated, current usage 20 GB,
  **peak 413 GB** — somebody has previously pushed close to full physical
  memory. Not unusual for a shared scientific compute box, but
  evidence the machine sees genuinely-large workloads from other users.

---

## 3. Memory

- Installed: **48 × 64 GB DDR4 RDIMM** (Micron 36ASF8G72PZ-3G2E1)
  = **3.07 TiB raw** (3,296,887,173,120 bytes reported by
  `Win32_ComputerSystem.TotalPhysicalMemory`)
- DIMM nominal speed: 3200 MT/s; configured at **2933 MT/s**
  (Cascade Lake IMC max with 2 DIMMs/channel populated)
- Layout: banks A1..A12, B1..B12, C1..C12, D1..D12 — i.e. 12 DIMMs per
  socket = fully populated 6 channels × 2 DIMMs/channel per socket
  → memory bandwidth balanced across all four sockets

**Visible to OS:** 3219 GiB (`TotalVisibleMemorySize`), so ~50 GiB lost
to firmware/RAID reservations. Normal for a 4-socket Cascade Lake
config.

### NUMA topology and current free memory

```
NUMA node 0:  337.1 GiB available
NUMA node 1:  668.3 GiB available
NUMA node 2:  383.6 GiB available
NUMA node 3:  258.6 GiB available
-------------
Total free:  1647.6 GiB (≈ 51% of installed RAM free right now)
```

That means **~1.5 TiB is currently allocated** by other workloads on the
box. This is a shared machine and the available-memory picture moves
during the day; the survey reflects 2026-05-28 ~mid-day local. For
planning purposes the floor to assume is "at least 1 TB usually
available," not "3 TB always available."

### Memory bandwidth — not measured

I did not run a STREAM/STREAM-equivalent benchmark — that would
take longer than the time budget allows and would compete with whatever
else is running. For reference, 8280L per-socket peak DDR4-2933
bandwidth is ~140 GB/s, so the box has a theoretical ~560 GB/s
aggregate across four sockets — but cross-socket NUMA accesses pay the
UPI hop, so single-process bandwidth is bounded by the local socket
(~140 GB/s) unless the workload is explicitly NUMA-aware.

---

## 4. Storage

| Volume | Type | Size | Free | Role |
|---|---|---|---|---|
| `C:\` (system) | RAID via DELL PERC S140 (3.84 TB physical) | 3.49 TiB | 400 GiB | OS + user profiles |
| `D:\` (data) | RAID via DELL PERC S140 (19.2 TB physical) | 17.46 TiB | 1.80 TiB | shared user data, software installs |
| Page file | `C:\pagefile.sys` | 437 GB allocated | — | peak 413 GB observed |

D:\ root is **admin-write-only** — non-admin users place data in their
home directory `D:\<user>\`. There are 20+ user homes already
established (e.g. `D:\gre538\`, `D:\spl004\`). This account doesn't
have one yet — would need to either request `D:\van538\` from the box
admin or use a writeable subdirectory under `C:\Users\van538\`.

### Disk throughput (rough single-shot)

Sequential write to D:\, 500 MB in 4 MB chunks: **~1.78 GB/s**.
(The read-back number is meaningless — cached in RAM, returned
83 GB/s.) For comparison, the previous bench's IASR-cache + filter
operation took ~160 s, suggesting a workload where disk is unlikely
to be the binding constraint at the 1.7 GB/s sequential rate now
available. The RAID controller is a software RAID (`DELL PERC S140`,
the chipset-tier option), not a hardware-cache-accelerated RAID; random
IOPS on this volume is probably modest compared with a single NVMe.
**Not benchmarked.**

---

## 5. OS and toolchain

- **OS:** Microsoft Windows Server 2022 Standard, build 20348
  (last reboot 2026-02-17, so ~100 days uptime)
- **Hostname / FQDN:** `Optimus-NC` / `Optimus-NC.nexus.csiro.au`
  (CSIRO Nexus AD domain)
- **Python:** **none on system PATH**; the project venv (`.venv` under
  `C:\Users\van538\GitHub\ISPyPSA`) provisions **CPython 3.12.3** via
  uv-managed cache (uv downloads Python on demand)
- **uv:** 0.9.26 (`C:\Program Files\uv\uv.exe`), system-wide install
- **Conda / Mamba:** not installed
- **Julia:** **not installed anywhere on the box.** The `D:\software\STABLE\`
  folder contains a single `.dat` snapshot — no Julia binary, no
  `Project.toml`, no STABLE source tree. Running STABLE/Dieter.jl on
  this machine would require fresh Julia installation, which is a
  user/group-level decision rather than something already provisioned.
- **GAMS:** **38.2.1** (Feb 2022) installed at `D:\software\GAMS\38\`.
  Licence status not verified.
- **MATLAB:** install root present at `D:\software\MATLAB\` and
  `D:\software\MATLAB Runtime\`; licence (`asc_license_matlab.dat`)
  present at `D:\spl004\`. Not directly relevant to LP work but noting
  for completeness.
- **Coreinfo:** `C:\Program Files\SysInternals\Coreinfo.exe` available
  (requires admin for full topology dump).

---

## 6. Solver availability

| Solver | Status | Detail |
|---|---|---|
| **HiGHS** | ✓ installed (`highspy 1.12.0`) | The current ISPyPSA project venv after `uv sync`. Same major as production. |
| **Gurobi 11.0.3** | ✓ **installed and licensed** | See below — floating licence to CSIRO token server. |
| `gurobipy` Python binding | ✓ `11.0.3` | Imports and solves a trivial LP successfully. |
| CPLEX | ✗ no install detected, no Python binding | |
| Mosek | ✗ no install detected | |
| Xpress (FICO) | ✗ no install detected | |
| COPT | ✗ no install detected | |
| GAMS | ✓ 38.2.1 | Solver gateway only; no native LP. Licence not verified. |
| GLPK | ✗ no install detected | |

### Gurobi licence — important detail

```
Gurobi Optimizer version 11.0.3 build v11.0.3rc0 (win64)
License file: C:\gurobi\gurobi.lic
  TOKENSERVER = sc-license1-cdc.it.csiro.au
  PORT        = 41954
```

This is a **CSIRO-wide floating (token) licence**, not a
single-user node-locked licence. Verification: a 1-variable LP
optimised successfully under the user account (`x = 1.0`, status `2`
= Optimal), so token check-out works for this user. Implications worth
flagging to the team:

- **Shared pool.** Other CSIRO users can hold tokens concurrently;
  Gurobi is not infinitely available. The licence pool size and current
  usage are not exposed by `gurobi_cl --license`; that would need a
  query to the CSIRO Service Catalogue / IM&T licence team.
- **Network dependency.** Optimisation runs require connectivity to
  `sc-license1-cdc.it.csiro.au` for the duration of the solve. If the
  machine loses network connectivity mid-solve, the solve fails. This
  matters for long (multi-hour) runs.
- **No version pinning visible from here.** Gurobi 11 is the installed
  client; the token server may or may not permit older clients. Newer
  Gurobi releases (12.x) would need a separate install and a confirmed
  licence-server compatibility.

This is the most significant new capability vs the previous bench.

### HiGHS version note

`highspy 1.12.0` matches the API the project already uses. The earlier
bench-records mention PDLP-at-1e-3 was developed against HiGHS as
shipped by linopy 0.5.x — same major. No version-shift surprises
expected.

---

## 7. Network and remote access

- **RDP** (`TermService`) running — primary interactive access route.
- **WinRM** running — admin scripting / remote PowerShell available.
- **OpenSSH server** *not* running on this user account (no `sshd`
  service surfaced). Bash-friendly remote workflows would need either
  WSL (presence not verified) or RDP into a PowerShell session.
- **Domain:** `nexus.csiro.au` Active Directory — meaning auth /
  filesystem ACLs are CSIRO-AD-managed, not local. This is why the D:\
  root denied a non-admin write.
- **Output transfer:** standard CSIRO patterns apply — SMB to network
  shares, `scp` from a Linux client to WSL if installed, RDP clipboard
  for small payloads, or just keeping outputs on local D:\ and pulling
  via SMB.
- **Network filesystems:** no UNC paths currently mapped under this
  session (`net use` empty). Whether IM&T-mounted shares would expose
  poor IO is untested — the working pattern would be writing to local
  D:\ during a run, then archiving.

---

## 8. Parallelism characterisation

The constraint stack, ordered by what binds first as concurrent jobs
increase:

1. **Other-user activity** — the box is shared. Current observed
   baseline: ~1.5 TiB RAM in use by other workloads, page-file peak
   413 GB historically. This is unpredictable from one day to the next.
2. **Memory per job** — at previous-bench per-archetype RSS of
   **7–15 GiB**, with ~1.5 TiB free as a planning floor, the
   memory-bound concurrent count is **~100 to 200 jobs**. With ~2.5 TiB
   free (off-peak), more like 170–350 jobs.
3. **Physical CPU cores** — 112 physical cores; oversubscribing with HT
   gives 224 logical procs but HiGHS/LP code tends to be memory-bandwidth
   and branch-bound, not throughput-bound, so 1 process per physical
   core (~100 concurrent jobs) is the natural cap.
4. **Windows processor-group layout** — see §2 caveat. Single-process
   parallelism is capped at 56 logical procs (one socket) unless
   affinity is set explicitly. For *multi-process* parallelism this
   doesn't bind — each process gets assigned to a group at launch.
5. **Solver token availability** (only if using Gurobi) — the floating
   licence pool size determines how many concurrent Gurobi-backed jobs
   are allowed. Unknown from here.

**Planning number for the team:** with 7–15 GB per archetype and the
current shared-machine reality, **~50–100 concurrent archetype-level
jobs** is realistic without monopolising the box. The previous bench
ran 6 concurrent; this is order-of-magnitude (10–15×) more.

The bottleneck class matters separately:

- **Embarrassingly-parallel work** (the 36 single-period LPs from
  Phase 6/7 production; per-archetype runs) — scales close to linearly
  with concurrent jobs up to the memory/CPU floor.
- **Single-LP convergence** (PDLP gap asymptote at 1.5–2e-3 on 4-week
  NEM, simplex degeneracy on v7.4 multi-period) — **does not scale
  with cores at all**. More compute does not reduce a single solver's
  iteration count or improve numerical conditioning. The Gurobi licence
  is the relevant capability here, not the core count.
- **Larger individual LPs** (the parked 8760 dispatch v2 question) —
  scales with **memory** per job and per-job solver memory limit. At
  ~3 TiB total with ~1.5 TiB usually free, an 8760 LP that previously
  ran out of memory at 64 GiB has 20–40× the per-job memory ceiling
  here. Whether it *solves* depends on conditioning, not memory.

---

## 9. Anomalies, opportunities, and things to flag

### Limitations the team should know

- **Shared machine, no isolation.** Other users can saturate CPU or
  RAM unpredictably. Memory and core availability at survey time
  (≈51% RAM free, baseline 617 GB allocated, peak page-file 413 GB)
  suggest this box does see real contention. Production runs scheduled
  during working hours would benefit from a quick `Get-Counter` check
  before launch.
- **No SSH.** Remote workflows are RDP / WinRM-based, not the
  Linux-server pattern. Long-running bench launches are typically done
  inside an RDP session; the session can be disconnected without
  killing the process.
- **No Julia.** STABLE/Dieter.jl integration would require a fresh
  Julia install (BinaryBuilder, juliaup, or manual). Not something
  available today.
- **No conda/pip system-wide.** uv is the only Python tooling. The
  uv-managed venv lacks `pip` by default (verified — had to
  `uv pip install` for py-cpuinfo). This is fine for project-tracked
  dependencies but means ad-hoc packages need uv-aware commands.
- **D:\ write requires per-user subdirectory.** Need an admin to
  provision `D:\van538\` (or equivalent) before storing large
  artefacts off C:.
- **Windows processor groups.** Naive multi-threaded Python is capped
  at 56 logical procs. Worth knowing before tuning solver thread counts.

### Opportunities not present on the previous bench

- **Gurobi 11.0.3 with working floating licence.** This is the most
  consequential single difference vs the previous bench. Conditioning
  problems where PDLP at 1e-3 stalls and HiGHS simplex degenerates
  are exactly the class of LP where commercial solvers historically
  deliver 10–100× speedups; the team now has the licence and the
  binding to run that comparison without procurement. Caveat: shared
  token pool, so it can't be assumed always-available.
- **AVX-512** in numerical code paths (NumPy/SciPy LAPACK, HiGHS PDLP
  matrix kernels, Gurobi 11 simplex/barrier). Unknown speedup on this
  specific workload — empirically untested.
- **Embarrassingly-parallel headroom.** Phase 6/7 wall-clock of 2–3
  days for 36 single-period LPs is compute-bound and parallelisable.
  At 36 jobs running concurrently on this box (each fitting easily in
  RAM at 7–15 GB and getting 2–3 physical cores), wall-clock drops to
  *single-job* wall-clock, modulo solver token contention and
  shared-machine variability — i.e. roughly the cost of one LP, not 36.
- **Memory headroom for 8760-scale LPs.** The 64 GiB ceiling on the
  previous bench is removed; LP size is bounded by solver behaviour,
  not RAM. The 8760 v2 question becomes a solver/conditioning question,
  not a compute one.
- **Disk capacity for archived bench outputs.** 17.5 TiB free on D:\
  means the previously-gitignored `runs/`, `bench/runs/`,
  `bench/runs_myopic/` artefacts can be retained durably across runs
  rather than regenerated, if the team finds that useful.

### Where leverage lives, given documented Phase 1–7 bottlenecks

Not recommending an approach — just noting which bottleneck class each
new capability could in principle address:

| Bottleneck (from prior phases) | Compute relationship | New capability that addresses it |
|---|---|---|
| Phase 6/7 wall-clock 2–3 days for 36 LPs | embarrassingly parallel | core count (10× logical, ~5–10× concurrency in practice) |
| PDLP gap asymptote ~1.5–2e-3 on 4-week NEM | solver-bound, not compute | **Gurobi** (different algorithm class) |
| HiGHS simplex degeneracy on v7.4 multi-period | conditioning, not compute | **Gurobi** (better degeneracy handling) — *empirically untested* |
| 8760 dispatch infeasibility at production scale | mixed — likely conditioning; possibly memory | **Memory** (3 TiB vs 64 GiB) + **Gurobi** if conditioning-limited |
| IASR cache load ~160 s | disk + serial deserialisation | RAID 1.78 GB/s vs prior; ceiling depends on serial code path, not raw bandwidth |

---

## 10. Reproduction

The probes used:

```powershell
# CPU + system + memory
Get-CimInstance Win32_Processor, Win32_ComputerSystem, Win32_OperatingSystem, Win32_PhysicalMemory

# SIMD (project venv)
uv run --project C:\Users\van538\GitHub\ISPyPSA --no-sync python -c "
import cpuinfo; print(cpuinfo.get_cpu_info()['flags'])"

# NUMA / processor groups (via kernel32)
uv run --project ... python -c "
import ctypes; k = ctypes.windll.kernel32
n = ctypes.c_ulong(0); k.GetNumaHighestNodeNumber(ctypes.byref(n)); print(n.value+1)"

# Storage
Get-PhysicalDisk; Get-Volume; Get-CimInstance Win32_PageFileUsage

# Gurobi licence check
& "D:\software\gurobi\win64\bin\gurobi_cl.exe" --license
uv run --project ... python -c "
import gurobipy as gp; m = gp.Model(); ..."

# Solver Python bindings
uv run --project ... python -c "
for pkg in ['highspy','gurobipy','cplex','xpress','mosek','coptpy']: ..."
```

All probes were read-only except `py-cpuinfo` (installed into the
project venv via `uv pip install`) and a single 500 MB sequential
write to `D:\` (denied; only the cached read returned). No solver
benchmarks were run.
