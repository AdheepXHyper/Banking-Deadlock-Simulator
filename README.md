# Banking Database Deadlock Simulator, Detector & Recovery Engine

A Python simulation and DBA diagnostic toolkit modeling a real-world banking database deadlock involving concurrent ATM withdrawals, ledger updates, and audit logging[cite: 2, 3, 12].

## Overview
When concurrent database transactions acquire exclusive locks in conflicting orders, a **circular wait** halts transaction processing[cite: 3, 7]. `banking_deadlock_project.py` implements three core phases[cite: 12]:

1. **Deadlock Reproduction, Detection & Recovery:**
   * Simulates transactions `T1`, `T2`, and `T3` forming a deadlock cycle across `R1_Account_A`, `R2_Ledger`, and `R3_Audit_Log`, while `T4` is indirectly blocked outside the cycle[cite: 3, 6, 12].
   * Builds a **Resource-Allocation Graph (RAG)** and a simplified **Wait-For Graph (WFG)**, using Depth-First Search (DFS) to detect cycles[cite: 5, 6, 7, 12].
   * Selects the cheapest victim (`T3`) based on work done, rolls back its locks to preserve ACID atomicity, retries idempotently, and verifies ledger debit/credit consistency[cite: 8, 10, 12].
2. **Deadlock Prevention (Global Lock Ordering):**
   * Enforces a strict acquisition hierarchy (`Account -> Ledger -> Audit Log`) to break the **Circular Wait** Coffman condition[cite: 3, 7, 12].
3. **Production DBA SQL Diagnostics:**
   * Includes ready-to-run PostgreSQL (`pg_stat_activity`, `pg_locks`) and MySQL InnoDB (`data_lock_waits`) queries for inspecting live lock contention and terminating victim sessions[cite: 4, 5, 9, 12].

## Quick Start
```bash
git clone [https://github.com/AdheepXHyper/Banking-Deadlock-Simulator.git](https://github.com/AdheepXHyper/Banking-Deadlock-Simulator.git)
cd Banking-Deadlock-Simulator
python3 banking_deadlock_project.py
