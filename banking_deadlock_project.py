"""
================================================================================
BANKING DATABASE DEADLOCK SIMULATOR, DETECTOR, RECOVERY & PREVENTION ENGINE
================================================================================
Based on:
- Coffman Conditions (Mutual Exclusion, Hold & Wait, No Preemption, Circular Wait)
- Resource-Allocation Graphs (RAG) & Wait-For Graphs (WFG)
- Depth-First Search (DFS) Cycle Detection & Cheapest-Victim Rollback
- Global Lock Ordering Prevention & Production SQL Diagnostic Queries

How to Run:
    python3 banking_deadlock_project.py
================================================================================
"""

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple


# ==============================================================================
# 1. DATA MODELS (RESOURCES & TRANSACTIONS)
# ==============================================================================

@dataclass
class Resource:
    name: str
    owner: Optional[str] = None
    waiters: List[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class Transaction:
    tx_id: str
    priority: int  # Lower number = higher priority
    work_done: int = 0
    held_resources: Set[str] = field(default_factory=set)
    waiting_for: Optional[str] = None
    status: str = "RUNNING"  # RUNNING, BLOCKED, COMMITTED, ABORTED


# ==============================================================================
# 2. LOCK MANAGER (RAG/WFG BUILDER, DFS DETECTOR, RECOVERY & PREVENTION)
# ==============================================================================

class LockManager:
    """Manages database locks, constructs RAG/WFG, detects cycles, and recovers."""

    def __init__(self, resources: List[str], global_order: Optional[Dict[str, int]] = None):
        self.resources: Dict[str, Resource] = {name: Resource(name) for name in resources}
        self.transactions: Dict[str, Transaction] = {}
        self.global_order = global_order  # Enforced when deadlock prevention is active
        self._meta_lock = threading.Lock()

    def register_transaction(self, tx: Transaction):
        with self._meta_lock:
            self.transactions[tx.tx_id] = tx

    def acquire(self, tx_id: str, res_name: str) -> bool:
        """Attempts to acquire an exclusive lock on a resource for a transaction."""
        with self._meta_lock:
            tx = self.transactions[tx_id]
            res = self.resources[res_name]

            if tx.status == "ABORTED":
                return False

            # PREVENTION: Enforce Global Lock Ordering (breaks Circular Wait)
            if self.global_order and tx.held_resources:
                max_held_rank = max(self.global_order[r] for r in tx.held_resources)
                requested_rank = self.global_order[res_name]
                if requested_rank <= max_held_rank:
                    raise RuntimeError(
                        f"[PREVENTION VIOLATION] {tx_id} tried to lock {res_name} "
                        f"(rank {requested_rank}) while holding higher/equal rank {max_held_rank}."
                    )

            if res.owner is None:
                res.owner = tx_id
                tx.held_resources.add(res_name)
                tx.waiting_for = None
                tx.status = "RUNNING"
                print(f"[LOCK GRANTED] {tx_id} acquired exclusive lock on {res_name}")
                return True
            else:
                if tx_id not in res.waiters:
                    res.waiters.append(tx_id)
                tx.waiting_for = res_name
                tx.status = "BLOCKED"
                print(f"[BLOCKED] {tx_id} is waiting for {res_name} (held by {res.owner})")

        # Spin-wait until lock becomes available or transaction is aborted as a victim
        while True:
            time.sleep(0.1)
            with self._meta_lock:
                if tx.status == "ABORTED":
                    if tx_id in res.waiters:
                        res.waiters.remove(tx_id)
                    tx.waiting_for = None
                    return False

                if res.owner is None:
                    if tx_id in res.waiters:
                        res.waiters.remove(tx_id)
                    res.owner = tx_id
                    tx.held_resources.add(res_name)
                    tx.waiting_for = None
                    tx.status = "RUNNING"
                    print(f"[LOCK GRANTED] {tx_id} acquired {res_name} after waiting")
                    return True

    def release_all(self, tx_id: str):
        """Releases all locks held by a transaction upon commit or rollback."""
        with self._meta_lock:
            tx = self.transactions[tx_id]
            for res_name in list(tx.held_resources):
                res = self.resources[res_name]
                if res.owner == tx_id:
                    res.owner = None
                    print(f"[LOCK RELEASED] {tx_id} released {res_name}")
            tx.held_resources.clear()
            tx.waiting_for = None

    def build_graphs(self) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """Builds both the Resource-Allocation Graph (RAG) and Wait-For Graph (WFG)."""
        rag = defaultdict(list)
        wfg = defaultdict(list)

        with self._meta_lock:
            # Assignment edges: Resource -> Transaction
            for res_name, res in self.resources.items():
                if res.owner:
                    rag[res_name].append(res.owner)

            # Request edges: Transaction -> Resource, and WFG: Tx_waiter -> Tx_owner
            for tx_id, tx in self.transactions.items():
                if tx.waiting_for and tx.status == "BLOCKED":
                    res_name = tx.waiting_for
                    rag[tx_id].append(res_name)
                    owner = self.resources[res_name].owner
                    if owner:
                        wfg[tx_id].append(owner)

        return dict(rag), dict(wfg)

    def detect_deadlock_cycle(self) -> Optional[List[str]]:
        """Uses Depth-First Search (DFS) on the Wait-For Graph to detect a cycle."""
        _, wfg = self.build_graphs()
        visited = set()
        rec_stack = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.append(node)

            for neighbor in wfg.get(node, []):
                if neighbor not in visited:
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    cycle_start = rec_stack.index(neighbor)
                    return rec_stack[cycle_start:] + [neighbor]

            rec_stack.pop()
            return None

        for tx_node in list(wfg.keys()):
            if tx_node not in visited:
                cycle = dfs(tx_node)
                if cycle:
                    return cycle
        return None

    def recover_from_deadlock(self, cycle: List[str]) -> str:
        """Selects the cheapest victim on the cycle, aborts it, and rolls back its locks."""
        unique_cycle_txs = list(dict.fromkeys(cycle[:-1]))
        with self._meta_lock:
            candidates = [self.transactions[tx_id] for tx_id in unique_cycle_txs]
            # Victim criteria: least work done, fewest locks held, lowest priority (highest number)
            victim = min(
                candidates,
                key=lambda t: (t.work_done, len(t.held_resources), -t.priority),
            )
            victim.status = "ABORTED"
            print(f"\n[RECOVERY] Victim selected: {victim.tx_id} (work_done={victim.work_done})")

        self.release_all(victim.tx_id)
        return victim.tx_id


# ==============================================================================
# 3. BANKING DATABASE & ACID CONSISTENCY VERIFIER
# ==============================================================================

class BankLedgerDB:
    """Simulates banking tables and verifies ACID post-recovery consistency."""

    def __init__(self):
        self.savings_account_A = 1000.00
        self.ledger_total_debits = 0.00
        self.ledger_total_credits = 0.00
        self.audit_logs: List[str] = []

    def verify_consistency(self):
        print("\n--- POST-INCIDENT CONSISTENCY & LIVENESS VERIFICATION ---")
        print(f"Account A Balance    : ${self.savings_account_A:.2f}")
        print(f"Ledger Debits/Credits: ${self.ledger_total_debits:.2f} / ${self.ledger_total_credits:.2f}")
        print(f"Audit Log Entries    : {self.audit_logs}")
        assert self.ledger_total_debits == self.ledger_total_credits, "ACID Violation: Ledger unbalanced!"
        print("[VERIFIED] Ledger debits equal credits. No partial updates remain.")


# ==============================================================================
# 4. PRODUCTION SQL DIAGNOSTIC QUERIES (POSTGRESQL & MYSQL INNODB)
# ==============================================================================

SQL_DIAGNOSTICS = """
-- 1. POSTGRESQL: Identify blocked sessions, wait duration, and blocking PIDs
SELECT
    activity.pid AS blocked_pid,
    activity.usename AS user,
    NOW() - activity.query_start AS wait_duration,
    activity.wait_event_type,
    activity.wait_event,
    pg_blocking_pids(activity.pid) AS blocking_pids,
    activity.query AS blocked_query
FROM pg_stat_activity activity
WHERE cardinality(pg_blocking_pids(activity.pid)) > 0
ORDER BY wait_duration DESC;

-- 2. POSTGRESQL: Inspect Lock Table (Holders vs. Waiters for RAG/WFG construction)
SELECT
    l.locktype,
    l.relation::regclass AS table_name,
    l.mode,
    l.granted,
    l.pid,
    a.query
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE NOT l.granted
   OR l.pid IN (SELECT unnest(pg_blocking_pids(pid)) FROM pg_stat_activity);

-- 3. POSTGRESQL: Terminate chosen victim session safely
-- SELECT pg_terminate_backend(<victim_pid>);

-- 4. MYSQL (InnoDB): Inspect active lock waits and latest deadlock report
SELECT
    r.trx_id AS waiting_trx_id,
    r.trx_mysql_thread_id AS waiting_thread,
    r.trx_query AS waiting_query,
    b.trx_id AS blocking_trx_id,
    b.trx_mysql_thread_id AS blocking_thread,
    b.trx_query AS blocking_query
FROM performance_schema.data_lock_waits w
INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_engine_transaction_id
INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_engine_transaction_id;
"""


# ==============================================================================
# 5. SIMULATION RUNNERS
# ==============================================================================

def run_deadlock_scenario():
    print("=================================================================")
    print("PHASE 1: REPRODUCING THE BANKING DEADLOCK SCENARIO (T1, T2, T3, T4)")
    print("=================================================================")

    lm = LockManager(["R1_Account_A", "R2_Ledger", "R3_Audit_Log"])
    db = BankLedgerDB()

    t1 = Transaction("T1", priority=1, work_done=30)
    t2 = Transaction("T2", priority=2, work_done=20)
    t3 = Transaction("T3", priority=3, work_done=10)  # Least work -> ideal victim
    t4 = Transaction("T4", priority=2, work_done=5)   # Blocked behind T1, not in cycle

    for tx in (t1, t2, t3, t4):
        lm.register_transaction(tx)

    def worker_t1():
        if lm.acquire("T1", "R1_Account_A"):
            time.sleep(0.3)
            if lm.acquire("T1", "R2_Ledger"):
                db.savings_account_A -= 100
                db.ledger_total_debits += 100
                db.ledger_total_credits += 100
                db.audit_logs.append("T1: Withdraw $100")
                t1.status = "COMMITTED"
                lm.release_all("T1")

    def worker_t2():
        if lm.acquire("T2", "R2_Ledger"):
            time.sleep(0.3)
            if lm.acquire("T2", "R3_Audit_Log"):
                db.audit_logs.append("T2: Ledger Sync")
                t2.status = "COMMITTED"
                lm.release_all("T2")

    def worker_t3():
        if lm.acquire("T3", "R3_Audit_Log"):
            time.sleep(0.3)
            if lm.acquire("T3", "R1_Account_A"):
                db.audit_logs.append("T3: Audit Check")
                t3.status = "COMMITTED"
                lm.release_all("T3")
            else:
                print("[ROLLBACK] T3 aborted cleanly; retrying idempotently after cycle clears...")
                time.sleep(0.5)
                if lm.acquire("T3", "R3_Audit_Log") and lm.acquire("T3", "R1_Account_A"):
                    db.audit_logs.append("T3 (Retried): Audit Check")
                    t3.status = "COMMITTED"
                    lm.release_all("T3")

    def worker_t4():
        time.sleep(0.1)
        if lm.acquire("T4", "R1_Account_A"):
            db.audit_logs.append("T4: Balance Enquiry")
            t4.status = "COMMITTED"
            lm.release_all("T4")

    threads = [
        threading.Thread(target=worker_t1),
        threading.Thread(target=worker_t2),
        threading.Thread(target=worker_t3),
        threading.Thread(target=worker_t4),
    ]
    for th in threads:
        th.start()

    # Wait for deadlock to form
    time.sleep(0.8)

    print("\n--- OBSERVING & ANALYSING GRAPHS ---")
    rag, wfg = lm.build_graphs()
    print(f"Resource-Allocation Graph (RAG): {rag}")
    print(f"Wait-For Graph (WFG)           : {wfg}")

    cycle = lm.detect_deadlock_cycle()
    if cycle:
        print(f"[DEADLOCK CONFIRMED] Cycle detected: {' -> '.join(cycle)}")
        print("Note: T4 is blocked waiting on R1, but is NOT part of the cycle.")
        lm.recover_from_deadlock(cycle)

    for th in threads:
        th.join()

    db.verify_consistency()


def run_prevention_demo():
    print("\n=================================================================")
    print("PHASE 2: DEADLOCK PREVENTION VIA GLOBAL LOCK ORDERING")
    print("=================================================================")
    # Strict order: R1_Account_A (1) -> R2_Ledger (2) -> R3_Audit_Log (3)
    order = {"R1_Account_A": 1, "R2_Ledger": 2, "R3_Audit_Log": 3}
    lm = LockManager(["R1_Account_A", "R2_Ledger", "R3_Audit_Log"], global_order=order)

    t1 = Transaction("T1_Ordered", priority=1)
    t3 = Transaction("T3_Ordered", priority=1)
    lm.register_transaction(t1)
    lm.register_transaction(t3)

    def ordered_tx(tx_id: str):
        # Every transaction must acquire resources in ascending global rank
        lm.acquire(tx_id, "R1_Account_A")
        lm.acquire(tx_id, "R2_Ledger")
        lm.acquire(tx_id, "R3_Audit_Log")
        lm.release_all(tx_id)
        print(f"[SUCCESS] {tx_id} completed without circular wait.")

    th1 = threading.Thread(target=ordered_tx, args=("T1_Ordered",))
    th2 = threading.Thread(target=ordered_tx, args=("T3_Ordered",))
    th1.start()
    th2.start()
    th1.join()
    th2.join()


def show_sql_diagnostics():
    print("\n=================================================================")
    print("PHASE 3: PRODUCTION DBA DIAGNOSTIC QUERIES (POSTGRESQL & MYSQL)")
    print("=================================================================")
    print(SQL_DIAGNOSTICS.strip())


if __name__ == "__main__":
    run_deadlock_scenario()
    run_prevention_demo()
    show_sql_diagnostics()
