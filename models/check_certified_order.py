"""Independent finite-state check of certified-order continuation.

The transition applies the certified rule: closed safe closure when no batch is
open; aged Solid roots open a batch; Solid members absorb active in-neighbors;
only Solid members execute, after every potential fairness predecessor
P(u) = {v : W(v,u) >= q_h + c(u) - n} has executed, unless the two lie in one
strongly connected component of the support graph (W >= T_e).

Properties are checked against the full correct receipt profiles, never
against the evidence graph used by the transition:
  * assignment prefix: when y executes and x precedes y for >= q_h correct
    replicas, x has executed in a batch <= b(y) or is assigned to y's open batch;
  * physical order: any such pair executed out of order lies in one
    weak-precedence component (threshold C - q_h + 1 over correct profiles);
  * progress: once every correct log is consumed, the target set executes
    even if Byzantine senders stop reporting.
The checker also evaluates an identifier-ordered reference rule, in which
Shaded members execute on absorption, to exercise the physical-order oracle.
"""
from __future__ import annotations

import argparse
import json
import time
from copy import deepcopy
from dataclasses import dataclass, field
from fractions import Fraction
from itertools import combinations, permutations, product
from pathlib import Path


def ceil_fraction(value: Fraction) -> int:
    return -(-value.numerator // value.denominator)


def scc(nodes, edges):
    """Tarjan over an explicit edge set restricted to nodes; returns id->component."""
    index, low, on, stack, comp = {}, {}, set(), [], {}
    counter = [0]

    def visit(u):
        index[u] = low[u] = counter[0]
        counter[0] += 1
        stack.append(u)
        on.add(u)
        for a, b in edges:
            if a != u or b not in nodes:
                continue
            if b not in index:
                visit(b)
                low[u] = min(low[u], low[b])
            elif b in on:
                low[u] = min(low[u], index[b])
        if low[u] == index[u]:
            while True:
                v = stack.pop()
                on.discard(v)
                comp[v] = u
                if v == u:
                    break

    for u in sorted(nodes):
        if u not in index:
            visit(u)
    return comp


@dataclass
class Model:
    reports: tuple
    faults: int
    gamma: Fraction
    delay: int
    strict_edges: bool = True
    legacy: bool = False
    prefix: tuple = ()
    sequence: int = 0
    batch_counter: int = 0
    executed: dict = field(default_factory=dict)   # id -> (batch, seq, ordinal)
    assigned: dict = field(default_factory=dict)   # open member -> batch
    open_index: int | None = None
    solid_since: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.prefix:
            self.prefix = (0,) * len(self.reports)
        assert 2 * self.qh >= self.n + 2 * self.faults + 1

    n = property(lambda self: len(self.reports))
    qh = property(lambda self: ceil_fraction(self.gamma * (self.n - self.faults)))
    threshold = property(lambda self: self.n - self.qh + 1)

    @property
    def correct(self):
        return self.reports[self.faults:]

    @property
    def target(self):
        return set().union(*(set(log) for log in self.correct))

    def derive(self):
        positions = [{u: i for i, u in enumerate(log[:size])} for log, size in zip(self.reports, self.prefix)]
        live = set().union(*(set(p) for p in positions)) - set(self.executed)
        count = {u: sum(u in p for p in positions) for u in live}
        weight = {}
        for u, v in permutations(sorted(live), 2):
            weight[u, v] = sum(u in p and v in p and p[u] < p[v] for p in positions)
        active = {u for u in live if count[u] >= self.threshold}
        solid = {u for u in active if count[u] >= self.n - 2 * self.faults}
        edges = set()
        for u, v in combinations(sorted(active), 2):
            uv, vu = weight[u, v], weight[v, u]
            if uv >= self.threshold and (uv > vu or (not self.strict_edges and uv == vu)):
                edges.add((u, v))
            elif vu >= self.threshold and vu > uv:
                edges.add((v, u))
        return live, count, weight, active, solid, edges

    def rounds(self, members, count, weight, active, solid):
        if self.legacy:
            # Pre-fix rule: every fresh member executes on absorption, in the
            # SCC/Kahn order of the current edges with identifier tie-breaks.
            fresh = {m for m in members if m not in self.assigned}
            edges = {(a, b) for a, b in combinations(sorted(active), 2) for a, b in ((a, b), (b, a))
                     if weight[a, b] >= self.threshold and (weight[a, b] > weight[b, a] or (not self.strict_edges and weight[a, b] == weight[b, a] and a < b))}
            comp = scc(fresh, edges)
            groups = {}
            for u in fresh:
                groups.setdefault(comp[u], set()).add(u)
            remaining, out = list(groups.values()), []
            while remaining:
                ready = [g for g in remaining if not any(a not in g and b in g and any(a in h for h in remaining) for a, b in edges)]
                chosen = min(ready, key=min)
                out.append(sorted(chosen))
                remaining.remove(chosen)
            return out
        need = lambda u: max(1, self.qh + count[u] - self.n)
        preds = {u: {v for v in active if v != u and weight[v, u] >= need(u)} for u in members if u in solid}
        domain = set(members).union(*preds.values()) if preds else set(members)
        support = {(a, b) for a in domain for b in domain if a != b and weight[a, b] >= self.threshold}
        comp = scc(domain, support)
        done, out = set(), []
        while True:
            ready = sorted(u for u, ps in preds.items() if u not in done and all(v in done or comp[v] == comp[u] for v in ps))
            if not ready:
                return out
            done |= set(ready)
            out.append(ready)

    def step(self, selected, increments):
        m = deepcopy(self)
        prefix = list(m.prefix)
        for i, amount in zip(selected, increments):
            prefix[i] += amount
        m.prefix, m.sequence = tuple(prefix), m.sequence + 1
        live, count, weight, active, solid, edges = m.derive()
        for u in solid:
            m.solid_since.setdefault(u, m.sequence)
        ordinal = 0
        if m.open_index is None:
            blocked = active - solid
            while True:
                grown = blocked | {v for u, v in edges if u in blocked}
                if grown == blocked:
                    break
                blocked = grown
            safe = solid - blocked
            comp = scc(safe, edges)
            groups = {}
            for u in safe:
                groups.setdefault(comp[u], set()).add(u)
            remaining = list(groups.values())
            while remaining:
                ready = [g for g in remaining if not any(a not in g and b in g and any(a in h for h in remaining) for a, b in edges)]
                chosen = min(ready, key=min)
                m.batch_counter += 1
                for u in sorted(chosen):
                    ordinal += 1
                    m.executed[u] = (m.batch_counter, m.sequence, ordinal)
                remaining.remove(chosen)
            live, count, weight, active, solid, edges = m.derive()
        roots = {u for u in solid if u in m.assigned or m.sequence - m.solid_since[u] >= m.delay}
        if m.open_index is None and roots:
            m.batch_counter += 1
            m.open_index = m.batch_counter
        if m.open_index is not None:
            members = set(m.assigned) | roots
            while True:
                grown = members | {u for u, v in edges if v in members and v in solid}
                if grown == members:
                    break
                members = grown
            for r in m.rounds(members, count, weight, active, solid):
                for u in r:
                    ordinal += 1
                    m.executed[u] = (m.open_index, m.sequence, ordinal)
                    m.assigned.pop(u, None)
            for u in members:
                if u not in m.executed:
                    m.assigned[u] = m.open_index
            if not m.assigned:
                m.open_index = None
        return m

    def key(self):
        ages = tuple(sorted((u, min(self.delay, self.sequence - s)) for u, s in self.solid_since.items() if u not in self.executed))
        return (self.prefix, self.batch_counter, tuple(sorted(self.executed.items())), tuple(sorted(self.assigned.items())), self.open_index, ages)


def oracle(model):
    correct = [{u: i for i, u in enumerate(log)} for log in model.correct]
    c, qh = len(correct), model.qh
    support = lambda x, y: sum(p[x] < p[y] for p in correct)
    target = sorted(model.target)
    fair = [(x, y) for x, y in permutations(target, 2) if support(x, y) >= qh]
    weak = {(x, y) for x, y in permutations(target, 2) if support(x, y) >= c - qh + 1}
    return fair, scc(set(target), weak)


def violations(model, fair, weak_comp):
    # The legacy rule kept executed Shaded members unsettled, so only its
    # physical order is comparable; the revised rule is checked in full.
    for x, y in fair:
        if y not in model.executed:
            continue
        by = model.executed[y]
        if x in model.executed:
            bx = model.executed[x]
            if bx[0] > by[0] and not model.legacy:
                return ("assignment-prefix", x, y)
            if bx[1:] > by[1:] and weak_comp[x] != weak_comp[y]:
                return ("cross-component physical inversion", x, y)
        else:
            if model.assigned.get(x) != by[0] and not model.legacy:
                return ("assignment-prefix", x, y)
            if weak_comp[x] != weak_comp[y]:
                return ("cross-component physical inversion", x, y)
    return None


def drain(model, f):
    """With every correct log consumed and Byzantine senders silent, run idle
    fragments; the target must execute (finite-input progress)."""
    state = model
    for _ in range(state.delay + len(state.target) + 3):
        if state.target <= set(state.executed):
            return True
        state = state.step(tuple(range(f, state.n)), (0,) * (state.n - f))
    return state.target <= set(state.executed)


def exhaustive(profiles, f, gammas, delays, strict, legacy, limit=None):
    totals = {"profiles_parameters": 0, "states": 0, "transitions": 0, "drain_checks": 0}
    first = None
    for reports in profiles:
        for gamma, delay in product(gammas, delays):
            initial = Model(reports, f, gamma, delay, strict, legacy)
            fair, weak_comp = oracle(initial)
            queue, seen = [initial], {initial.key()}
            totals["profiles_parameters"] += 1
            while queue:
                state = queue.pop()
                totals["states"] += 1
                if all(state.prefix[i] == len(reports[i]) for i in range(f, state.n)):
                    totals["drain_checks"] += 1
                    if not drain(state, f) and first is None:
                        first = {"failure": "progress", "reports": reports, "gamma": str(gamma), "D": delay, "prefix": state.prefix}
                    if state.target <= set(state.executed):
                        continue
                for selected in combinations(range(state.n), state.n - f):
                    choices = []
                    for i in selected:
                        remaining = len(reports[i]) - state.prefix[i]
                        lowest = 0 if i < f or remaining == 0 else 1
                        choices.append(tuple(range(lowest, min(2, remaining) + 1)) if remaining else (0,))
                    for increments in product(*choices):
                        successor = state.step(selected, increments)
                        totals["transitions"] += 1
                        bad = violations(successor, fair, weak_comp)
                        if bad and first is None:
                            first = {"failure": bad[0], "x": bad[1], "y": bad[2], "reports": reports, "gamma": str(gamma), "D": delay,
                                     "prefix": successor.prefix, "executed": {str(k): v for k, v in successor.executed.items()}}
                        if bad and legacy:
                            continue
                        if bad:
                            return totals, first
                        k = successor.key()
                        if k not in seen:
                            seen.add(k)
                            queue.append(successor)
                if limit and totals["states"] >= limit:
                    return totals, first
    return totals, first


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--four", action="store_true", help="add the four-transaction n=3 family")
    args = parser.parse_args()
    report = {"scope": "independent model of the certified-order continuation rule; not the Go implementation or a general proof", "runs": []}
    three = [tuple(p) for p in product(tuple(permutations(range(3))), repeat=3)]
    two = tuple(permutations(range(2)))
    five = [(byz,) + logs for byz in ((), (0, 1), (1, 0)) for logs in product(two, repeat=4)]
    families = [("n3_f0_three_tx_all_profiles", three, 0, (Fraction(1), Fraction(2, 3)), (0, 1, 2)),
                ("n5_f1_two_tx_all_profiles", five, 1, (Fraction(1),), (0, 1, 2))]
    if args.four:
        identity = (0, 1, 2, 3)
        four = [(identity, a, b) for a, b in product(tuple(permutations(range(4))), repeat=2)]
        families.append(("n3_f0_four_tx_first_log_fixed", four, 0, (Fraction(2, 3), Fraction(1)), (0, 2)))
    for legacy in (True, False):
        for name, profiles, f, gammas, delays in families:
            for strict in (True, False):
                start = time.monotonic()
                totals, first = exhaustive(profiles, f, gammas, delays, strict, legacy)
                row = {"rule": "legacy-shaded-execution" if legacy else "certified-order", "family": name,
                       "edge_policy": "strict" if strict else "id_ties", **totals, "first_violation": first,
                       "wall_seconds": round(time.monotonic() - start, 1)}
                report["runs"].append(row)
                args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
                print(json.dumps({k: v for k, v in row.items() if k != "first_violation"} | {"violation": bool(first)}), flush=True)
                if legacy:
                    break


if __name__ == "__main__":
    main()
