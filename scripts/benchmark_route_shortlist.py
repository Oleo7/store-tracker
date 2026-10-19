"""Offline, synthetic route benchmark; no credentials or network requests.

Uses exhaustive enumeration of at most two stops as an independent oracle.
The larger star-road cases deliberately include geographically sparse pairs.
This establishes no global guarantee for production problems with more stops.
"""
import argparse
import itertools
import json
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "web-app"))
import route_proposal as rp


def star_case(size=50):
    candidates = [rp.RouteCandidate(i+1, f"d{i}", rp.Coordinate(57.7+i*.0001, 11.9),
                  90, commercial_value=90) for i in range(size-2)]
    candidates += [rp.RouteCandidate(size-1, "pair1", rp.Coordinate(59, 13), 70, commercial_value=70),
                   rp.RouteCandidate(size, "pair2", rp.Coordinate(59.001, 13), 70, commercial_value=70)]
    direct = {c.row:100 if c.row <= size-2 else 110 for c in candidates}
    def distance(a, b):
        if a == b:
            return 0
        if a > size-2 and b > size-2:
            return 1
        return direct[a]+direct[b]
    return candidates, direct, distance, 300


def small_case(seed):
    rng = random.Random(seed)
    points = {i:(rng.randrange(1, 100), rng.randrange(1, 100)) for i in range(1, 9)}
    direct = {i:x+y for i,(x,y) in points.items()}
    def distance(a,b):
        return sum(abs(x-y) for x,y in zip(points[a],points[b]))
    candidates = [rp.RouteCandidate(i,str(i),rp.Coordinate(57.7+x*.001,11.9+y*.001),
                  score,commercial_value=score) for i,(x,y) in points.items()
                  for score in [rng.randrange(20,101)]]
    return candidates, direct, distance, 240


def benchmark(case):
    candidates, direct, distance, budget = case
    start = time.perf_counter()
    selected = rp.shortlist_candidates(start=rp.Coordinate(57.7,11.9),candidates=candidates,
                                       direct_seconds=direct,service_seconds_per_stop=20)
    matrix = [[direct[c.row] for c in selected]] + [[distance(c.row,d.row) for d in selected] for c in selected]
    solution = rp.solve_route(selected,matrix,return_seconds=[direct[c.row] for c in selected],
                             max_total_seconds=budget,service_seconds_per_stop=20,max_route_stops=2)
    elapsed = time.perf_counter()-start
    chosen = [selected[i] for i in solution.route_indices]
    def evaluate(route):
        if not route:
            return (0,0)
        drive = direct[route[0].row]+direct[route[-1].row]+sum(distance(a.row,b.row) for a,b in zip(route,route[1:]))
        return (sum(c.objective_value for c in route),drive) if drive+20*len(route)<=budget else (-1,0)
    oracle = max((evaluate(p) for length in [0,1,2] for p in itertools.permutations(candidates,length)),
                 key=lambda item:(item[0],-item[1]))
    score, drive = evaluate(chosen)
    return {"candidates":len(candidates),"shortlist":len(selected),"score":score,"drive_seconds":drive,
            "oracle_score":oracle[0],"oracle_drive_seconds":oracle[1],
            "score_gap":oracle[0]-score,"secondary_gap_seconds":drive-oracle[1] if score==oracle[0] else None,
            "solver_seconds":round(elapsed,6),"oracle_scope":"all candidates, at most two stops"}


if __name__ == "__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--output");args=parser.parse_args()
    results={"small":[benchmark(small_case(seed)) for seed in range(12)],
             "star":[benchmark(star_case(n)) for n in [50,100,500]]}
    payload=json.dumps(results,indent=2)
    if args.output:
        Path(args.output).write_text(payload,encoding="utf-8")
    print(payload)
