"""Render the measured burst timelines (results/timeline-*.csv) to docs/autoscale.svg. Stdlib only."""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
W, H, L, R, T, B = 900, 360, 70, 70, 40, 50
T_MAX = 90.0


def load(name: str) -> list[dict[str, float]]:
    with open(ROOT / "results" / name, encoding="utf-8") as fh:
        return [{k: float(v) if v else 0.0 for k, v in row.items()} for row in csv.DictReader(fh)]


def main() -> None:
    keda, one = load("timeline-burstkeda.csv"), load("timeline-burst1.csv")
    sc = json.loads((ROOT / "results" / "bench.json").read_text())["scenarios"]
    lag_max = max(r["lag"] for r in keda + one) * 1.05
    rep_max = 6.5

    def x(t: float) -> float:
        return L + (W - L - R) * min(t, T_MAX) / T_MAX

    def y_lag(v: float) -> float:
        return H - B - (H - T - B) * v / lag_max

    def y_rep(v: float) -> float:
        return H - B - (H - T - B) * v / rep_max

    def path(rows: list[dict[str, float]], key: str, fy, step: bool = False) -> str:
        pts = [r for r in rows if 0 <= r["t"] <= T_MAX]
        out, prev = [], None
        for r in pts:
            if step and prev is not None:
                out.append(f"L{x(r['t']):.1f},{fy(prev):.1f}")
            out.append(f"{'M' if not out else 'L'}{x(r['t']):.1f},{fy(r[key]):.1f}")
            prev = r[key]
        return " ".join(out)

    title = "1.2M-event burst: consumer lag and processor replicas (measured, 1 s samples)"
    s = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="ui-monospace,Menlo,monospace" font-size="12">',
        f'<rect width="{W}" height="{H}" fill="#0d1117"/>',
        f'<text x="{L}" y="24" fill="#e6edf3" font-size="14">{title}</text>',
    ]
    for i in range(0, 7):
        yy = y_rep(i)
        s.append(f'<line x1="{L}" x2="{W - R}" y1="{yy:.1f}" y2="{yy:.1f}" stroke="#21262d"/>')
        s.append(f'<text x="{W - R + 8}" y="{yy + 4:.1f}" fill="#a371f7">{i}</text>')
    for v in range(0, int(lag_max), 200_000):
        s.append(f'<text x="{L - 8}" y="{y_lag(v) + 4:.1f}" fill="#8b949e" text-anchor="end">{v // 1000}k</text>')
    for t in range(0, int(T_MAX) + 1, 10):
        s.append(f'<text x="{x(t):.1f}" y="{H - B + 18}" fill="#8b949e" text-anchor="middle">{t}s</text>')
    s.append(f'<path d="{path(one, "lag", y_lag)}" fill="none" stroke="#f0883e" stroke-width="2" stroke-dasharray="5 4"/>')
    s.append(f'<path d="{path(keda, "lag", y_lag)}" fill="none" stroke="#3fb950" stroke-width="2"/>')
    s.append(f'<path d="{path(keda, "ready", y_rep, step=True)}" fill="none" stroke="#a371f7" stroke-width="2"/>')
    b1, bk = sc["burst1"]["drain_seconds"], sc["burstkeda"]["drain_seconds"]
    ly = H - 14
    s.append(f'<text x="{L}" y="{ly}" fill="#f0883e">-- lag, 1 replica pinned (drained {b1:.1f}s)</text>')
    s.append(f'<text x="{L + 330}" y="{ly}" fill="#3fb950">-- lag, KEDA (drained {bk:.1f}s)</text>')
    s.append(f'<text x="{L + 590}" y="{ly}" fill="#a371f7">-- ready replicas (KEDA)</text>')
    s.append("</svg>")
    out = ROOT / "docs" / "autoscale.svg"
    out.write_text("\n".join(s), encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
