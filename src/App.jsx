import { useState, useRef } from "react";
import axios from "axios";
import "./App.css";

const S1 = import.meta.env.VITE_S1_URL || "http://localhost:8081";
const S2 = import.meta.env.VITE_S2_URL || "http://localhost:8082";
const S3 = import.meta.env.VITE_S3_URL || "http://localhost:8083";

const COLORS = [
  "#4f46e5",
  "#0891b2",
  "#059669",
  "#d97706",
  "#7c3aed",
  "#dc2626",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export default function App() {
  const [count, setCount] = useState(30);
  const [concA, setConcA] = useState(3);
  const [concB, setConcB] = useState(1);
  const [phase, setPhase] = useState("idle");
  const [progressA, setProgressA] = useState(null);
  const [progressB, setProgressB] = useState(null);
  const [timeA, setTimeA] = useState(null);
  const [timeB, setTimeB] = useState(null);
  const [error, setError] = useState(null);
  const abortRef = useRef(false);
  const [users, setUsers] = useState([]);

  async function fetchUsers() {
    const [s1Res, s3Res] = await Promise.all([
      axios.get(`${S1}/users`),
      axios.get(`${S3}/users`),
    ]);
    const s1Map = {};
    s1Res.data.forEach((u) => (s1Map[u.id] = u.status));
    const merged = s3Res.data.map((u) => ({
      id: u.id,
      name: u.name,
      raw: s1Map[u.id] || "?",
      mapped: u.status,
    }));
    setUsers(merged);
  }

  async function pollUntilDone(setProgress, startTime, total) {
    while (true) {
      if (abortRef.current) throw new Error("Aborted");
      const res = await axios.get(`${S1}/outbox/stats`);
      const published = Number(res.data.PUBLISHED || 0);
      const pending = Number(res.data.PENDING || 0);
      const failed = Number(res.data.FAILED || 0);
      const elapsed = Date.now() - startTime;
      setProgress({ published, pending, failed, total, elapsed });
      if (published + failed >= total) break;
      await sleep(800);
    }
    return Date.now() - startTime;
  }

  async function runSide(concurrency, setProgress) {
    await axios.post(`${S2}/concurrency?value=${concurrency}`);
    await sleep(1200);
    const start = Date.now();
    await axios.post(`${S1}/seed?count=${count}`);
    return await pollUntilDone(setProgress, start, count);
  }

  async function runBenchmark() {
    setError(null);
    setProgressA(null);
    setProgressB(null);
    setTimeA(null);
    setTimeB(null);
    abortRef.current = false;
    try {
      setPhase("runningA");
      const msA = await runSide(concA, setProgressA);
      setTimeA(msA);
      await sleep(2000);
      setPhase("runningB");
      const msB = await runSide(concB, setProgressB);
      setTimeB(msB);
      setPhase("done");
      await fetchUsers();
    } catch (e) {
      setError(e.message);
      setPhase("idle");
    }
  }

  const partitions = 3;
  const isRunning = phase === "runningA" || phase === "runningB";
  const winner = timeA && timeB ? (timeA <= timeB ? "A" : "B") : null;

  function PartitionBars({ progress, concurrency }) {
    if (!progress) return <div className="bars-empty">Waiting to start…</div>;
    const { published, total, elapsed } = progress;
    const active = Math.min(concurrency, partitions);
    const perPart = Math.ceil(total / partitions);

    return (
      <div className="bars">
        {Array.from({ length: partitions }, (_, i) => {
          const thread = i % active;
          const col = COLORS[thread % COLORS.length];
          const done = Math.min(Math.max(published - i * perPart, 0), perPart);
          const pct = Math.round((done / perPart) * 100);
          return (
            <div key={i} className="bar-row">
              <div className="bar-label">P{i}</div>
              <div className="bar-track">
                <div
                  className="bar-fill"
                  style={{ width: pct + "%", background: col }}
                />
              </div>
              <div className="bar-pct">{pct}%</div>
            </div>
          );
        })}
        <div className="elapsed-row">
          <span className="elapsed-label">Elapsed</span>
          <span className="elapsed-val">{(elapsed / 1000).toFixed(1)}s</span>
        </div>
      </div>
    );
  }

  function StatusBadge({ side }) {
    if (phase === `running${side}`)
      return (
        <span className={`status status-running${side === "B" ? "-b" : ""}`}>
          <span className="status-dot" />
          Running
        </span>
      );
    if ((side === "A" && timeA) || (side === "B" && timeB))
      return <span className="status status-done">Done</span>;
    return <span className="status status-idle">Idle</span>;
  }

  function TimeResult({ time, side }) {
    if (!time) return null;
    const isWinner = winner === side;
    return (
      <div className={`time-result ${isWinner ? "time-winner" : "time-loser"}`}>
        <span className="time-num">{(time / 1000).toFixed(2)}s</span>
        <span className="time-tag">{isWinner ? "⚡ Faster" : "Slower"}</span>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="topbar">
        <div className="topbar-brand">
          <div className="topbar-name">Kafka Pipeline Benchmark</div>
        </div>
        <div className="topbar-badge">
          <span className="dot-live" />
          Pipeline live
        </div>
      </div>

      <div className="page-title">
        <h1>Concurrency Benchmark</h1>
        <p>
          Compare how different concurrency settings affect Kafka pipeline
          throughput.
        </p>
      </div>

      <div className="config-card">
        <div className="config-header">
          <span className="config-header-title">Configuration</span>
        </div>
        <div className="config-body">
          <div className="count-row">
            <div className="inp-group">
              <label>Messages to process</label>
              <input
                type="number"
                min="5"
                max="200"
                value={count}
                onChange={(e) => setCount(parseInt(e.target.value) || 30)}
              />
            </div>
          </div>

          <div className="sides-grid">
            <div className="side-card side-card-a">
              <div className="side-tag tag-a">Side A</div>
              <div className="inp-group">
                <label>Concurrency</label>
                <input
                  type="number"
                  min="1"
                  max="10"
                  value={concA}
                  onChange={(e) => setConcA(parseInt(e.target.value) || 1)}
                />
              </div>
            </div>
            <div className="vs-divider">VS</div>
            <div className="side-card side-card-b">
              <div className="side-tag tag-b">Side B</div>
              <div className="inp-group">
                <label>Concurrency</label>
                <input
                  type="number"
                  min="1"
                  max="10"
                  value={concB}
                  onChange={(e) => setConcB(parseInt(e.target.value) || 1)}
                />
              </div>
            </div>
          </div>

          <div className="btn-row">
            <button
              className="btn btn-primary"
              onClick={runBenchmark}
              disabled={isRunning}
            >
              {isRunning ? (
                <>
                  <span className="spinner" />
                  {phase === "runningA" ? "Running Side A…" : "Running Side B…"}
                </>
              ) : (
                "Run Benchmark"
              )}
            </button>
            {phase === "done" && (
              <button
                className="btn btn-ghost"
                onClick={() => {
                  setPhase("idle");
                  setProgressA(null);
                  setProgressB(null);
                  setTimeA(null);
                  setTimeB(null);
                }}
              >
                Reset
              </button>
            )}
          </div>

          {error && <div className="error-bar">⚠ {error}</div>}
        </div>
      </div>

      <div className="panels">
        <div
          className={`panel ${phase === "runningA" ? "panel-active-a" : ""}`}
        >
          <div className="panel-top">
            <span className="panel-name">Side A — concurrency={concA}</span>
            <StatusBadge side="A" />
          </div>
          <div className="panel-body">
            <PartitionBars progress={progressA} concurrency={concA} />
            <TimeResult time={timeA} side="A" />
          </div>
        </div>

        <div
          className={`panel ${phase === "runningB" ? "panel-active-b" : ""}`}
        >
          <div className="panel-top">
            <span className="panel-name">Side B — concurrency={concB}</span>
            <StatusBadge side="B" />
          </div>
          <div className="panel-body">
            <PartitionBars progress={progressB} concurrency={concB} />
            <TimeResult time={timeB} side="B" />
          </div>
        </div>
      </div>

      {phase === "done" && timeA && timeB && (
        <div className="result-banner">
          <div className="result-banner-top">
            <span className="result-headline">
              {winner === "A"
                ? `Side A (concurrency=${concA}) wins`
                : `Side B (concurrency=${concB}) wins`}
            </span>
            <span className="speedup-chip">
              {winner === "A"
                ? `${(timeB / timeA).toFixed(1)}x faster`
                : `${(timeA / timeB).toFixed(1)}x faster`}
            </span>
          </div>
          <div className="result-stats">
            <div className="result-stat">
              <div className="result-stat-label">Side A time</div>
              <div className="result-stat-val">
                {(timeA / 1000).toFixed(2)}s
              </div>
            </div>
            <div className="result-stat">
              <div className="result-stat-label">Side B time</div>
              <div className="result-stat-val">
                {(timeB / 1000).toFixed(2)}s
              </div>
            </div>
            <div className="result-stat">
              <div className="result-stat-label">Messages</div>
              <div className="result-stat-val">{count}</div>
            </div>
          </div>
        </div>
      )}
      {users.length > 0 && (
        <div className="users-card">
          <div className="users-header">
            <span className="users-title">Pipeline Output</span>
            <span className="users-count">{users.length} records</span>
          </div>
          <div className="users-table-wrap">
            <table className="users-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Raw Status</th>
                  <th>Mapped Status</th>
                </tr>
              </thead>
              <tbody>
                {users.map((u) => (
                  <tr key={u.id}>
                    <td className="mono">{u.id.substring(0, 8)}...</td>
                    <td>{u.name}</td>
                    <td>
                      <span className="raw-badge">{u.raw}</span>
                    </td>
                    <td>
                      <span
                        className={`status-badge status-${u.mapped.toLowerCase()}`}
                      >
                        {u.mapped}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
