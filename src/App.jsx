import { useState, useRef, useEffect } from "react";
import axios from "axios";
import "./App.css";

const S2 = import.meta.env.VITE_S2_URL || "http://localhost:8082";
const S3 = import.meta.env.VITE_S3_URL || "http://localhost:8083";

const TOTAL = 30_000;
const PARTITIONS = 12;

const HIGH_COLORS = [
  "#4f46e5","#0891b2","#059669","#d97706","#7c3aed","#dc2626",
  "#0d9488","#ea580c","#1d4ed8","#15803d","#b91c1c","#6d28d9",
];
const LOW_COLOR = "#0891b2";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export default function App() {
  const [phase, setPhase] = useState("idle");
  const [highProgress, setHighProgress] = useState(null);
  const [lowProgress, setLowProgress] = useState(null);
  const [highTime, setHighTime] = useState(null);
  const [lowTime, setLowTime] = useState(null);
  const [error, setError] = useState(null);

  const abortRef = useRef(false);

  async function pollProgress(setProgress, startTime) {
    while (!abortRef.current) {
      const res = await axios.get(`${S3}/users/count`);
      const count = Number(res.data.count || 0);
      const elapsed = Date.now() - startTime;
      setProgress({ count, elapsed });
      if (count >= TOTAL) return elapsed;
      await sleep(400);
    }
    throw new Error("Aborted");
  }

  async function runSimulation(type) {
    setError(null);
    abortRef.current = false;

    const setProgress = type === "high" ? setHighProgress : setLowProgress;
    const setTime = type === "high" ? setHighTime : setLowTime;

    setPhase(`running-${type}`);
    setProgress({ count: 0, elapsed: 0 });

    try {
      await axios.delete(`${S3}/users`);
      await axios.post(`${S2}/simulation/${type}/start`);

      const start = Date.now();
      const elapsed = await pollProgress(setProgress, start);
      setTime(elapsed);
      setPhase(`done-${type}`);
    } catch (e) {
      setError(e.message);
      setPhase("idle");
    }
  }

  function stopSimulation() {
    abortRef.current = true;
    axios.post(`${S2}/simulation/stop`).catch(() => {});
    setPhase("idle");
  }

  const isRunning = phase.startsWith("running-");
  const isHigh = phase === "running-high";
  const isLow = phase === "running-low";
  const winner =
    highTime && lowTime ? (highTime < lowTime ? "high" : "low") : null;

  function PartitionBars({ progress, concurrency, isHighGroup }) {
    if (!progress)
      return <div className="bars-empty">Waiting to start…</div>;

    const { count, elapsed } = progress;
    const perPart = Math.ceil(TOTAL / PARTITIONS);

    return (
      <div className="bars">
        {Array.from({ length: PARTITIONS }, (_, i) => {
          const done = Math.min(Math.max(count - i * perPart, 0), perPart);
          const pct = Math.round((done / perPart) * 100);
          const col = isHighGroup
            ? HIGH_COLORS[i % HIGH_COLORS.length]
            : LOW_COLOR;
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

  function SimPanel({ type, label, subtitle, active, progress, time, onRun }) {
    const isRunningThis = phase === `running-${type}`;
    const isA = type === "high";

    return (
      <div className={`panel ${active ? (isA ? "panel-active-a" : "panel-active-b") : ""}`}>
        <div className="panel-top">
          <span className="panel-name">{label}</span>
          {isRunningThis ? (
            <span className={`status ${isA ? "status-running" : "status-running-b"}`}>
              <span className="status-dot" />
              Running
            </span>
          ) : time ? (
            <span className="status status-done">Done</span>
          ) : (
            <span className="status status-idle">Idle</span>
          )}
        </div>
        <div className="panel-desc">{subtitle}</div>
        <div className="panel-body">
          {progress && (
            <>
              <div className="count-display">
                <span className="count-num">
                  {progress.count.toLocaleString()}
                </span>
                <span className="count-sep"> / </span>
                <span className="count-total">{TOTAL.toLocaleString()}</span>
              </div>
              <div className="progress-outer">
                <div
                  className={`progress-inner${isA ? "" : " progress-inner-b"}`}
                  style={{
                    width: `${Math.min((progress.count / TOTAL) * 100, 100).toFixed(1)}%`,
                  }}
                />
              </div>
            </>
          )}
          <PartitionBars
            progress={progress}
            concurrency={type === "high" ? 12 : 1}
            isHighGroup={type === "high"}
          />
          {time && (
            <div
              className={`time-result ${
                winner === type ? "time-winner" : "time-loser"
              }`}
            >
              <span className="time-num">{(time / 1000).toFixed(2)}s</span>
              <span className="time-tag">
                {winner === type ? "Faster" : "Slower"}
              </span>
            </div>
          )}
        </div>
        <div className="panel-footer">
          <button
            className={`btn ${isA ? "btn-primary" : "btn-primary-b"}`}
            disabled={isRunning}
            onClick={onRun}
          >
            {isRunningThis ? (
              <>
                <span className="spinner" />
                Running…
              </>
            ) : (
              `Run ${label}`
            )}
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="topbar">
        <div className="topbar-brand">
          <div className="topbar-name">Kafka Concurrency Simulator</div>
        </div>
      </div>

      <div className="page-title">
        <h1>Concurrency Simulator</h1>
        <p>
          Same topic · 12 partitions · two consumer groups — watch how
          concurrency changes throughput.
        </p>
      </div>

      {error && <div className="error-bar">{error}</div>}

      <div className="panels">
        <SimPanel
          type="high"
          label="12-Consumer Group"
          subtitle="12 threads · 12 partitions"
          active={isHigh}
          progress={highProgress}
          time={highTime}
          onRun={() => runSimulation("high")}
        />
        <SimPanel
          type="low"
          label="1-Consumer Group"
          subtitle="1 thread · 12 partitions"
          active={isLow}
          progress={lowProgress}
          time={lowTime}
          onRun={() => runSimulation("low")}
        />
      </div>

      {isRunning && (
        <div className="stop-row">
          <button className="btn btn-ghost" onClick={stopSimulation}>
            Stop Simulation
          </button>
        </div>
      )}

      {winner && highTime && lowTime && (
        <div className="result-banner">
          <div className="result-banner-top">
            <span className="result-headline">
              {winner === "high"
                ? "12-Consumer Group wins!"
                : "1-Consumer Group wins!"}
            </span>
            <span className="speedup-chip">
              {winner === "high"
                ? `${(lowTime / highTime).toFixed(1)}x faster`
                : `${(highTime / lowTime).toFixed(1)}x faster`}
            </span>
          </div>
          <div className="result-stats">
            <div className="result-stat">
              <div className="result-stat-label">12-Consumer</div>
              <div className="result-stat-val">
                {(highTime / 1000).toFixed(2)}s
              </div>
            </div>
            <div className="result-stat">
              <div className="result-stat-label">1-Consumer</div>
              <div className="result-stat-val">
                {(lowTime / 1000).toFixed(2)}s
              </div>
            </div>
            <div className="result-stat">
              <div className="result-stat-label">Messages</div>
              <div className="result-stat-val">
                {TOTAL.toLocaleString()}
              </div>
            </div>
          </div>
          <div className="reset-row">
            <button
              className="btn btn-ghost"
              onClick={() => {
                setHighProgress(null);
                setLowProgress(null);
                setHighTime(null);
                setLowTime(null);
                setPhase("idle");
                setError(null);
              }}
            >
              Reset
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
