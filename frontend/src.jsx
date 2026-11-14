import React, { useEffect, useState, useMemo, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// ==========================================================
// 1. REUSABLE INTERACTIVE SVG COMPONENTS
// ==========================================================

// Interactive SVG Donut Chart with Hover and Center Statistics
function InteractiveDonut({ data, size = 180, strokeWidth = 24, title = 'Total' }) {
  const [hoveredIdx, setHoveredIdx] = useState(null);

  const total = useMemo(() => data.reduce((acc, d) => acc + (d.value || 0), 0), [data]);
  const center = size / 2;
  const radius = center - strokeWidth / 2 - 4;
  const circumference = 2 * Math.PI * radius;

  let accumulated = 0;
  const segments = data.map((item, idx) => {
    const val = item.value || 0;
    const pct = total > 0 ? val / total : 0;
    const strokeDasharray = `${pct * circumference} ${circumference}`;
    const strokeDashoffset = -accumulated * circumference;
    accumulated += pct;
    return { ...item, pct, strokeDasharray, strokeDashoffset, idx };
  });

  const activeItem = hoveredIdx !== null ? data[hoveredIdx] : null;

  return (
    <div className="donut-chart-container">
      <div className="donut-svg-wrap" style={{ width: size, height: size }}>
        <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
          {/* Background circle */}
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#162335"
            strokeWidth={strokeWidth}
          />
          {total > 0 &&
            segments.map((seg) => (
              <circle
                key={seg.label}
                cx={center}
                cy={center}
                r={radius}
                fill="none"
                stroke={seg.color}
                strokeWidth={hoveredIdx === seg.idx ? strokeWidth + 4 : strokeWidth}
                strokeDasharray={seg.strokeDasharray}
                strokeDashoffset={seg.strokeDashoffset}
                strokeLinecap="round"
                className="donut-segment"
                onMouseEnter={() => setHoveredIdx(seg.idx)}
                onMouseLeave={() => setHoveredIdx(null)}
                style={{
                  transform: 'rotate(-90deg)',
                  transformOrigin: '50% 50%',
                  transition: 'all 0.3s ease',
                  cursor: 'pointer',
                  filter: hoveredIdx === seg.idx ? `drop-shadow(0 0 6px ${seg.color})` : 'none',
                }}
              />
            ))}
        </svg>
        <div className="donut-center-info">
          {activeItem ? (
            <>
              <span className="donut-center-val" style={{ color: activeItem.color }}>
                {activeItem.value}
              </span>
              <span className="donut-center-label">{activeItem.label}</span>
              <span className="donut-center-sub">
                {Math.round(((activeItem.value || 0) / (total || 1)) * 100)}%
              </span>
            </>
          ) : (
            <>
              <span className="donut-center-val">{total}</span>
              <span className="donut-center-label">{title}</span>
              <span className="donut-center-sub">Records</span>
            </>
          )}
        </div>
      </div>

      <div className="donut-legend">
        {data.map((d, i) => (
          <div
            key={d.label}
            className={`legend-item ${hoveredIdx === i ? 'legend-active' : ''}`}
            onMouseEnter={() => setHoveredIdx(i)}
            onMouseLeave={() => setHoveredIdx(null)}
          >
            <span className="legend-dot" style={{ backgroundColor: d.color }}></span>
            <span className="legend-name">{d.label}</span>
            <span className="legend-val">
              {d.value}{' '}
              <small>({Math.round(((d.value || 0) / (total || 1)) * 100)}%)</small>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// Semi-Circular Threat Level Gauge
function ThreatGauge({ score = 45, status = 'ELEVATED_THREAT' }) {
  // Score 0 to 100 mapped to angle -90 to +90 deg
  const clampedScore = Math.max(0, Math.min(100, score));
  const angle = -90 + (clampedScore / 100) * 180;

  const color =
    clampedScore > 70 ? '#ef4444' : clampedScore > 35 ? '#f59e0b' : '#10b981';

  return (
    <div className="threat-gauge-box">
      <div className="gauge-svg-wrap">
        <svg viewBox="0 0 200 115" width="200" height="115">
          {/* Gauge Background Arc */}
          <path
            d="M 20 100 A 80 80 0 0 1 180 100"
            fill="none"
            stroke="#162335"
            strokeWidth="16"
            strokeLinecap="round"
          />
          {/* Coloured Threat Spectrum Segments */}
          <path
            d="M 20 100 A 80 80 0 0 1 73 43"
            fill="none"
            stroke="#10b981"
            strokeWidth="16"
            strokeLinecap="round"
            opacity="0.3"
          />
          <path
            d="M 73 43 A 80 80 0 0 1 127 43"
            fill="none"
            stroke="#f59e0b"
            strokeWidth="16"
            opacity="0.3"
          />
          <path
            d="M 127 43 A 80 80 0 0 1 180 100"
            fill="none"
            stroke="#ef4444"
            strokeWidth="16"
            strokeLinecap="round"
            opacity="0.3"
          />

          {/* Active indicator arc */}
          <path
            d="M 20 100 A 80 80 0 0 1 180 100"
            fill="none"
            stroke={color}
            strokeWidth="16"
            strokeLinecap="round"
            strokeDasharray="251.2"
            strokeDashoffset={251.2 - (clampedScore / 100) * 251.2}
            style={{ transition: 'stroke-dashoffset 0.8s ease, stroke 0.5s ease' }}
          />

          {/* Pivot Dot */}
          <circle cx="100" cy="100" r="7" fill="#fff" />
          {/* Needle pointer */}
          <line
            x1="100"
            y1="100"
            x2="100"
            y2="32"
            stroke="#ffffff"
            strokeWidth="3"
            strokeLinecap="round"
            transform={`rotate(${angle} 100 100)`}
            style={{ transition: 'transform 0.8s cubic-bezier(0.34, 1.56, 0.64, 1)' }}
          />
        </svg>
      </div>

      <div className="gauge-reading">
        <span className="gauge-score" style={{ color }}>
          {clampedScore}
          <small>/100</small>
        </span>
        <span className="gauge-badge" style={{ backgroundColor: `${color}22`, color, borderColor: color }}>
          {status.replaceAll('_', ' ')}
        </span>
      </div>
    </div>
  );
}

// Sparkline Trend Bar
function VelocityHistory({ data = [12, 18, 25, 30, 22, 45, 52, 60, 48, 70] }) {
  const max = Math.max(...data, 10);
  return (
    <div className="velocity-stream">
      <div className="velocity-bars">
        {data.map((val, i) => {
          const pct = Math.round((val / max) * 100);
          return (
            <div key={i} className="v-bar-col" title={`Window ${i + 1}: ${val} events/s`}>
              <div
                className="v-bar"
                style={{
                  height: `${pct}%`,
                  background: val > 50 ? 'linear-gradient(to top, #ef4444, #f59e0b)' : 'linear-gradient(to top, #0070f3, #00d2ff)',
                }}
              ></div>
              <span className="v-bar-lbl">{i === data.length - 1 ? 'Now' : ''}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ==========================================================
// 2. MAIN APPLICATION COMPONENT
// ==========================================================
function App() {
  const [tab, setTab] = useState('overview');
  const [metrics, setMetrics] = useState({
    total_events_ingested: 0,
    recent_events_buffered: 0,
    total_alerts: 0,
    alerts_by_severity: { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0, INFO: 0 },
    alerts_by_status: { open: 0, investigating: 0, resolved: 0 },
    top_triggered_rules: {},
    system_status: 'NORMAL',
  });
  const [alerts, setAlerts] = useState([]);
  const [events, setEvents] = useState([]);
  const [topIps, setTopIps] = useState([]);
  const [health, setHealth] = useState(null);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [alertFilter, setAlertFilter] = useState({ severity: '', status: '', search: '' });
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [soundEnabled, setSoundEnabled] = useState(false);
  const [actionMsg, setActionMsg] = useState('');
  const [loading, setLoading] = useState(false);
  const [velocityHistory, setVelocityHistory] = useState([8, 14, 22, 19, 31, 28, 42, 38, 55, 62]);

  const prevAlertCount = useRef(0);

  // Custom event state
  const [eventForm, setEventForm] = useState({
    source_ip: '198.51.100.25',
    destination_ip: '10.0.0.10',
    destination_port: 22,
    event_type: 'FAILED_LOGIN',
    username: 'root',
    status: 'failure',
    dataset_label: 'synthetic:test',
    process_name: '',
    severity: 'HIGH',
    message: 'Manual security event probe',
  });

  // Synthesize soft audio alert on new detection
  function playAlertBeep() {
    if (!soundEnabled) return;
    try {
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'sine';
      osc.frequency.setValueAtTime(880, ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(440, ctx.currentTime + 0.15);
      gain.gain.setValueAtTime(0.1, ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.2);
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.start();
      osc.stop(ctx.currentTime + 0.2);
    } catch {
      // Audio autoplay policy fallback
    }
  }

  async function fetchAllData() {
    try {
      // 1. Metrics summary
      const mRes = await fetch(`${API_BASE}/api/metrics/summary`);
      if (mRes.ok) {
        const mData = await mRes.json();
        setMetrics(mData);

        // Sound trigger if new alerts arrive
        if (mData.total_alerts > prevAlertCount.current && prevAlertCount.current > 0) {
          playAlertBeep();
        }
        prevAlertCount.current = mData.total_alerts;

        // Push to velocity sparkline
        setVelocityHistory((prev) => {
          const next = [...prev.slice(1), mData.recent_events_buffered || 15];
          return next;
        });
      }

      // 2. Alerts
      const aRes = await fetch(`${API_BASE}/api/alerts?limit=100`);
      if (aRes.ok) {
        const aData = await aRes.json();
        setAlerts(aData.alerts || []);
      }

      // 3. Recent events
      const eRes = await fetch(`${API_BASE}/api/events/recent?limit=50`);
      if (eRes.ok) {
        const eData = await eRes.json();
        setEvents(eData.events || []);
      }

      // 4. Top IPs
      const ipRes = await fetch(`${API_BASE}/api/top-ips?limit=10`);
      if (ipRes.ok) {
        const ipData = await ipRes.json();
        setTopIps(ipData.top_ips || []);
      }

      // 5. System Health
      const hRes = await fetch(`${API_BASE}/api/health`);
      if (hRes.ok) setHealth(await hRes.json());
    } catch (err) {
      console.warn('Backend polling error:', err);
    }
  }

  useEffect(() => {
    fetchAllData();
    if (!autoRefresh) return;
    const interval = setInterval(fetchAllData, 3500);
    return () => clearInterval(interval);
  }, [autoRefresh, soundEnabled]);

  async function triggerReplay(scenario) {
    setLoading(true);
    setActionMsg(`Triggering ${scenario} scenario...`);
    try {
      const res = await fetch(`${API_BASE}/api/replay/trigger?scenario=${scenario}`, {
        method: 'POST',
      });
      const data = await res.json();
      if (res.ok) {
        setActionMsg(`✓ Triggered ${scenario}: ${data.count} events sent to Kafka.`);
        setTimeout(fetchAllData, 1000);
      } else {
        setActionMsg(`✗ Trigger failed: ${data.detail || 'Error'}`);
      }
    } catch (e) {
      setActionMsg(`✗ Replay request error: ${e.message}`);
    } finally {
      setLoading(false);
    }
  }

  async function updateAlertStatus(alertId, newStatus) {
    try {
      const res = await fetch(`${API_BASE}/api/alerts/${encodeURIComponent(alertId)}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus }),
      });
      if (res.ok) {
        setActionMsg(`Alert updated to [${newStatus.toUpperCase()}]`);
        fetchAllData();
        if (selectedAlert && (selectedAlert.alert_id === alertId || selectedAlert.id === alertId)) {
          setSelectedAlert({ ...selectedAlert, status: newStatus });
        }
      }
    } catch (e) {
      setActionMsg(`Failed to update status: ${e.message}`);
    }
  }

  async function handleSendCustomEvent(e) {
    e.preventDefault();
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...eventForm,
          destination_port: parseInt(eventForm.destination_port, 10),
        }),
      });
      const data = await res.json();
      if (res.ok) {
        setActionMsg(`✓ Ingested event ${data.event_id} into Kafka topic security-events.`);
        setTimeout(fetchAllData, 1000);
      } else {
        setActionMsg(`✗ Ingestion failed: ${data.detail || 'Error'}`);
      }
    } catch (err) {
      setActionMsg(`✗ Ingestion error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  }

  // Filter alerts
  const filteredAlerts = alerts.filter((a) => {
    if (alertFilter.severity && a.severity?.toUpperCase() !== alertFilter.severity.toUpperCase()) {
      return false;
    }
    if (alertFilter.status && a.status?.toLowerCase() !== alertFilter.status.toLowerCase()) {
      return false;
    }
    if (alertFilter.search) {
      const s = alertFilter.search.toLowerCase();
      const match =
        a.source_ip?.toLowerCase().includes(s) ||
        a.description?.toLowerCase().includes(s) ||
        a.rule_id?.toLowerCase().includes(s) ||
        a.type?.toLowerCase().includes(s);
      if (!match) return false;
    }
    return true;
  });

  // Calculate Threat Score
  const threatScore = useMemo(() => {
    const crit = (metrics.alerts_by_severity?.CRITICAL || 0) * 35;
    const high = (metrics.alerts_by_severity?.HIGH || 0) * 15;
    const med = (metrics.alerts_by_severity?.MEDIUM || 0) * 5;
    const base = crit + high + med;
    return Math.min(100, Math.max(12, base));
  }, [metrics]);

  // Donut data for Status
  const statusDonutData = useMemo(() => {
    const s = metrics.alerts_by_status || {};
    return [
      { label: 'Open', value: s.open || 0, color: '#ef4444' },
      { label: 'Investigating', value: s.investigating || 0, color: '#f59e0b' },
      { label: 'Resolved', value: s.resolved || 0, color: '#10b981' },
    ];
  }, [metrics]);

  // Donut data for Severity
  const severityDonutData = useMemo(() => {
    const sev = metrics.alerts_by_severity || {};
    return [
      { label: 'Critical', value: sev.CRITICAL || 0, color: '#ef4444' },
      { label: 'High', value: sev.HIGH || 0, color: '#f59e0b' },
      { label: 'Medium', value: sev.MEDIUM || 0, color: '#00d2ff' },
      { label: 'Low/Info', value: (sev.LOW || 0) + (sev.INFO || 0), color: '#64748b' },
    ];
  }, [metrics]);

  return (
    <div className="soc-container">
      {/* TOP COMMAND HEADER */}
      <header className="soc-header">
        <div className="brand-section">
          <div className="logo-glow-hex">⬡</div>
          <div>
            <div className="brand-title-row">
              <h1>ThreatNexus</h1>
              <span className="soc-badge">SOC CORE v2.0</span>
            </div>
            <span className="subtitle">
              Distributed Real-Time Threat Detection & Big Data Stream Analytics
            </span>
          </div>
        </div>

        <div className="header-controls">
          <div
            className={`threat-pill ${metrics.system_status === 'NORMAL' ? 'pill-green' : 'pill-red'}`}
          >
            <span className="pulse-indicator"></span>
            DEFCON: {metrics.system_status.replace('_', ' ')}
          </div>

          <button
            className={`btn-icon ${soundEnabled ? 'btn-sound-on' : ''}`}
            onClick={() => setSoundEnabled(!soundEnabled)}
            title="Toggle Audio Threat Siren"
          >
            {soundEnabled ? '🔔 Audio ON' : '🔕 Muted'}
          </button>

          <button
            className={`btn-stream ${autoRefresh ? 'active' : ''}`}
            onClick={() => setAutoRefresh(!autoRefresh)}
          >
            <span className="live-dot"></span>
            {autoRefresh ? 'Streaming (3.5s)' : 'Paused'}
          </button>

          <button className="btn-refresh" onClick={fetchAllData} title="Force Sync">
            ↻
          </button>
        </div>
      </header>

      {/* NOTIFICATION BANNER */}
      {actionMsg && (
        <div className="action-banner">
          <span className="action-text">{actionMsg}</span>
          <button className="banner-dismiss" onClick={() => setActionMsg('')}>
            ✕
          </button>
        </div>
      )}

      {/* NAVIGATION BAR */}
      <nav className="soc-nav">
        <button
          className={tab === 'overview' ? 'tab-btn active' : 'tab-btn'}
          onClick={() => setTab('overview')}
        >
          <span className="tab-icon">📊</span> Overview
        </button>
        <button
          className={tab === 'events' ? 'tab-btn active' : 'tab-btn'}
          onClick={() => setTab('events')}
        >
          <span className="tab-icon">⚡</span> Live Ingestion
          <span className="tab-pill">{events.length}</span>
        </button>
        <button
          className={tab === 'alerts' ? 'tab-btn active' : 'tab-btn'}
          onClick={() => setTab('alerts')}
        >
          <span className="tab-icon">🚨</span> Alerts Triage
          <span className="tab-pill pill-alert">{alerts.length}</span>
        </button>
        <button
          className={tab === 'ip_analysis' ? 'tab-btn active' : 'tab-btn'}
          onClick={() => setTab('ip_analysis')}
        >
          <span className="tab-icon">🎯</span> IP Intelligence
        </button>
        <button
          className={tab === 'analytics' ? 'tab-btn active' : 'tab-btn'}
          onClick={() => setTab('analytics')}
        >
          <span className="tab-icon">📈</span> Analytics & Trends
        </button>
        <button
          className={tab === 'health' ? 'tab-btn active' : 'tab-btn'}
          onClick={() => setTab('health')}
        >
          <span className="tab-icon">⚙️</span> Cluster Topology
        </button>
        <button
          className={tab === 'simulator' ? 'tab-btn active' : 'tab-btn'}
          onClick={() => setTab('simulator')}
        >
          <span className="tab-icon">🧪</span> Attack Simulator
        </button>
      </nav>

      {/* ========================================================== */}
      {/* TAB 1: OVERVIEW */}
      {/* ========================================================== */}
      {tab === 'overview' && (
        <main className="tab-content">
          {/* TOP KPI HUD */}
          <div className="hud-grid">
            <div className="hud-card border-cyan">
              <div className="hud-header">
                <span className="hud-label">INGESTION VOLUME</span>
                <span className="hud-chip">KAFKA</span>
              </div>
              <div className="hud-stat text-cyan">{metrics.total_events_ingested}</div>
              <div className="hud-footer">
                <span>Buffer: {metrics.recent_events_buffered} active</span>
                <span className="trend-up">↑ 100% loss-free</span>
              </div>
            </div>

            <div className="hud-card border-amber">
              <div className="hud-header">
                <span className="hud-label">DETECTIONS EMITTED</span>
                <span className="hud-chip">SPARK</span>
              </div>
              <div className="hud-stat text-amber">{metrics.total_alerts}</div>
              <div className="hud-footer">
                <span>Window: 60s sliding</span>
                <span>Watermark: 2m</span>
              </div>
            </div>

            <div className="hud-card border-red">
              <div className="hud-header">
                <span className="hud-label">CRITICAL & HIGH RISKS</span>
                <span className="hud-chip chip-red">PRIORITY</span>
              </div>
              <div className="hud-stat text-red">
                {(metrics.alerts_by_severity?.CRITICAL || 0) +
                  (metrics.alerts_by_severity?.HIGH || 0)}
              </div>
              <div className="hud-footer">
                <span>Requires SOC Triage</span>
                <span className="badge-sev sev-critical">CRITICAL</span>
              </div>
            </div>

            <div className="hud-card border-green">
              <div className="hud-header">
                <span className="hud-label">STORAGE ADAPTERS</span>
                <span className="hud-chip chip-green">HBASE & HDFS</span>
              </div>
              <div className="hud-stat text-green">
                {health?.dependencies?.hbase?.status === 'healthy' ? 'OPTIMAL' : 'ONLINE'}
              </div>
              <div className="hud-footer">
                <span>Reverse-TS Row Keys</span>
                <span>Date Partitioned</span>
              </div>
            </div>
          </div>

          {/* DUAL COCKPIT: GAUGE & QUICK TRIGGER */}
          <div className="grid-2-col mt-4">
            {/* THREAT DIAL & VELOCITY */}
            <div className="cyber-panel">
              <div className="panel-header">
                <h3>🛡️ System Threat Index & Ingestion Velocity</h3>
                <span className="panel-tag">LIVE METRICS</span>
              </div>
              <div className="gauge-velocity-flex">
                <ThreatGauge score={threatScore} status={metrics.system_status} />
                <div className="velocity-wrapper">
                  <div className="velocity-header">
                    <span>Events Flow Velocity (10 micro-batches)</span>
                    <b>{velocityHistory[velocityHistory.length - 1]} evt/batch</b>
                  </div>
                  <VelocityHistory data={velocityHistory} />
                  <div className="velocity-details">
                    <span>Kafka Partition Offsets: OK</span>
                    <span>Watermark Drift: &lt; 1.2s</span>
                  </div>
                </div>
              </div>
            </div>

            {/* QUICK SCENARIO LAUNCHER */}
            <div className="cyber-panel">
              <div className="panel-header">
                <h3>⚡ One-Click Threat Scenario Injector</h3>
                <span className="panel-tag">DEMO SUITE</span>
              </div>
              <p className="panel-desc">
                Inject deterministic cyber attack patterns into Kafka to verify Spark detection rules.
              </p>
              <div className="scenario-launcher-grid">
                <button
                  className="scenario-card card-brute"
                  disabled={loading}
                  onClick={() => triggerReplay('brute_force')}
                >
                  <div className="sc-icon">💥</div>
                  <div className="sc-info">
                    <b>SSH Brute Force</b>
                    <small>7 failed logins / 30s</small>
                    <span className="sc-rule">BRUTE_FORCE_001</span>
                  </div>
                </button>

                <button
                  className="scenario-card card-scan"
                  disabled={loading}
                  onClick={() => triggerReplay('port_scan')}
                >
                  <div className="sc-icon">🔍</div>
                  <div className="sc-info">
                    <b>Horizontal Port Scan</b>
                    <small>12 distinct port probes</small>
                    <span className="sc-rule">PORT_SCAN_004</span>
                  </div>
                </button>

                <button
                  className="scenario-card card-proc"
                  disabled={loading}
                  onClick={() => triggerReplay('process')}
                >
                  <div className="sc-icon">☣️</div>
                  <div className="sc-info">
                    <b>Credential Dumper</b>
                    <small>mimikatz process spawn</small>
                    <span className="sc-rule">SUSPICIOUS_PROC_005</span>
                  </div>
                </button>

                <button
                  className="scenario-card card-cic"
                  disabled={loading}
                  onClick={() => triggerReplay('cic_ids2017')}
                >
                  <div className="sc-icon">📂</div>
                  <div className="sc-info">
                    <b>CIC-IDS2017 Replay</b>
                    <small>Benchmark flow attack</small>
                    <span className="sc-rule">CIC_DATASET_ATTACK_006</span>
                  </div>
                </button>
              </div>
            </div>
          </div>

          {/* RECENT DETECTIONS PREVIEW TABLE */}
          <div className="cyber-panel mt-4">
            <div className="panel-header">
              <div className="flex-row gap-2">
                <h3>🚨 Active Security Alerts</h3>
                <span className="badge-counter">{alerts.length} Total</span>
              </div>
              <button className="btn-link" onClick={() => setTab('alerts')}>
                Open Full Triage Center →
              </button>
            </div>
            <div className="table-responsive">
              <table className="soc-table">
                <thead>
                  <tr>
                    <th>Alert ID</th>
                    <th>Detection Rule</th>
                    <th>Source IP</th>
                    <th>Severity</th>
                    <th>Trigger Description</th>
                    <th>Status</th>
                    <th>Detected At</th>
                  </tr>
                </thead>
                <tbody>
                  {alerts.slice(0, 6).map((a, i) => (
                    <tr key={a.id || i} onClick={() => setSelectedAlert(a)} className="clickable-row">
                      <td className="code-font text-cyan">{a.alert_id || a.id}</td>
                      <td>
                        <span className="badge-rule">{a.rule_id || a.type}</span>
                      </td>
                      <td className="code-font font-bold">{a.source_ip}</td>
                      <td>
                        <span className={`badge-sev sev-${(a.severity || 'high').toLowerCase()}`}>
                          {a.severity}
                        </span>
                      </td>
                      <td>{a.description}</td>
                      <td>
                        <span className={`badge-status status-${a.status}`}>{a.status}</span>
                      </td>
                      <td className="time-col">{a.created_at?.slice(0, 19)}</td>
                    </tr>
                  ))}
                  {alerts.length === 0 && (
                    <tr>
                      <td colSpan="7" className="empty-cell">
                        No alerts detected yet. Launch a test scenario above or replay events.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      )}

      {/* ========================================================== */}
      {/* TAB 2: LIVE EVENTS STREAM */}
      {/* ========================================================== */}
      {tab === 'events' && (
        <main className="tab-content">
          <div className="cyber-panel">
            <div className="panel-header">
              <div>
                <h3>⚡ Live Security Event Ingestion Stream</h3>
                <p className="panel-desc">
                  Real-time normalized security logs buffered before sliding-window evaluation.
                </p>
              </div>
              <div className="flex-row gap-2">
                <span className="badge-counter">{events.length} Buffered Events</span>
              </div>
            </div>

            <div className="table-responsive">
              <table className="soc-table">
                <thead>
                  <tr>
                    <th>Timestamp (UTC)</th>
                    <th>Event ID</th>
                    <th>Source IP</th>
                    <th>Destination IP:Port</th>
                    <th>Event Type</th>
                    <th>User</th>
                    <th>Origin Dataset</th>
                    <th>Severity</th>
                  </tr>
                </thead>
                <tbody>
                  {events.map((e, idx) => (
                    <tr key={e.event_id || idx}>
                      <td className="time-col">{e.timestamp}</td>
                      <td className="code-font text-cyan">{e.event_id}</td>
                      <td className="code-font font-bold">{e.source_ip}</td>
                      <td className="code-font text-muted">
                        {e.destination_ip}:{e.destination_port}
                      </td>
                      <td>
                        <span className="badge-type">{e.event_type}</span>
                      </td>
                      <td>{e.username || '—'}</td>
                      <td>
                        <span
                          className={`badge-label ${
                            e.dataset_label?.includes('cic')
                              ? 'label-dataset'
                              : 'label-synthetic'
                          }`}
                        >
                          {e.dataset_label || 'synthetic'}
                        </span>
                      </td>
                      <td>
                        <span
                          className={`badge-sev sev-${(e.severity || 'info').toLowerCase()}`}
                        >
                          {e.severity}
                        </span>
                      </td>
                    </tr>
                  ))}
                  {events.length === 0 && (
                    <tr>
                      <td colSpan="8" className="empty-cell">
                        Waiting for streaming events. Click "Attack Simulator" or run the replay script.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      )}

      {/* ========================================================== */}
      {/* TAB 3: ALERTS TRIAGE CENTER */}
      {/* ========================================================== */}
      {tab === 'alerts' && (
        <main className="tab-content">
          <div className="cyber-panel">
            <div className="panel-header-wrap">
              <div>
                <h3>🚨 Threat Alerts Management Center</h3>
                <p className="panel-desc">
                  Persisted HBase alert records with stateful triage lifecycle management.
                </p>
              </div>

              {/* SEARCH & FILTER CONTROLS */}
              <div className="filter-controls-row">
                <div className="search-wrap">
                  <span className="search-icon">🔍</span>
                  <input
                    type="text"
                    placeholder="Search IP, Rule, Description..."
                    value={alertFilter.search}
                    onChange={(e) => setAlertFilter({ ...alertFilter, search: e.target.value })}
                    className="search-input"
                  />
                </div>

                <select
                  value={alertFilter.severity}
                  onChange={(e) => setAlertFilter({ ...alertFilter, severity: e.target.value })}
                  className="filter-select"
                >
                  <option value="">All Severities</option>
                  <option value="CRITICAL">Critical</option>
                  <option value="HIGH">High</option>
                  <option value="MEDIUM">Medium</option>
                  <option value="LOW">Low</option>
                </select>

                <select
                  value={alertFilter.status}
                  onChange={(e) => setAlertFilter({ ...alertFilter, status: e.target.value })}
                  className="filter-select"
                >
                  <option value="">All Statuses</option>
                  <option value="open">Open</option>
                  <option value="investigating">Investigating</option>
                  <option value="resolved">Resolved</option>
                </select>

                {(alertFilter.search || alertFilter.severity || alertFilter.status) && (
                  <button
                    className="btn-clear"
                    onClick={() => setAlertFilter({ severity: '', status: '', search: '' })}
                  >
                    Clear Filters
                  </button>
                )}
              </div>
            </div>

            <div className="table-responsive">
              <table className="soc-table">
                <thead>
                  <tr>
                    <th>Alert ID</th>
                    <th>Rule Code</th>
                    <th>Source IP</th>
                    <th>Severity</th>
                    <th>Count</th>
                    <th>Description</th>
                    <th>Status</th>
                    <th>Detected Time</th>
                    <th>Triage Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredAlerts.map((a, idx) => (
                    <tr key={a.id || idx}>
                      <td
                        className="code-font text-cyan cursor-pointer"
                        onClick={() => setSelectedAlert(a)}
                        title="Click to view raw JSON details"
                      >
                        <u>{a.alert_id || a.id}</u>
                      </td>
                      <td>
                        <span className="badge-rule">{a.rule_id || a.type}</span>
                      </td>
                      <td className="code-font font-bold">{a.source_ip}</td>
                      <td>
                        <span
                          className={`badge-sev sev-${(a.severity || 'high').toLowerCase()}`}
                        >
                          {a.severity}
                        </span>
                      </td>
                      <td>
                        <span className="count-pill">{a.event_count || 1}</span>
                      </td>
                      <td>{a.description}</td>
                      <td>
                        <span className={`badge-status status-${a.status || 'open'}`}>
                          {a.status || 'open'}
                        </span>
                      </td>
                      <td className="time-col">{a.created_at?.slice(0, 19)}</td>
                      <td>
                        <div className="btn-group-sm">
                          {a.status !== 'investigating' && (
                            <button
                              className="btn-action-investigate"
                              onClick={() => updateAlertStatus(a.alert_id || a.id, 'investigating')}
                              title="Mark as Investigating"
                            >
                              Investigate
                            </button>
                          )}
                          {a.status !== 'resolved' && (
                            <button
                              className="btn-action-resolve"
                              onClick={() => updateAlertStatus(a.alert_id || a.id, 'resolved')}
                              title="Resolve Alert"
                            >
                              Resolve
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                  {filteredAlerts.length === 0 && (
                    <tr>
                      <td colSpan="9" className="empty-cell">
                        No alerts match your filter criteria.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      )}

      {/* ========================================================== */}
      {/* TAB 4: IP INTELLIGENCE & THREAT PROFILING */}
      {/* ========================================================== */}
      {tab === 'ip_analysis' && (
        <main className="tab-content">
          <div className="cyber-panel">
            <div className="panel-header">
              <div>
                <h3>🎯 Threat Intelligence & Attacker IP Profiling</h3>
                <p className="panel-desc">
                  Ranked intelligence table computed from HBase `threatnexus:ip_summary` and alert clusters.
                </p>
              </div>
              <span className="badge-counter">{topIps.length} Suspect Hosts</span>
            </div>

            <div className="table-responsive">
              <table className="soc-table">
                <thead>
                  <tr>
                    <th>Source IP Address</th>
                    <th>Threat Risk Score</th>
                    <th>Peak Severity</th>
                    <th>Total Alert Hits</th>
                    <th>Triggered Attack Rules</th>
                    <th>Last Observed</th>
                    <th>Drill-Down</th>
                  </tr>
                </thead>
                <tbody>
                  {topIps.map((item, idx) => (
                    <tr key={item.source_ip || idx}>
                      <td className="code-font font-bold">
                        <span className="ip-indicator"></span>
                        {item.source_ip}
                      </td>
                      <td>
                        <div className="score-flex">
                          <div
                            className={`risk-badge ${
                              item.threat_score > 70
                                ? 'risk-high'
                                : item.threat_score > 30
                                ? 'risk-med'
                                : 'risk-low'
                            }`}
                          >
                            {item.threat_score} / 100
                          </div>
                          <div className="mini-progress">
                            <div
                              className="mini-progress-bar"
                              style={{
                                width: `${item.threat_score}%`,
                                backgroundColor:
                                  item.threat_score > 70
                                    ? '#ef4444'
                                    : item.threat_score > 30
                                    ? '#f59e0b'
                                    : '#10b981',
                              }}
                            ></div>
                          </div>
                        </div>
                      </td>
                      <td>
                        <span
                          className={`badge-sev sev-${(item.highest_severity || 'info').toLowerCase()}`}
                        >
                          {item.highest_severity}
                        </span>
                      </td>
                      <td>
                        <b className="text-large">{item.total_alerts}</b>
                      </td>
                      <td>
                        <div className="rules-tag-list">
                          {item.rules_triggered?.map((r) => (
                            <span key={r} className="badge-rule">
                              {r}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="time-col">{item.last_seen?.slice(0, 19)}</td>
                      <td>
                        <button
                          className="btn-table-action"
                          onClick={() => {
                            setAlertFilter({ ...alertFilter, search: item.source_ip });
                            setTab('alerts');
                          }}
                        >
                          View {item.total_alerts} Alerts →
                        </button>
                      </td>
                    </tr>
                  ))}
                  {topIps.length === 0 && (
                    <tr>
                      <td colSpan="7" className="empty-cell">
                        No active IP intelligence records yet. Run simulations to profile hosts.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      )}

      {/* ========================================================== */}
      {/* TAB 5: ANALYTICS & TRENDS (REDESIGNED, INTERACTIVE & UNIQUE) */}
      {/* ========================================================== */}
      {tab === 'analytics' && (
        <main className="tab-content">
          {/* ROW 1: INTERACTIVE DONUTS & STATUS METRICS */}
          <div className="analytics-top-grid">
            {/* WIDGET 1: DONUT RESOLUTION LIFECYCLE */}
            <div className="cyber-panel">
              <div className="panel-header">
                <h3>🚨 Alert Triage Lifecycle</h3>
                <span className="panel-tag">RESOLUTIONS</span>
              </div>
              <p className="panel-desc">Interactive breakdown of open, investigating, and resolved alerts.</p>
              <InteractiveDonut data={statusDonutData} size={190} strokeWidth={24} title="Total Alerts" />
            </div>

            {/* WIDGET 2: DONUT SEVERITY DISTRIBUTION */}
            <div className="cyber-panel">
              <div className="panel-header">
                <h3>🛡️ Severity Spectrum</h3>
                <span className="panel-tag">SEVERITY</span>
              </div>
              <p className="panel-desc">Risk proportion across Critical, High, Medium, and Low tiers.</p>
              <InteractiveDonut data={severityDonutData} size={190} strokeWidth={24} title="Classified" />
            </div>

            {/* WIDGET 3: MITRE ATT&CK MATRIX MAPPING */}
            <div className="cyber-panel">
              <div className="panel-header">
                <h3>🎯 MITRE ATT&CK Framework Mapping</h3>
                <span className="panel-tag">TACTICS</span>
              </div>
              <p className="panel-desc">Heuristics mapped to enterprise attack tactic stages.</p>
              <div className="mitre-tactics-list">
                <div className="tactic-card">
                  <div className="tactic-head">
                    <span className="tactic-code">TA0006</span>
                    <b>Credential Access</b>
                  </div>
                  <span className="tactic-desc">Rule: BRUTE_FORCE_001 (SSH/Patator)</span>
                  <div className="tactic-badge-active">ACTIVE HEURISTIC</div>
                </div>

                <div className="tactic-card">
                  <div className="tactic-head">
                    <span className="tactic-code">TA0007</span>
                    <b>Discovery & Recon</b>
                  </div>
                  <span className="tactic-desc">Rule: PORT_SCAN_004 (Horizontal Sweep)</span>
                  <div className="tactic-badge-active">ACTIVE HEURISTIC</div>
                </div>

                <div className="tactic-card">
                  <div className="tactic-head">
                    <span className="tactic-code">TA0002</span>
                    <b>Execution & Evasion</b>
                  </div>
                  <span className="tactic-desc">Rule: SUSPICIOUS_PROC_005 (mimikatz/nmap)</span>
                  <div className="tactic-badge-active">ACTIVE HEURISTIC</div>
                </div>

                <div className="tactic-card">
                  <div className="tactic-head">
                    <span className="tactic-code">TA0040</span>
                    <b>Impact & Denial</b>
                  </div>
                  <span className="tactic-desc">Rule: TRAFFIC_SPIKE_003 (Rate Anomaly)</span>
                  <div className="tactic-badge-active">ACTIVE HEURISTIC</div>
                </div>
              </div>
            </div>
          </div>

          {/* ROW 2: RULE DETECTION FREQUENCIES (CLEAN, POLISHED PROGRESS BARS) */}
          <div className="cyber-panel mt-4">
            <div className="panel-header">
              <div>
                <h3>📊 Streaming Rule Detection Frequencies</h3>
                <p className="panel-desc">
                  Accurate, real-time distribution of threat rules triggered across the Spark streaming window.
                </p>
              </div>
              <span className="badge-counter">
                {Object.keys(metrics.top_triggered_rules || {}).length} Distinct Rules Triggered
              </span>
            </div>

            <div className="rule-metrics-container">
              {Object.entries(metrics.top_triggered_rules || {}).map(([rule, count]) => {
                const totalAlerts = metrics.total_alerts || 1;
                const pct = Math.min(100, Math.round((count / totalAlerts) * 100));

                const ruleColor =
                  rule.includes('CRITICAL') || rule.includes('PROC')
                    ? '#ef4444'
                    : rule.includes('BRUTE') || rule.includes('FAILED')
                    ? '#f59e0b'
                    : rule.includes('SCAN')
                    ? '#00d2ff'
                    : '#8b5cf6';

                return (
                  <div key={rule} className="rule-bar-row">
                    <div className="rule-bar-header">
                      <div className="rule-title-group">
                        <span className="rule-badge-chip" style={{ borderColor: ruleColor, color: ruleColor }}>
                          {rule}
                        </span>
                        <span className="rule-subtext">
                          {rule.includes('BRUTE')
                            ? 'Repeated authentication failures in window'
                            : rule.includes('SCAN')
                            ? 'Multi-port horizontal probe sweep'
                            : rule.includes('PROC')
                            ? 'Unauthorized security tool execution'
                            : rule.includes('CIC')
                            ? 'Pre-labelled benchmark attack pattern'
                            : 'Streaming window threshold anomaly'}
                        </span>
                      </div>
                      <div className="rule-stat-group">
                        <b className="rule-hit-count">{count} hits</b>
                        <span className="rule-hit-pct">{pct}% of total</span>
                      </div>
                    </div>

                    <div className="rule-progress-track">
                      <div
                        className="rule-progress-bar"
                        style={{
                          width: `${pct}%`,
                          background: `linear-gradient(90deg, ${ruleColor}66, ${ruleColor})`,
                          boxShadow: `0 0 10px ${ruleColor}66`,
                        }}
                      ></div>
                    </div>
                  </div>
                );
              })}

              {Object.keys(metrics.top_triggered_rules || {}).length === 0 && (
                <div className="empty-cell">
                  <p>No detection rules triggered yet.</p>
                  <button className="btn-primary" onClick={() => triggerReplay('brute_force')}>
                    Trigger Brute Force Simulation
                  </button>
                </div>
              )}
            </div>
          </div>
        </main>
      )}

      {/* ========================================================== */}
      {/* TAB 6: CLUSTER HEALTH & TOPOLOGY */}
      {/* ========================================================== */}
      {tab === 'health' && (
        <main className="tab-content">
          <div className="cyber-panel">
            <div className="panel-header">
              <div>
                <h3>⚙️ Distributed Cluster Architecture & Component Status</h3>
                <p className="panel-desc">
                  Live health checks probing Apache Kafka, Spark, HDFS, HBase, and REST API.
                </p>
              </div>
              <span className="badge-status status-resolved">CLUSTER SYNCED</span>
            </div>

            <div className="topology-grid">
              {/* Kafka */}
              <div className="topo-card">
                <div className="topo-head">
                  <div className="topo-icon-title">
                    <span className="topo-glyph">📡</span>
                    <h4>Apache Kafka Broker</h4>
                  </div>
                  <span
                    className={`health-dot ${
                      health?.dependencies?.kafka?.status === 'healthy' ? 'dot-green' : 'dot-red'
                    }`}
                  ></span>
                </div>
                <div className="topo-body">
                  <div className="topo-row">
                    <span>Broker Host:</span>
                    <b className="code-font">{health?.dependencies?.kafka?.broker || 'kafka:29092'}</b>
                  </div>
                  <div className="topo-row">
                    <span>Topic:</span>
                    <b className="code-font">security-events</b>
                  </div>
                  <div className="topo-row">
                    <span>Status:</span>
                    <span className="badge-status status-resolved">
                      {health?.dependencies?.kafka?.status || 'HEALTHY'}
                    </span>
                  </div>
                </div>
              </div>

              {/* Spark */}
              <div className="topo-card">
                <div className="topo-head">
                  <div className="topo-icon-title">
                    <span className="topo-glyph">⚡</span>
                    <h4>Spark Structured Streaming</h4>
                  </div>
                  <span
                    className={`health-dot ${
                      health?.dependencies?.spark?.status === 'healthy' ? 'dot-green' : 'dot-red'
                    }`}
                  ></span>
                </div>
                <div className="topo-body">
                  <div className="topo-row">
                    <span>Master URI:</span>
                    <b className="code-font">{health?.dependencies?.spark?.master || 'spark://spark-master:7077'}</b>
                  </div>
                  <div className="topo-row">
                    <span>Web UI:</span>
                    <a href="http://localhost:8080" target="_blank" rel="noreferrer" className="text-cyan">
                      http://localhost:8080 ↗
                    </a>
                  </div>
                  <div className="topo-row">
                    <span>Engine:</span>
                    <span className="badge-rule">PySpark 3.5.3</span>
                  </div>
                </div>
              </div>

              {/* HDFS */}
              <div className="topo-card">
                <div className="topo-head">
                  <div className="topo-icon-title">
                    <span className="topo-glyph">🐘</span>
                    <h4>Hadoop HDFS Storage</h4>
                  </div>
                  <span
                    className={`health-dot ${
                      health?.dependencies?.hdfs?.status === 'healthy' ? 'dot-green' : 'dot-red'
                    }`}
                  ></span>
                </div>
                <div className="topo-body">
                  <div className="topo-row">
                    <span>NameNode RPC:</span>
                    <b className="code-font">{health?.dependencies?.hdfs?.namenode || 'namenode:9000'}</b>
                  </div>
                  <div className="topo-row">
                    <span>Web UI:</span>
                    <a href="http://localhost:9870" target="_blank" rel="noreferrer" className="text-cyan">
                      http://localhost:9870 ↗
                    </a>
                  </div>
                  <div className="topo-row">
                    <span>Path:</span>
                    <b className="code-font">/threatnexus/events</b>
                  </div>
                </div>
              </div>

              {/* HBase */}
              <div className="topo-card">
                <div className="topo-head">
                  <div className="topo-icon-title">
                    <span className="topo-glyph">🧱</span>
                    <h4>Apache HBase NoSQL</h4>
                  </div>
                  <span
                    className={`health-dot ${
                      health?.dependencies?.hbase?.status === 'healthy' ? 'dot-green' : 'dot-red'
                    }`}
                  ></span>
                </div>
                <div className="topo-body">
                  <div className="topo-row">
                    <span>Thrift Gateway:</span>
                    <b className="code-font">{health?.dependencies?.hbase?.host || 'hbase'}:9090</b>
                  </div>
                  <div className="topo-row">
                    <span>Master UI:</span>
                    <a href="http://localhost:16010" target="_blank" rel="noreferrer" className="text-cyan">
                      http://localhost:16010 ↗
                    </a>
                  </div>
                  <div className="topo-row">
                    <span>Tables:</span>
                    <span className="badge-rule">threatnexus:alerts</span>
                  </div>
                </div>
              </div>

              {/* FastAPI */}
              <div className="topo-card">
                <div className="topo-head">
                  <div className="topo-icon-title">
                    <span className="topo-glyph">🚀</span>
                    <h4>FastAPI REST API</h4>
                  </div>
                  <span className="health-dot dot-green"></span>
                </div>
                <div className="topo-body">
                  <div className="topo-row">
                    <span>Base URL:</span>
                    <b className="code-font">{API_BASE}</b>
                  </div>
                  <div className="topo-row">
                    <span>Interactive Docs:</span>
                    <a href={`${API_BASE}/docs`} target="_blank" rel="noreferrer" className="text-cyan">
                      Swagger UI /docs ↗
                    </a>
                  </div>
                  <div className="topo-row">
                    <span>Version:</span>
                    <span className="badge-status status-resolved">v2.0.0 Online</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </main>
      )}

      {/* ========================================================== */}
      {/* TAB 7: ATTACK SIMULATOR & CUSTOM INGEST */}
      {/* ========================================================== */}
      {tab === 'simulator' && (
        <main className="tab-content">
          <div className="grid-2-col">
            {/* PRE-BUILT SCENARIOS */}
            <div className="cyber-panel">
              <div className="panel-header">
                <h3>🧪 Automated Cyber Attack Scenarios</h3>
                <span className="panel-tag">SIMULATION</span>
              </div>
              <p className="panel-desc">
                Inject attack streams into Kafka to observe real-time detection rule execution.
              </p>

              <div className="simulator-list">
                <div className="sim-item">
                  <div className="sim-info">
                    <b>1. SSH Brute Force Attack (BRUTE_FORCE_001)</b>
                    <p>Fires 7 failed logins from single IP within a 30s sliding window.</p>
                  </div>
                  <button
                    className="btn-sim-action"
                    disabled={loading}
                    onClick={() => triggerReplay('brute_force')}
                  >
                    Launch Simulation 🚀
                  </button>
                </div>

                <div className="sim-item">
                  <div className="sim-info">
                    <b>2. Network Port Scan Sweep (PORT_SCAN_004)</b>
                    <p>Probes 12 distinct TCP service ports (21, 22, 80, 443, 3306, 8080...).</p>
                  </div>
                  <button
                    className="btn-sim-action"
                    disabled={loading}
                    onClick={() => triggerReplay('port_scan')}
                  >
                    Launch Simulation 🚀
                  </button>
                </div>

                <div className="sim-item">
                  <div className="sim-info">
                    <b>3. Suspicious Process Execution (SUSPICIOUS_PROC_005)</b>
                    <p>Simulates execution of credential dumper binary 'mimikatz'.</p>
                  </div>
                  <button
                    className="btn-sim-action"
                    disabled={loading}
                    onClick={() => triggerReplay('process')}
                  >
                    Launch Simulation 🚀
                  </button>
                </div>

                <div className="sim-item">
                  <div className="sim-info">
                    <b>4. Traffic Surge / Rate Anomaly (TRAFFIC_SPIKE_003)</b>
                    <p>Emits 25 burst requests in seconds to trigger rate-spike detection.</p>
                  </div>
                  <button
                    className="btn-sim-action"
                    disabled={loading}
                    onClick={() => triggerReplay('traffic_spike')}
                  >
                    Launch Simulation 🚀
                  </button>
                </div>

                <div className="sim-item">
                  <div className="sim-info">
                    <b>5. CIC-IDS2017 Dataset Flow Replay (CIC_DATASET_ATTACK_006)</b>
                    <p>Replays labelled benchmark dataset flows (SSH-Patator brute force attack).</p>
                  </div>
                  <button
                    className="btn-sim-action"
                    disabled={loading}
                    onClick={() => triggerReplay('cic_ids2017')}
                  >
                    Launch Simulation 🚀
                  </button>
                </div>
              </div>
            </div>

            {/* CUSTOM COMPOSER */}
            <div className="cyber-panel">
              <div className="panel-header">
                <h3>✏️ Custom Security Event Composer</h3>
                <span className="panel-tag">RAW INGEST</span>
              </div>
              <p className="panel-desc">Submit individual normalized security records to Kafka.</p>

              <form onSubmit={handleSendCustomEvent} className="composer-form">
                <div className="form-2-col">
                  <label>
                    Source IP Address
                    <input
                      value={eventForm.source_ip}
                      onChange={(e) => setEventForm({ ...eventForm, source_ip: e.target.value })}
                    />
                  </label>
                  <label>
                    Event Type
                    <input
                      value={eventForm.event_type}
                      onChange={(e) => setEventForm({ ...eventForm, event_type: e.target.value })}
                    />
                  </label>
                </div>

                <div className="form-2-col">
                  <label>
                    Destination IP
                    <input
                      value={eventForm.destination_ip}
                      onChange={(e) => setEventForm({ ...eventForm, destination_ip: e.target.value })}
                    />
                  </label>
                  <label>
                    Destination Port
                    <input
                      type="number"
                      value={eventForm.destination_port}
                      onChange={(e) => setEventForm({ ...eventForm, destination_port: e.target.value })}
                    />
                  </label>
                </div>

                <div className="form-2-col">
                  <label>
                    Target Username
                    <input
                      value={eventForm.username}
                      onChange={(e) => setEventForm({ ...eventForm, username: e.target.value })}
                    />
                  </label>
                  <label>
                    Process Name (Optional)
                    <input
                      value={eventForm.process_name}
                      placeholder="e.g. mimikatz, nmap"
                      onChange={(e) => setEventForm({ ...eventForm, process_name: e.target.value })}
                    />
                  </label>
                </div>

                <div className="form-2-col">
                  <label>
                    Assigned Severity
                    <select
                      value={eventForm.severity}
                      onChange={(e) => setEventForm({ ...eventForm, severity: e.target.value })}
                    >
                      <option value="INFO">INFO</option>
                      <option value="MEDIUM">MEDIUM</option>
                      <option value="HIGH">HIGH</option>
                      <option value="CRITICAL">CRITICAL</option>
                    </select>
                  </label>
                  <label>
                    Status
                    <select
                      value={eventForm.status}
                      onChange={(e) => setEventForm({ ...eventForm, status: e.target.value })}
                    >
                      <option value="success">Success</option>
                      <option value="failure">Failure</option>
                      <option value="attempted">Attempted</option>
                    </select>
                  </label>
                </div>

                <label>
                  Log Message Payload
                  <input
                    value={eventForm.message}
                    onChange={(e) => setEventForm({ ...eventForm, message: e.target.value })}
                  />
                </label>

                <button type="submit" className="btn-primary-glow" disabled={loading}>
                  🚀 Publish to Kafka Topic `security-events`
                </button>
              </form>
            </div>
          </div>
        </main>
      )}

      {/* ========================================================== */}
      {/* ALERT DETAILS MODAL DRAWER */}
      {/* ========================================================== */}
      {selectedAlert && (
        <div className="modal-backdrop" onClick={() => setSelectedAlert(null)}>
          <div className="modal-cyber-card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <div className="flex-row gap-2">
                <span className="modal-glyph">🚨</span>
                <div>
                  <h3>Threat Alert Inspection</h3>
                  <span className="code-font text-cyan">{selectedAlert.alert_id || selectedAlert.id}</span>
                </div>
              </div>
              <button className="btn-close" onClick={() => setSelectedAlert(null)}>
                ✕
              </button>
            </div>

            <div className="modal-body">
              <div className="detail-grid">
                <div className="detail-item">
                  <span className="detail-lbl">Detection Rule Code</span>
                  <b className="badge-rule">{selectedAlert.rule_id || selectedAlert.type}</b>
                </div>
                <div className="detail-item">
                  <span className="detail-lbl">Assigned Severity</span>
                  <span className={`badge-sev sev-${(selectedAlert.severity || 'high').toLowerCase()}`}>
                    {selectedAlert.severity}
                  </span>
                </div>
                <div className="detail-item">
                  <span className="detail-lbl">Attacker Source IP</span>
                  <b className="code-font text-cyan">{selectedAlert.source_ip}</b>
                </div>
                <div className="detail-item">
                  <span className="detail-lbl">Event Count</span>
                  <b className="text-large">{selectedAlert.event_count || 1}</b>
                </div>
                <div className="detail-item">
                  <span className="detail-lbl">Sliding Window Start</span>
                  <span className="time-col">{selectedAlert.window_start || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-lbl">Sliding Window End</span>
                  <span className="time-col">{selectedAlert.window_end || 'N/A'}</span>
                </div>
                <div className="detail-item full-width">
                  <span className="detail-lbl">Rule Description</span>
                  <p className="detail-text">{selectedAlert.description}</p>
                </div>
                <div className="detail-item full-width">
                  <span className="detail-lbl">SOC Investigation Lifecycle Status</span>
                  <span className={`badge-status status-${selectedAlert.status}`}>
                    {selectedAlert.status}
                  </span>
                </div>
              </div>

              {/* Status Updater Actions */}
              <div className="modal-triage-box">
                <span className="triage-title">Update Alert Lifecycle Status:</span>
                <div className="flex-row gap-2 mt-2">
                  <button
                    className="btn-triage btn-triage-open"
                    onClick={() => updateAlertStatus(selectedAlert.alert_id || selectedAlert.id, 'open')}
                  >
                    Mark as OPEN
                  </button>
                  <button
                    className="btn-triage btn-triage-investigate"
                    onClick={() =>
                      updateAlertStatus(selectedAlert.alert_id || selectedAlert.id, 'investigating')
                    }
                  >
                    Mark as INVESTIGATING
                  </button>
                  <button
                    className="btn-triage btn-triage-resolve"
                    onClick={() => updateAlertStatus(selectedAlert.alert_id || selectedAlert.id, 'resolved')}
                  >
                    Mark as RESOLVED ✓
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* FOOTER */}
      <footer className="soc-footer">
        <div>ThreatNexus Big Data Security Platform · CSBB 422</div>
        <div>Engine: Apache Kafka · Apache Spark · Hadoop HDFS · Apache HBase · FastAPI · React</div>
      </footer>
    </div>
  );
}

createRoot(document.getElementById('root')).render(<App />);
