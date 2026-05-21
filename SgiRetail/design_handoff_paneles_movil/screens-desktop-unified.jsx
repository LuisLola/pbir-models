// Desktop dashboard · BI2GO-style single-view with expandable tree-table
// + date navigator (end-date only, backward navigation)

const { StatusPill: SPB, statusFromPct: spStatusB, A_TOKENS: ATB, ChannelIcon: ChIconB } = window;

const dbFont = "'Manrope', -apple-system, system-ui, sans-serif";
const dbMono = "'JetBrains Mono', 'SF Mono', ui-monospace, monospace";

// ─────────────────────────────────────────────────────────────
// Date navigator — end-date only + arrows + period type selector
// ─────────────────────────────────────────────────────────────
function DateNavigator() {
  const periodTypes = [
    { id: 'year',  label: 'Año',     value: '2026' },
    { id: 'month', label: 'Mes',     value: 'May.' },
    { id: 'week',  label: 'Semana',  value: 'S 21' },
    { id: 'day',   label: 'Día',     value: '19/05' },
  ];
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      {/* Period type selector */}
      <div style={{
        display: 'inline-flex', background: ATB.cream,
        border: `1px solid ${ATB.line}`, borderRadius: 8, padding: 2,
      }}>
        {periodTypes.map((p, i) => (
          <button key={p.id} style={{
            padding: '6px 10px', borderRadius: 6, border: 'none',
            background: i === 2 ? ATB.paper : 'transparent',
            color: i === 2 ? ATB.ink : ATB.mute,
            fontFamily: dbFont, fontSize: 11, fontWeight: 700,
            letterSpacing: '0.04em', cursor: 'pointer',
            boxShadow: i === 2 ? `0 1px 3px rgba(10,15,30,0.08)` : 'none',
            display: 'flex', flexDirection: 'column', alignItems: 'center',
            minWidth: 38, lineHeight: 1.1,
          }}>
            <span style={{ fontSize: 9, opacity: 0.6, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{p.label}</span>
            <span style={{ fontFamily: dbMono, marginTop: 2 }}>{p.value}</span>
          </button>
        ))}
      </div>

      {/* Date navigator */}
      <div style={{
        display: 'inline-flex', alignItems: 'center',
        background: ATB.paper, border: `1px solid ${ATB.line}`, borderRadius: 8,
        overflow: 'hidden',
      }}>
        <button style={{
          width: 32, height: 32, background: 'transparent', border: 'none',
          color: ATB.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
          borderRight: `1px solid ${ATB.line}`,
        }} title="Periodo anterior">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M9 2 L4 7 L9 12" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
        <div style={{
          padding: '0 14px', display: 'flex', alignItems: 'center', gap: 8,
          fontFamily: dbMono, fontSize: 12, color: ATB.ink, fontWeight: 600, height: 32,
        }}>
          <svg width="13" height="13" viewBox="0 0 13 13" fill="none" style={{ color: ATB.mute }}><rect x="1" y="3" width="11" height="9" rx="1" stroke="currentColor" strokeWidth="1.2"/><path d="M1 6 H12 M4 1 V4 M9 1 V4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/></svg>
          <span>19 may. 2026</span>
        </div>
        <button style={{
          width: 32, height: 32, background: 'transparent', border: 'none',
          color: ATB.line, cursor: 'not-allowed', display: 'flex', alignItems: 'center', justifyContent: 'center',
          borderLeft: `1px solid ${ATB.line}`,
        }} title="Periodo siguiente · sin datos" disabled>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M5 2 L10 7 L5 12" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// Mini KPI with sparkline + 4 time-bucket evolution stamps
// ─────────────────────────────────────────────────────────────
function MiniKpi({ label, value, prefix, suffix, sparkline, sparklinePy, buckets, primary = false }) {
  // sparkline: array of values 0-100
  const points = sparkline || [];
  const pyPoints = sparklinePy || [];
  const max = Math.max(...points, ...pyPoints, 1);
  const w = 200, h = 56;
  const toPath = (arr) => arr.map((v, i) => `${i === 0 ? 'M' : 'L'} ${(i / (arr.length - 1)) * w} ${h - (v / max) * h}`).join(' ');

  return (
    <div style={{
      flex: 1, minWidth: 0,
      background: primary ? ATB.navy : ATB.paper,
      border: primary ? 'none' : `1px solid ${ATB.line}`,
      borderRadius: 12, padding: '16px 18px 12px',
      color: primary ? '#fff' : ATB.ink,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12, marginBottom: 8 }}>
        <div>
          <div style={{
            fontSize: 11, fontWeight: 700, letterSpacing: '0.10em',
            textTransform: 'uppercase', opacity: primary ? 0.6 : 1,
            color: primary ? '#fff' : ATB.mute, marginBottom: 6,
          }}>{label}</div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 3 }}>
            {prefix && <span style={{ fontSize: 14, opacity: 0.7 }}>{prefix}</span>}
            <span style={{ fontSize: 28, fontWeight: 800, letterSpacing: '-0.02em', lineHeight: 1 }}>{value}</span>
            {suffix && <span style={{ fontSize: 13, opacity: 0.7, marginLeft: 2 }}>{suffix}</span>}
          </div>
        </div>
        {/* sparkline */}
        <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} style={{ flexShrink: 0 }}>
          {pyPoints.length > 0 && (
            <path d={toPath(pyPoints)} stroke={primary ? 'rgba(255,255,255,0.3)' : ATB.mute} strokeWidth="1.5" strokeDasharray="3 3" fill="none" />
          )}
          <path d={toPath(points)} stroke={primary ? ATB.pink : ATB.ink} strokeWidth="2" fill="none" strokeLinecap="round" strokeLinejoin="round" />
          {/* end dot */}
          <circle cx={(points.length - 1) * (w / (points.length - 1 || 1))} cy={h - (points[points.length - 1] / max) * h} r="3.5" fill={primary ? ATB.pink : ATB.pink} />
        </svg>
      </div>

      {/* Time-bucket evolutions */}
      <div style={{
        display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 6,
        paddingTop: 10, borderTop: `1px solid ${primary ? 'rgba(255,255,255,0.12)' : ATB.line}`,
      }}>
        {buckets.map((b, i) => {
          const status = spStatusB(b.pct);
          const c = status === 'pos' ? ATB.pos : status === 'warn' ? ATB.warn : ATB.neg;
          const bg = status === 'pos' ? ATB.posBg : status === 'warn' ? ATB.warnBg : ATB.negBg;
          const sign = b.pct >= 0 ? '+' : '';
          return (
            <div key={i} style={{ textAlign: 'center' }}>
              <div style={{
                fontSize: 9, fontWeight: 700, letterSpacing: '0.06em',
                textTransform: 'uppercase',
                color: primary ? 'rgba(255,255,255,0.5)' : ATB.mute,
                fontFamily: dbMono, marginBottom: 4,
              }}>{b.label}</div>
              <div style={{
                display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                padding: '3px 6px', borderRadius: 4,
                background: bg, color: c,
                fontSize: 10, fontWeight: 700, fontFamily: dbMono,
              }}>{sign}{b.pct}%</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// Tree table — single view with expandable rows
// ─────────────────────────────────────────────────────────────
const TREE = [
  {
    id: 'tienda', tipo: 'TIENDA', share: 40, expandable: true, expanded: false,
    ventas: '22.015', obj: '37.950', objPct: -42, py: '28.571', pyPct: -23,
    devol: '13,0%', evolDevol: 0, nVentas: '211', evolN: -12, tm: '104', evolTm: -12,
    upt: '1,2', evolUpt: -8, trafico: '—', evolTraf: '-100p', atraccion: '—', evolAtr: -2,
    entradas: '—', evolEnt: '-100%', conv: '—', evolConv: -10.0,
  },
  {
    id: 'corner', tipo: 'CORNER ECI', share: 36, expandable: true, expanded: false,
    ventas: '19.745', obj: '33.350', objPct: -41, py: '26.082', pyPct: -24,
    devol: '19,6%', evolDevol: -2, nVentas: '199', evolN: -36, tm: '99', evolTm: 19,
    upt: '1,0', evolUpt: 11, trafico: '—', evolTraf: 0, atraccion: '—', evolAtr: 0,
    entradas: '—', evolEnt: 0, conv: '—', evolConv: 0,
  },
  {
    id: 'online', tipo: 'ONLINE', share: 18, expandable: true, expanded: true, level: 0,
    ventas: '9.983', obj: '13.520', objPct: -26, py: '17.088', pyPct: -42,
    devol: '16,0%', evolDevol: 14, nVentas: '77', evolN: -44, tm: '154', evolTm: 21,
    upt: '1,6', evolUpt: 10, trafico: '—', evolTraf: 0, atraccion: '—', evolAtr: 0,
    entradas: '—', evolEnt: 0, conv: '—', evolConv: 0,
    children: [
      {
        id: 'prestashop', tienda: 'Tienda Web (Prestashop)', share: 73, expandable: true, expanded: true, level: 1,
        ventas: '7.280', obj: '10.400', objPct: -30, py: '13.487', pyPct: -46,
        devol: '20,7%', evolDevol: 18, nVentas: '62', evolN: -39, tm: '148', evolTm: 9,
        upt: '1,5', evolUpt: -2, trafico: '—', evolTraf: 0, atraccion: '—', evolAtr: 0,
        entradas: '—', evolEnt: 0, conv: '—', evolConv: 0,
        children: [
          { id: 'prenda',   categoria: 'Prenda',     share: 66, level: 2, ventas: '4.789', obj: '0',     objPct: 0, py: '9.685', pyPct: -51, devol: '26,1%', evolDevol: 23, nVentas: '0', evolN: 0, tm: '0', evolTm: 0, upt: '0', evolUpt: 0, trafico: '—', evolTraf: 0, atraccion: '—', evolAtr: 0, entradas: '—', evolEnt: 0, conv: '—', evolConv: 0 },
          { id: 'accesorio', categoria: 'Accesorio',  share: 34, level: 2, ventas: '2.491', obj: '0',     objPct: 0, py: '3.802', pyPct: -34, devol: '7,7%',  evolDevol: 5,  nVentas: '0', evolN: 0, tm: '0', evolTm: 0, upt: '0', evolUpt: 0, trafico: '—', evolTraf: 0, atraccion: '—', evolAtr: 0, entradas: '—', evolEnt: 0, conv: '—', evolConv: 0 },
          { id: 'null',     categoria: 'NULL',       share: 0,  level: 2, ventas: '—',     obj: '10.400', objPct: -100, py: '—', pyPct: 0, devol: '—', evolDevol: 0, nVentas: '62', evolN: -39, tm: '0', evolTm: 0, upt: '0,0', evolUpt: 0, trafico: '—', evolTraf: 0, atraccion: '—', evolAtr: 0, entradas: '—', evolEnt: 0, conv: '—', evolConv: 0 },
        ],
      },
      { id: 'eci-online', tienda: 'ECI Online', share: 27, level: 1, ventas: '2.703', obj: '3.120', objPct: -13, py: '3.601', pyPct: -25, devol: '0,0%', evolDevol: 0, nVentas: '15', evolN: -57, tm: '180', evolTm: 75, upt: '1,8', evolUpt: 66, trafico: '—', evolTraf: 0, atraccion: '—', evolAtr: 0, entradas: '—', evolEnt: 0, conv: '—', evolConv: 0 },
    ],
  },
  {
    id: 'outlet', tipo: 'OUTLET', share: 6, level: 0, expandable: true,
    ventas: '3.270', obj: '11.180', objPct: -71, py: '8.099', pyPct: -60,
    devol: '10,2%', evolDevol: 1, nVentas: '37', evolN: -62, tm: '88', evolTm: 7,
    upt: '1,2', evolUpt: -15, trafico: '—', evolTraf: '-100%', atraccion: '—', evolAtr: -12,
    entradas: '—', evolEnt: '-100%', conv: '—', evolConv: -7.9,
  },
];

// Flatten tree for rendering, respecting "expanded" state
function flatten(nodes, level = 0, acc = []) {
  for (const n of nodes) {
    acc.push({ ...n, level: n.level !== undefined ? n.level : level });
    if (n.expanded && n.children) {
      flatten(n.children, level + 1, acc);
    }
  }
  return acc;
}

function PctCell({ pct, kind = '%' }) {
  if (typeof pct === 'string') {
    // Already-formatted (e.g. "-100p")
    return <span style={{
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
      padding: '3px 6px', borderRadius: 4,
      background: ATB.negBg, color: ATB.neg,
      fontSize: 11, fontWeight: 700, fontFamily: dbMono,
    }}>{pct}</span>;
  }
  const status = spStatusB(pct);
  const c = status === 'pos' ? ATB.pos : status === 'warn' ? ATB.warn : ATB.neg;
  const bg = status === 'pos' ? ATB.posBg : status === 'warn' ? ATB.warnBg : ATB.negBg;
  const sign = pct >= 0 ? '+' : '';
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 4,
      padding: '3px 7px', borderRadius: 4,
      background: bg, color: c,
      fontSize: 11, fontWeight: 700, fontFamily: dbMono,
      whiteSpace: 'nowrap',
    }}>
      <span style={{ width: 5, height: 5, borderRadius: 3, background: c }}></span>
      {sign}{pct}{kind}
    </span>
  );
}

function ShareBar({ pct }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 78 }}>
      <div style={{ flex: 1, height: 4, background: ATB.line, borderRadius: 2, overflow: 'hidden', maxWidth: 50 }}>
        <div style={{ width: Math.min(100, pct) + '%', height: '100%', background: ATB.pink }}></div>
      </div>
      <span style={{ fontSize: 11, color: ATB.mute, fontFamily: dbMono, fontWeight: 600, minWidth: 26, textAlign: 'right' }}>{pct}%</span>
    </div>
  );
}

function TreeTable() {
  const flat = flatten(TREE);

  const cellNum = (v, weight = 600, fontSize = 12) => (
    <td style={{ padding: '12px 10px', textAlign: 'right', fontSize, fontWeight: weight, color: ATB.ink, fontFamily: dbMono, whiteSpace: 'nowrap' }}>{v}</td>
  );
  const cellPct = (pct, kind = '%') => (
    <td style={{ padding: '12px 6px', textAlign: 'right', whiteSpace: 'nowrap' }}><PctCell pct={pct} kind={kind} /></td>
  );
  const cellHeader = (v, sub) => (
    <th style={{
      padding: '12px 10px', textAlign: 'right',
      fontSize: 10, fontWeight: 700, color: ATB.mute,
      letterSpacing: '0.06em', textTransform: 'uppercase',
      borderBottom: `1px solid ${ATB.line}`,
      whiteSpace: 'nowrap', background: ATB.cream, verticalAlign: 'bottom',
    }}>
      {v}
      {sub && <div style={{ fontSize: 9, fontWeight: 500, color: ATB.mute, opacity: 0.7, textTransform: 'none', letterSpacing: 0, marginTop: 2, fontFamily: dbMono }}>{sub}</div>}
    </th>
  );

  return (
    <div style={{ background: ATB.paper, border: `1px solid ${ATB.line}`, borderRadius: 12, overflow: 'hidden' }}>
      {/* Header chain */}
      <div style={{
        padding: '14px 20px',
        display: 'flex', alignItems: 'center', gap: 0,
        borderBottom: `1px solid ${ATB.line}`,
        flexWrap: 'wrap', background: ATB.paper,
      }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: ATB.mute, letterSpacing: '0.08em', textTransform: 'uppercase', marginRight: 14 }}>Desempeño por</div>
        {['Tipo Tienda', 'Tienda', 'Categoría', 'Familia agregada'].map((label, i, arr) => (
          <React.Fragment key={label}>
            <button style={{
              display: 'inline-flex', alignItems: 'center', gap: 6,
              padding: '6px 10px', borderRadius: 6,
              background: i === 0 ? ATB.pinkSoft : 'transparent',
              border: 'none',
              color: i === 0 ? ATB.pink : ATB.ink,
              fontSize: 12, fontWeight: 700, cursor: 'pointer',
              fontFamily: dbFont,
            }}>
              {label}
              <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M2 3.5 L5 6.5 L8 3.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
            </button>
            {i < arr.length - 1 && (
              <span style={{ color: ATB.line, margin: '0 4px', fontSize: 12 }}>|</span>
            )}
          </React.Fragment>
        ))}

        <div style={{ marginLeft: 'auto', display: 'flex', gap: 6, alignItems: 'center' }}>
          <button style={{ width: 30, height: 30, borderRadius: 6, background: ATB.cream, border: `1px solid ${ATB.line}`, color: ATB.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }} title="Expandir todo">
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M3 4 L6 7 L9 4 M3 8 L6 11 L9 8" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </button>
          <button style={{ width: 30, height: 30, borderRadius: 6, background: ATB.cream, border: `1px solid ${ATB.line}`, color: ATB.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }} title="Colapsar todo">
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M3 7 L6 4 L9 7 M3 11 L6 8 L9 11" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </button>
          <button style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '6px 10px', borderRadius: 6, background: ATB.cream, border: `1px solid ${ATB.line}`, fontSize: 11, fontWeight: 600, color: ATB.body, cursor: 'pointer' }}>
            <svg width="11" height="11" viewBox="0 0 11 11" fill="none"><path d="M1 2 H10 L7 6 V10 L4 9 V6 Z" stroke="currentColor" strokeWidth="1.3" strokeLinejoin="round"/></svg>
            Filtros
          </button>
          <button style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '6px 10px', borderRadius: 6, background: ATB.cream, border: `1px solid ${ATB.line}`, fontSize: 11, fontWeight: 600, color: ATB.body, cursor: 'pointer' }}>
            <svg width="11" height="11" viewBox="0 0 11 11" fill="none"><path d="M1.5 5.5 L4.5 8.5 L9.5 2.5" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round"/></svg>
            Exportar
          </button>
        </div>
      </div>

      {/* Table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', width: '100%', minWidth: 1700, fontFamily: dbFont }}>
          <thead>
            <tr>
              <th style={{
                padding: '12px 20px', textAlign: 'left',
                fontSize: 10, fontWeight: 700, color: ATB.mute,
                letterSpacing: '0.06em', textTransform: 'uppercase',
                borderBottom: `1px solid ${ATB.line}`,
                background: ATB.cream, width: 320,
              }}>Tipo · Tienda · Categoría · Familia</th>
              <th style={{
                padding: '12px 10px', textAlign: 'left',
                fontSize: 10, fontWeight: 700, color: ATB.mute,
                letterSpacing: '0.06em', textTransform: 'uppercase',
                borderBottom: `1px solid ${ATB.line}`,
                background: ATB.cream, width: 110,
              }}>Share</th>
              {cellHeader('Venta neta', 's21 2026')}
              {cellHeader('Objetivo')}
              {cellHeader('Evol. %', 'vs OBJ')}
              {cellHeader('Venta neta', 's21 2025')}
              {cellHeader('Evol. %', 'vs PY')}
              {cellHeader('Tasa Dev.')}
              {cellHeader('Evol. p')}
              {cellHeader('Nº ventas')}
              {cellHeader('Evol. %', 'nº ventas')}
              {cellHeader('Ticket M.')}
              {cellHeader('Evol. %', 'TM')}
              {cellHeader('UPT')}
              {cellHeader('Evol. %', 'UPT')}
              {cellHeader('% Atrac.')}
              {cellHeader('% Conv.')}
            </tr>
          </thead>
          <tbody>
            {/* TOTALES row */}
            <tr style={{ background: ATB.cream, borderBottom: `2px solid ${ATB.ink}` }}>
              <td style={{ padding: '14px 20px', fontSize: 12, fontWeight: 800, color: ATB.ink, letterSpacing: '0.10em', textTransform: 'uppercase' }}>Totales</td>
              <td style={{ padding: '14px 10px' }}><ShareBar pct={100} /></td>
              {cellNum('55.013', 800, 14)}
              {cellNum('96.000', 700)}
              {cellPct(-43)}
              {cellNum('79.840', 700)}
              {cellPct(-31)}
              {cellNum('15,9%', 700)}
              {cellPct(2, 'p')}
              {cellNum('524', 700)}
              {cellPct(-34)}
              {cellNum('109', 700)}
              {cellPct(7)}
              {cellNum('1,2', 700)}
              {cellPct(-1)}
              {cellPct(-100)}
              {cellPct(-9.3, 'p')}
            </tr>

            {/* Data rows */}
            {flat.map((r, i) => {
              const isExpandable = r.expandable;
              const expanded = r.expanded;
              const indent = r.level * 18;
              const isParent = r.level === 0;
              const isLeaf = r.level === 2;

              const label =
                isParent ? r.tipo :
                r.level === 1 ? r.tienda :
                r.categoria;

              const labelColor = isParent ? ATB.ink : isLeaf ? ATB.body : ATB.ink;
              const labelWeight = isParent ? 800 : 600;
              const labelLetterSpacing = isParent ? '0.06em' : 'normal';
              const labelTransform = isParent ? 'uppercase' : 'none';
              const labelSize = isParent ? 12 : 12;

              const rowBg = r.level === 0 ? 'transparent' :
                            r.level === 1 ? 'rgba(225,26,111,0.025)' :
                            'rgba(225,26,111,0.05)';

              return (
                <tr key={r.id + '-' + i} style={{
                  background: rowBg,
                  borderBottom: `1px solid ${ATB.line}`,
                }}>
                  <td style={{ padding: '12px 20px', paddingLeft: 20 + indent }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {isExpandable ? (
                        <button style={{
                          width: 18, height: 18, borderRadius: 4,
                          background: expanded ? ATB.pink : 'transparent',
                          border: `1px solid ${expanded ? ATB.pink : ATB.line}`,
                          color: expanded ? '#fff' : ATB.body, cursor: 'pointer',
                          display: 'flex', alignItems: 'center', justifyContent: 'center',
                          padding: 0, flexShrink: 0,
                        }}>
                          <svg width="9" height="9" viewBox="0 0 9 9" fill="none">
                            {expanded
                              ? <path d="M2 4.5 H7" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round"/>
                              : <path d="M2 4.5 H7 M4.5 2 V7" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round"/>
                            }
                          </svg>
                        </button>
                      ) : <div style={{ width: 18, flexShrink: 0 }}></div>}

                      {isParent && (
                        <div style={{
                          width: 22, height: 22, borderRadius: 5,
                          background: ATB.pinkSoft, color: ATB.pink,
                          display: 'flex', alignItems: 'center', justifyContent: 'center',
                          flexShrink: 0,
                        }}>
                          <ChIconB id={r.id === 'corner' ? 'corner' : r.id === 'online' ? 'online' : r.id === 'outlet' ? 'mkt' : 'tienda'} size={12} />
                        </div>
                      )}
                      <span style={{
                        fontSize: labelSize, fontWeight: labelWeight, color: labelColor,
                        letterSpacing: labelLetterSpacing, textTransform: labelTransform,
                      }}>{label}</span>
                    </div>
                  </td>
                  <td style={{ padding: '12px 10px' }}><ShareBar pct={r.share} /></td>
                  {cellNum(r.ventas, isParent ? 800 : 600, isParent ? 13 : 12)}
                  {cellNum(r.obj)}
                  {cellPct(r.objPct)}
                  {cellNum(r.py)}
                  {cellPct(r.pyPct)}
                  {cellNum(r.devol)}
                  {cellPct(r.evolDevol, 'p')}
                  {cellNum(r.nVentas)}
                  {cellPct(r.evolN)}
                  {cellNum(r.tm)}
                  {cellPct(r.evolTm)}
                  {cellNum(r.upt)}
                  {cellPct(r.evolUpt)}
                  {typeof r.evolAtr === 'number' ? cellPct(r.evolAtr) : cellPct(r.evolAtr || 0)}
                  {typeof r.evolConv === 'number' ? cellPct(r.evolConv) : cellPct(r.evolConv || 0)}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Footer */}
      <div style={{
        padding: '12px 20px',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        borderTop: `1px solid ${ATB.line}`,
        background: ATB.cream,
        fontSize: 11, color: ATB.mute,
      }}>
        <span>Mostrando 4 grupos · 2 expandidos · {flat.length} filas totales</span>
        <span style={{ fontFamily: dbMono }}>Actualizado · 19 may. 2026 · 09:41</span>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// Main desktop screen — single view, BI2GO style
// ─────────────────────────────────────────────────────────────
function DesktopUnifiedA() {
  const spark1 = [50, 60, 45, 70, 80, 65, 85, 70, 90, 78, 95, 60];
  const spark1py = [55, 65, 60, 75, 78, 72, 85, 75, 82, 70, 88, 75];
  const spark2 = [80, 82, 85, 79, 88, 90, 92, 87, 89, 91, 95, 92];
  const spark2py = [75, 77, 80, 76, 83, 86, 88, 85, 87, 89, 92, 90];
  const spark3 = [70, 72, 75, 71, 73, 76, 78, 75, 77, 74, 78, 73];
  const spark3py = [68, 70, 72, 70, 71, 74, 76, 73, 75, 72, 76, 74];

  return (
    <div style={{
      width: 1440, minHeight: 980, background: '#F4F1EA',
      fontFamily: dbFont, color: ATB.ink,
    }}>
      {/* Top header */}
      <div style={{
        background: ATB.paper, borderBottom: `1px solid ${ATB.line}`,
        padding: '14px 36px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <div style={{
              width: 36, height: 36, borderRadius: 8,
              background: ATB.pink, color: '#fff',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontWeight: 800, fontSize: 16, letterSpacing: '-0.02em',
            }}>LC</div>
            <div>
              <div style={{ fontSize: 14, fontWeight: 700, color: ATB.ink, display: 'flex', alignItems: 'center', gap: 8 }}>
                Desempeño de ventas
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none" style={{ color: ATB.pink }}><path d="M7 12 C2 9, 1 5, 3.5 3 C5 1.5, 6 2, 7 3.5 C8 2, 9 1.5, 10.5 3 C13 5, 12 9, 7 12 Z" stroke="currentColor" strokeWidth="1.4" fill="none"/></svg>
              </div>
              <div style={{ fontSize: 11, color: ATB.mute, marginTop: 1 }}>Vista jerárquica · expande por dimensión</div>
            </div>
          </div>
        </div>

        <DateNavigator />
      </div>

      {/* KPI strip */}
      <div style={{ padding: '20px 36px', display: 'flex', gap: 12 }}>
        <MiniKpi
          label="Venta neta con IVA" value="55.013" prefix="€" primary
          sparkline={spark1} sparklinePy={spark1py}
          buckets={[
            { label: '2026',   pct: 24 },
            { label: 'May.',   pct: 11 },
            { label: 'S 21',   pct: -18 },
            { label: '19/05',  pct: -31 },
          ]}
        />
        <MiniKpi
          label="Ticket Medio" value="108,61" prefix="€"
          sparkline={spark2} sparklinePy={spark2py}
          buckets={[
            { label: '2026',   pct: 5 },
            { label: 'May.',   pct: 6 },
            { label: 'S 21',   pct: 7 },
            { label: '19/05',  pct: 7 },
          ]}
        />
        <MiniKpi
          label="UPT" value="1,2"
          sparkline={spark3} sparklinePy={spark3py}
          buckets={[
            { label: '2026',   pct: 0 },
            { label: 'May.',   pct: 0 },
            { label: 'S 21',   pct: 0 },
            { label: '19/05',  pct: -1 },
          ]}
        />
        <MiniKpi
          label="% Conversión" value="15,9" suffix="%"
          sparkline={[60, 62, 58, 65, 67, 63, 70, 68, 72, 69, 75, 71]}
          sparklinePy={[58, 60, 62, 64, 65, 67, 68, 69, 70, 71, 72, 73]}
          buckets={[
            { label: '2026',   pct: 2 },
            { label: 'May.',   pct: 0 },
            { label: 'S 21',   pct: -1 },
            { label: '19/05',  pct: -3 },
          ]}
        />
      </div>

      {/* Tree table */}
      <div style={{ padding: '0 36px 28px' }}>
        <TreeTable />
      </div>

      {/* Annotation */}
      <div style={{
        padding: '14px 36px', background: ATB.paper,
        borderTop: `1px solid ${ATB.line}`,
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        fontSize: 11, color: ATB.mute,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 8, height: 8, borderRadius: 4, background: ATB.pos }}></span> Positivo · ≥ 0
          </span>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 8, height: 8, borderRadius: 4, background: ATB.warn }}></span> Atención · entre −10% y 0
          </span>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 8, height: 8, borderRadius: 4, background: ATB.neg }}></span> Crítico · &lt; −10%
          </span>
        </div>
        <span>Lola Casademunt · Cuadro de mando comercial</span>
      </div>
    </div>
  );
}

Object.assign(window, { DesktopUnifiedA });
