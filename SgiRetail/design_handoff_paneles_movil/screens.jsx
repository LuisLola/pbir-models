// Mobile PowerBI dashboard mockups for Lola Casademunt
// Two variations: A (Conservadora) and B (Moderna)

// ─────────────────────────────────────────────────────────────
// Shared data (matches the desktop PowerBI panel)
// ─────────────────────────────────────────────────────────────
const TOTAL = {
  ventas: '174.741',
  objetivo: '202.557',
  objPct: -14,
  pyPct: -4,
  py: '181.287',
  tm: '109,33',
  upt: '1,1',
  conv: '13,85%',
  atrac: '2,23%',
  fechaIni: '01/12/2025',
  fechaFin: '12/12/2025',
};

const CANALES = [
  { id: 'tienda',     nombre: 'Tienda',       ventas: '57.914', obj: '59.800', objPct: -3,  pyPct: 10,  py: '52.575', share: 33 },
  { id: 'corner',     nombre: 'Corner ECI',   ventas: '85.991', obj: '93.520', objPct: -8,  pyPct: -5,  py: '90.860', share: 49 },
  { id: 'online',     nombre: 'Online',       ventas: '17.783', obj: '25.000', objPct: -29, pyPct: -16, py: '21.168', share: 10 },
  { id: 'mkt',        nombre: 'Marketplaces', ventas: '13.053', obj: '24.237', objPct: -46, pyPct: -22, py: '16.685', share: 8  },
];

// Status helpers
const statusFromPct = (pct) => {
  if (pct >= 0) return 'pos';
  if (pct >= -10) return 'warn';
  return 'neg';
};

// ─────────────────────────────────────────────────────────────
// Tiny SVG glyphs (channel icons — geometric, no emojis)
// ─────────────────────────────────────────────────────────────
const ChannelIcon = ({ id, size = 16, color = 'currentColor' }) => {
  const s = size, c = color;
  if (id === 'tienda') return (
    <svg width={s} height={s} viewBox="0 0 16 16" fill="none"><path d="M2 6 L3 3 H13 L14 6" stroke={c} strokeWidth="1.3"/><path d="M3 6 V13 H13 V6" stroke={c} strokeWidth="1.3"/><path d="M7 13 V9 H9 V13" stroke={c} strokeWidth="1.3"/></svg>
  );
  if (id === 'corner') return (
    <svg width={s} height={s} viewBox="0 0 16 16" fill="none"><rect x="2" y="2" width="5" height="12" stroke={c} strokeWidth="1.3"/><rect x="9" y="5" width="5" height="9" stroke={c} strokeWidth="1.3"/></svg>
  );
  if (id === 'online') return (
    <svg width={s} height={s} viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6" stroke={c} strokeWidth="1.3"/><path d="M2 8 H14 M8 2 C 10.5 5, 10.5 11, 8 14 M8 2 C 5.5 5, 5.5 11, 8 14" stroke={c} strokeWidth="1.3"/></svg>
  );
  if (id === 'mkt') return (
    <svg width={s} height={s} viewBox="0 0 16 16" fill="none"><path d="M2 6 L8 2 L14 6 V13 H2 Z" stroke={c} strokeWidth="1.3"/><path d="M6 13 V9 H10 V13" stroke={c} strokeWidth="1.3"/></svg>
  );
  return null;
};

// ─────────────────────────────────────────────────────────────
// Variación A — Conservadora (refinada)
// Mantiene rosa fucsia + navy, pero con jerarquía clara
// ─────────────────────────────────────────────────────────────
const A = {
  pink: '#E11A6F',
  pinkSoft: '#FBE6EF',
  navy: '#10182F',
  navy2: '#1B2546',
  ink: '#0A0F1E',
  body: '#3A4055',
  mute: '#8089A2',
  line: '#E4E7F0',
  paper: '#FFFFFF',
  cream: '#F7F6F2',
  pos: '#1F8A5B',
  posBg: '#E3F4EB',
  warn: '#B57A0E',
  warnBg: '#FAF1DD',
  neg: '#C73838',
  negBg: '#FBE6E6',
};

const aFont = "'Manrope', -apple-system, system-ui, sans-serif";
const aMono = "'JetBrains Mono', 'SF Mono', ui-monospace, monospace";

function StatusPill({ pct, label, theme = 'A' }) {
  const t = theme === 'A' ? A : B;
  const status = statusFromPct(pct);
  const c = status === 'pos' ? t.pos : status === 'warn' ? t.warn : t.neg;
  const bg = status === 'pos' ? t.posBg : status === 'warn' ? t.warnBg : t.negBg;
  const sign = pct >= 0 ? '+' : '';
  return (
    <div style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      padding: '4px 8px', borderRadius: 6,
      background: bg, color: c,
      fontFamily: theme === 'A' ? aFont : bFont,
      fontWeight: 700, fontSize: 11, letterSpacing: '0.02em',
      lineHeight: 1, whiteSpace: 'nowrap',
    }}>
      <span style={{ width: 6, height: 6, borderRadius: 3, background: c }}></span>
      {sign}{pct}% {label}
    </div>
  );
}

function ProgressDots({ pct, theme = 'A' }) {
  // pct relative to objective (negative means below)
  const filled = Math.max(0, Math.min(10, Math.round((100 + pct) / 10)));
  const t = theme === 'A' ? A : B;
  const c = pct >= 0 ? t.pos : pct >= -10 ? t.warn : t.neg;
  return (
    <div style={{ display: 'flex', gap: 2 }}>
      {Array.from({ length: 10 }).map((_, i) => (
        <div key={i} style={{
          width: 5, height: 5, borderRadius: 2.5,
          background: i < filled ? c : (theme === 'A' ? A.line : B.line),
        }} />
      ))}
    </div>
  );
}

function VariationA() {
  return (
    <div style={{
      width: 390, minHeight: 1400, background: A.paper,
      fontFamily: aFont, color: A.ink,
      paddingTop: 62, paddingBottom: 80,
    }}>
      {/* Header — slim brand bar */}
      <div style={{
        background: A.pink, color: '#fff',
        padding: '10px 20px 12px',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <button style={{
          width: 28, height: 28, borderRadius: 14,
          background: 'rgba(255,255,255,0.18)', border: 'none', color: '#fff',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 0, cursor: 'pointer',
        }}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M9 2 L4 7 L9 12" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
        <div style={{
          fontFamily: aFont, fontWeight: 700, fontSize: 13,
          letterSpacing: '0.18em', textTransform: 'uppercase',
        }}>Lola Casademunt</div>
        <div style={{ width: 28 }}></div>
      </div>

      {/* Date range */}
      <div style={{
        padding: '14px 20px 12px',
        display: 'flex', alignItems: 'center', gap: 10,
        background: A.cream, borderBottom: `1px solid ${A.line}`,
      }}>
        <div style={{ fontSize: 11, color: A.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>Periodo</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontFamily: aMono, fontSize: 12, color: A.ink, fontWeight: 600 }}>
          <span>{TOTAL.fechaIni}</span>
          <span style={{ color: A.mute }}>→</span>
          <span>{TOTAL.fechaFin}</span>
        </div>
      </div>

      {/* Hero KPI — Total */}
      <div style={{
        background: A.navy, color: '#fff',
        padding: '24px 20px 22px',
        position: 'relative',
      }}>
        <div style={{
          fontSize: 10, fontWeight: 700, letterSpacing: '0.18em',
          textTransform: 'uppercase', color: '#fff', opacity: 0.55, marginBottom: 6,
        }}>Venta Neta · Total</div>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 16 }}>
          <span style={{ fontSize: 14, opacity: 0.7 }}>€</span>
          <span style={{ fontSize: 42, fontWeight: 800, letterSpacing: '-0.02em', lineHeight: 1 }}>{TOTAL.ventas}</span>
        </div>
        {/* progress vs obj */}
        <div style={{ marginBottom: 14 }}>
          <div style={{
            display: 'flex', justifyContent: 'space-between',
            fontSize: 11, opacity: 0.7, marginBottom: 6,
          }}>
            <span>vs Objetivo · 86% completado</span>
            <span style={{ fontFamily: aMono }}>€ {TOTAL.objetivo}</span>
          </div>
          <div style={{ height: 4, background: 'rgba(255,255,255,0.15)', borderRadius: 2, overflow: 'hidden' }}>
            <div style={{ width: '86%', height: '100%', background: A.pink }}></div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <StatusPill pct={TOTAL.objPct} label="vs OBJ" />
          <StatusPill pct={TOTAL.pyPct} label="vs PY" />
        </div>
      </div>

      {/* Secondary KPI grid */}
      <div style={{
        padding: '0 20px',
        display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)',
        borderBottom: `1px solid ${A.line}`,
      }}>
        {[
          { label: 'Ticket Medio', value: '€ ' + TOTAL.tm },
          { label: 'UPT', value: TOTAL.upt },
          { label: '% Conversión', value: TOTAL.conv },
          { label: '% Atracción', value: TOTAL.atrac },
        ].map((k, i) => (
          <div key={i} style={{
            padding: '16px 0',
            borderRight: i % 2 === 0 ? `1px solid ${A.line}` : 'none',
            borderBottom: i < 2 ? `1px solid ${A.line}` : 'none',
            paddingLeft: i % 2 === 1 ? 18 : 0,
            paddingRight: i % 2 === 0 ? 18 : 0,
          }}>
            <div style={{ fontSize: 10, color: A.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 6 }}>{k.label}</div>
            <div style={{ fontSize: 22, fontWeight: 700, color: A.ink, letterSpacing: '-0.01em' }}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* Channels section */}
      <div style={{ padding: '24px 20px 0' }}>
        <div style={{
          display: 'flex', alignItems: 'baseline', justifyContent: 'space-between',
          marginBottom: 14,
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: A.ink }}>Por canal</div>
          <div style={{ fontSize: 11, color: A.mute }}>4 canales</div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {CANALES.map((c) => {
            const completado = Math.round((parseFloat(c.ventas.replace('.', '').replace(',', '.')) / parseFloat(c.obj.replace('.', '').replace(',', '.'))) * 100);
            return (
              <div key={c.id} style={{
                background: A.paper,
                border: `1px solid ${A.line}`,
                borderRadius: 10,
                padding: 14,
              }}>
                {/* Top row */}
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <div style={{
                      width: 28, height: 28, borderRadius: 6,
                      background: A.pinkSoft, color: A.pink,
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}>
                      <ChannelIcon id={c.id} size={14} />
                    </div>
                    <div style={{ fontWeight: 700, fontSize: 14, color: A.ink }}>{c.nombre}</div>
                  </div>
                  <div style={{ fontSize: 10, color: A.mute, fontFamily: aMono }}>{c.share}% del total</div>
                </div>

                {/* Ventas */}
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 4, marginBottom: 10 }}>
                  <span style={{ fontSize: 11, color: A.mute }}>€</span>
                  <span style={{ fontSize: 26, fontWeight: 700, letterSpacing: '-0.02em', color: A.ink }}>{c.ventas}</span>
                  <span style={{ marginLeft: 8, fontSize: 11, color: A.mute }}>Ventas</span>
                </div>

                {/* Progress vs obj */}
                <div style={{ marginBottom: 12 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: A.mute, marginBottom: 4 }}>
                    <span>{completado}% del objetivo</span>
                    <span style={{ fontFamily: aMono }}>OBJ € {c.obj}</span>
                  </div>
                  <div style={{ height: 3, background: A.line, borderRadius: 2, overflow: 'hidden' }}>
                    <div style={{
                      width: Math.min(100, completado) + '%', height: '100%',
                      background: c.objPct >= 0 ? A.pos : c.objPct >= -10 ? A.warn : A.neg,
                    }}></div>
                  </div>
                </div>

                {/* Status pills row */}
                <div style={{ display: 'flex', gap: 6 }}>
                  <StatusPill pct={c.objPct} label="vs OBJ" />
                  <StatusPill pct={c.pyPct} label="vs PY" />
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Footer hint */}
      <div style={{
        marginTop: 24, padding: '14px 20px',
        textAlign: 'center', fontSize: 10, color: A.mute,
        letterSpacing: '0.08em', textTransform: 'uppercase', fontWeight: 600,
      }}>
        Actualizado · 12 Dic 2025 · 09:41
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// Variación B — Moderna (editorial fashion)
// Cream + pink + navy, tipografía mixta serif/sans, mucho aire
// ─────────────────────────────────────────────────────────────
const B = {
  pink: '#D8336B',
  pinkSoft: '#F7E2EA',
  navy: '#1A1F36',
  ink: '#0B0F1F',
  body: '#3F4458',
  mute: '#8F8A82',
  line: '#E8E2D8',
  paper: '#FAF6EF',
  paper2: '#FFFFFF',
  pos: '#2D7A4F',
  posBg: '#E1EFE6',
  warn: '#A56B14',
  warnBg: '#F4E9D3',
  neg: '#B83232',
  negBg: '#F4DCDC',
};

const bFont = "'Manrope', -apple-system, system-ui, sans-serif";
const bSerif = "'Instrument Serif', 'Cormorant Garamond', Georgia, serif";
const bMono = "'JetBrains Mono', 'SF Mono', ui-monospace, monospace";

function VariationB() {
  return (
    <div style={{
      width: 390, minHeight: 1400, background: B.paper,
      fontFamily: bFont, color: B.ink,
      paddingTop: 62, paddingBottom: 80,
    }}>
      {/* Header — minimal wordmark */}
      <div style={{
        padding: '16px 24px 14px',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        borderBottom: `1px solid ${B.line}`,
      }}>
        <button style={{
          width: 32, height: 32, borderRadius: 16,
          background: 'transparent', border: `1px solid ${B.line}`, color: B.ink,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 0, cursor: 'pointer',
        }}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M9 2 L4 7 L9 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
        <div style={{
          fontFamily: bSerif, fontSize: 19, fontStyle: 'italic',
          color: B.ink, letterSpacing: '0.01em',
        }}>Lola Casademunt</div>
        <button style={{
          width: 32, height: 32, borderRadius: 16,
          background: 'transparent', border: `1px solid ${B.line}`, color: B.ink,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 0, cursor: 'pointer',
        }}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <circle cx="3" cy="7" r="1" fill="currentColor"/>
            <circle cx="7" cy="7" r="1" fill="currentColor"/>
            <circle cx="11" cy="7" r="1" fill="currentColor"/>
          </svg>
        </button>
      </div>

      {/* Period chip */}
      <div style={{ padding: '20px 24px 10px', display: 'flex', alignItems: 'center', gap: 10 }}>
        <div style={{
          display: 'inline-flex', alignItems: 'center', gap: 8,
          padding: '8px 12px', borderRadius: 100,
          background: B.paper2, border: `1px solid ${B.line}`,
          fontFamily: bMono, fontSize: 11, color: B.ink, fontWeight: 500,
        }}>
          <svg width="11" height="11" viewBox="0 0 11 11" fill="none"><rect x="1" y="2.5" width="9" height="7.5" rx="1" stroke="currentColor" strokeWidth="1"/><path d="M1 5 H10 M3.5 1 V3.5 M7.5 1 V3.5" stroke="currentColor" strokeWidth="1" strokeLinecap="round"/></svg>
          {TOTAL.fechaIni} – {TOTAL.fechaFin}
        </div>
        <div style={{ fontSize: 11, color: B.mute, marginLeft: 'auto' }}>12 días</div>
      </div>

      {/* Hero — Editorial */}
      <div style={{ padding: '14px 24px 28px' }}>
        <div style={{
          fontFamily: bSerif, fontStyle: 'italic',
          fontSize: 15, color: B.body, marginBottom: 4,
        }}>Venta neta total</div>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 4, marginBottom: 18 }}>
          <span style={{ fontSize: 18, color: B.body, fontWeight: 500 }}>€</span>
          <span style={{ fontSize: 56, fontWeight: 700, letterSpacing: '-0.035em', lineHeight: 0.95, color: B.ink }}>{TOTAL.ventas}</span>
        </div>

        {/* Status row */}
        <div style={{ display: 'flex', gap: 8, marginBottom: 22 }}>
          <StatusPill pct={TOTAL.objPct} label="vs OBJ" theme="B" />
          <StatusPill pct={TOTAL.pyPct} label="vs PY" theme="B" />
        </div>

        {/* Progress bar to objective — editorial style */}
        <div>
          <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
            marginBottom: 8,
          }}>
            <div style={{ fontSize: 11, color: B.body, fontWeight: 600, letterSpacing: '0.04em', textTransform: 'uppercase' }}>Objetivo · 86%</div>
            <div style={{ fontFamily: bMono, fontSize: 11, color: B.mute }}>€ {TOTAL.objetivo}</div>
          </div>
          <div style={{ height: 6, background: B.paper2, border: `1px solid ${B.line}`, borderRadius: 100, overflow: 'hidden', padding: 1 }}>
            <div style={{ width: '86%', height: '100%', background: B.pink, borderRadius: 100 }}></div>
          </div>
        </div>
      </div>

      {/* Secondary KPI grid */}
      <div style={{ padding: '0 24px 28px' }}>
        <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.16em', textTransform: 'uppercase', color: B.mute, marginBottom: 12 }}>Indicadores</div>
        <div style={{
          display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)',
          gap: 8,
        }}>
          {[
            { label: 'Ticket Medio', value: TOTAL.tm, unit: '€' },
            { label: 'UPT', value: TOTAL.upt, unit: '' },
            { label: 'Conversión', value: TOTAL.conv, unit: '' },
            { label: 'Atracción', value: TOTAL.atrac, unit: '' },
          ].map((k, i) => (
            <div key={i} style={{
              background: B.paper2,
              border: `1px solid ${B.line}`,
              borderRadius: 14, padding: '14px 14px 12px',
            }}>
              <div style={{ fontSize: 10, color: B.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 8 }}>{k.label}</div>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 3 }}>
                {k.unit && <span style={{ fontSize: 12, color: B.body }}>{k.unit}</span>}
                <span style={{ fontSize: 24, fontWeight: 700, color: B.ink, letterSpacing: '-0.02em' }}>{k.value}</span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Channels — editorial cards */}
      <div style={{ padding: '0 24px' }}>
        <div style={{
          display: 'flex', alignItems: 'baseline', justifyContent: 'space-between',
          marginBottom: 16, paddingTop: 4,
          borderTop: `1px solid ${B.line}`, paddingTop: 24,
        }}>
          <div style={{
            fontFamily: bSerif, fontStyle: 'italic',
            fontSize: 22, color: B.ink,
          }}>Por canal</div>
          <div style={{ fontSize: 11, color: B.mute, fontFamily: bMono }}>04 / 04</div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {CANALES.map((c, idx) => {
            const completado = Math.round((parseFloat(c.ventas.replace('.', '').replace(',', '.')) / parseFloat(c.obj.replace('.', '').replace(',', '.'))) * 100);
            const status = statusFromPct(c.objPct);
            const barColor = status === 'pos' ? B.pos : status === 'warn' ? B.warn : B.neg;
            return (
              <div key={c.id} style={{
                background: B.paper2,
                borderRadius: 16,
                padding: '18px 18px 16px',
                border: `1px solid ${B.line}`,
              }}>
                {/* Header */}
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <div style={{ color: B.ink }}>
                      <ChannelIcon id={c.id} size={16} />
                    </div>
                    <div style={{
                      fontFamily: bFont, fontWeight: 600,
                      fontSize: 13, letterSpacing: '0.08em',
                      textTransform: 'uppercase', color: B.ink,
                    }}>{c.nombre}</div>
                  </div>
                  <div style={{
                    fontFamily: bMono, fontSize: 10, color: B.mute,
                    background: B.paper, padding: '3px 7px', borderRadius: 100,
                    border: `1px solid ${B.line}`,
                  }}>{String(idx + 1).padStart(2, '0')}</div>
                </div>

                {/* Big number */}
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 4, marginBottom: 4 }}>
                  <span style={{ fontSize: 13, color: B.body, fontWeight: 500 }}>€</span>
                  <span style={{ fontSize: 32, fontWeight: 700, letterSpacing: '-0.02em', color: B.ink, lineHeight: 1 }}>{c.ventas}</span>
                </div>
                <div style={{
                  fontFamily: bSerif, fontStyle: 'italic',
                  fontSize: 13, color: B.mute, marginBottom: 14,
                }}>Ventas del periodo</div>

                {/* Mini progress */}
                <div style={{ marginBottom: 14 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 6 }}>
                    <span style={{ fontSize: 10, fontWeight: 700, color: B.body, letterSpacing: '0.06em', textTransform: 'uppercase' }}>{completado}% obj</span>
                    <span style={{ fontFamily: bMono, fontSize: 10, color: B.mute }}>€ {c.obj}</span>
                  </div>
                  <div style={{ height: 4, background: B.paper, border: `1px solid ${B.line}`, borderRadius: 100, overflow: 'hidden' }}>
                    <div style={{
                      width: Math.min(100, completado) + '%', height: '100%',
                      background: barColor, borderRadius: 100,
                    }}></div>
                  </div>
                </div>

                {/* Metric rows */}
                <div style={{
                  display: 'grid', gridTemplateColumns: '1fr 1fr',
                  gap: 10, paddingTop: 12,
                  borderTop: `1px solid ${B.line}`,
                }}>
                  <div>
                    <div style={{ fontSize: 9, color: B.mute, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 4 }}>vs Objetivo</div>
                    <StatusPill pct={c.objPct} label="" theme="B" />
                  </div>
                  <div>
                    <div style={{ fontSize: 9, color: B.mute, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 4 }}>vs Año Anterior</div>
                    <StatusPill pct={c.pyPct} label="" theme="B" />
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        {/* Footer */}
        <div style={{
          marginTop: 28, paddingBottom: 12,
          textAlign: 'center', fontFamily: bSerif, fontStyle: 'italic',
          fontSize: 12, color: B.mute,
        }}>
          Actualizado · 12 Dic 2025
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { VariationA, VariationB, StatusPill, statusFromPct, ChannelIcon, A_TOKENS: A, B_TOKENS: B });
