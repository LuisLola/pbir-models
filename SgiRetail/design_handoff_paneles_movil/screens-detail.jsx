// Channel detail + element detail screens — Variación A
// Reuses tokens and StatusPill from screens.jsx

const { StatusPill, statusFromPct, A_TOKENS } = window;

// ─────────────────────────────────────────────────────────────
// Channel detail data (per-channel breakdown)
// ─────────────────────────────────────────────────────────────
const TIENDAS = [
  { id: 'rambla',   nombre: 'LC Barcelona Rambla',         ventas: '4.388', obj: '4.030', objPct: 109, pyPct: 106, py: '2.126', completado: 109 },
  { id: 'andorra',  nombre: 'LC Andorra',                  ventas: '2.085', obj: '3.000', objPct: -31, pyPct: 104, py: '1.020', completado: 69  },
  { id: 'illa',     nombre: "LC Barcelona L'Illa Diagonal", ventas: '2.095', obj: '2.220', objPct: -6,  pyPct: 20,  py: '1.741', completado: 94  },
  { id: 'bilbao',   nombre: 'LC Bilbao Rodriguez Arias',   ventas: '1.923', obj: '2.100', objPct: -8,  pyPct: 5,   py: '1.830', completado: 92  },
  { id: 'aero',     nombre: 'LC Barcelona Aeropuerto T1',  ventas: '1.295', obj: '2.350', objPct: -45, pyPct: -38, py: '2.081', completado: 55  },
  { id: 'almeria',  nombre: 'LC Almería Reyes Católicos',  ventas: '979',   obj: '2.110', objPct: -54, pyPct: -44, py: '1.739', completado: 46  },
  { id: 'mad',      nombre: 'LC Madrid Serrano',           ventas: '892',   obj: '1.800', objPct: -50, pyPct: -22, py: '1.144', completado: 50  },
  { id: 'val',      nombre: 'LC Valencia Colón',           ventas: '745',   obj: '1.400', objPct: -47, pyPct: -15, py: '877',   completado: 53  },
];

// Channel info used in the header of detail screens
const CHANNEL_INFO = {
  tienda: {
    nombre: 'Tienda', icon: 'tienda',
    ventas: '57.914', obj: '59.800', objPct: -3, pyPct: 10, py: '52.575',
    tm: '135,63', upt: '1,5', conv: '15,38%', atrac: '2,23%',
    nVentas: '427', items: TIENDAS, itemLabel: 'tiendas',
  },
  corner: {
    nombre: 'Corner ECI', icon: 'corner',
    ventas: '85.991', obj: '93.520', objPct: -8, pyPct: -5, py: '90.860',
    tm: '91,38', upt: '1,1', conv: '11,9%', atrac: null,
    nVentas: '941', items: null, itemLabel: 'corners',
  },
  online: {
    nombre: 'Online', icon: 'online',
    ventas: '17.783', obj: '25.000', objPct: -29, pyPct: -16, py: '21.168',
    tm: '138,37', upt: '1,4', conv: '11,4%', atrac: null,
    nVentas: '159', items: null, itemLabel: 'fuentes',
  },
  mkt: {
    nombre: 'Marketplaces', icon: 'mkt',
    ventas: '13.053', obj: '24.237', objPct: -46, pyPct: -22, py: '16.685',
    tm: '114,15', upt: '0,8', conv: '61,8%', atrac: null,
    nVentas: '460', items: null, itemLabel: 'marketplaces',
  },
};

// ─────────────────────────────────────────────────────────────
// Channel detail screen — Variación A
// ─────────────────────────────────────────────────────────────
function ChannelDetailA({ channelId = 'tienda' }) {
  const ch = CHANNEL_INFO[channelId];
  const completado = Math.round((parseFloat(ch.ventas.replace('.', '').replace(',', '.')) / parseFloat(ch.obj.replace('.', '').replace(',', '.'))) * 100);
  const items = ch.items || [];

  return (
    <div style={{
      width: 390, minHeight: 1700, background: A_TOKENS.paper,
      fontFamily: "'Manrope', system-ui, sans-serif", color: A_TOKENS.ink,
      paddingTop: 62, paddingBottom: 80,
    }}>
      {/* Header — slim brand bar with breadcrumb */}
      <div style={{
        background: A_TOKENS.pink, color: '#fff',
        padding: '10px 16px 12px',
        display: 'flex', alignItems: 'center', gap: 12,
      }}>
        <button style={{
          width: 28, height: 28, borderRadius: 14,
          background: 'rgba(255,255,255,0.18)', border: 'none', color: '#fff',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 0, cursor: 'pointer', flexShrink: 0,
        }}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M9 2 L4 7 L9 12" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
        <div style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
          <div style={{ fontSize: 9, opacity: 0.7, fontWeight: 600, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: 1 }}>Lola Casademunt · Canal</div>
          <div style={{ fontSize: 14, fontWeight: 800, letterSpacing: '0.04em', textTransform: 'uppercase' }}>{ch.nombre}</div>
        </div>
        <div style={{
          background: 'rgba(255,255,255,0.18)', padding: '5px 9px', borderRadius: 100,
          fontFamily: "'JetBrains Mono', monospace", fontSize: 10, fontWeight: 600,
        }}>
          {items.length || ch.nVentas} {items.length ? ch.itemLabel : 'ventas'}
        </div>
      </div>

      {/* Period */}
      <div style={{
        padding: '12px 20px',
        display: 'flex', alignItems: 'center', gap: 10,
        background: A_TOKENS.cream, borderBottom: `1px solid ${A_TOKENS.line}`,
      }}>
        <div style={{ fontSize: 10, color: A_TOKENS.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>Periodo</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: A_TOKENS.ink, fontWeight: 600 }}>
          <span>01/12/2025</span>
          <span style={{ color: A_TOKENS.mute }}>→</span>
          <span>12/12/2025</span>
        </div>
      </div>

      {/* Hero KPI */}
      <div style={{ background: A_TOKENS.navy, color: '#fff', padding: '22px 20px 20px' }}>
        <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.18em', textTransform: 'uppercase', color: '#fff', opacity: 0.55, marginBottom: 6 }}>Venta Neta · {ch.nombre}</div>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 14 }}>
          <span style={{ fontSize: 14, opacity: 0.7 }}>€</span>
          <span style={{ fontSize: 38, fontWeight: 800, letterSpacing: '-0.02em', lineHeight: 1 }}>{ch.ventas}</span>
        </div>

        {/* progress */}
        <div style={{ marginBottom: 12 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, opacity: 0.7, marginBottom: 6 }}>
            <span>vs Objetivo · {completado}% completado</span>
            <span style={{ fontFamily: "'JetBrains Mono', monospace" }}>€ {ch.obj}</span>
          </div>
          <div style={{ height: 4, background: 'rgba(255,255,255,0.15)', borderRadius: 2, overflow: 'hidden' }}>
            <div style={{ width: Math.min(100, completado) + '%', height: '100%', background: A_TOKENS.pink }}></div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <StatusPill pct={ch.objPct} label="vs OBJ" />
          <StatusPill pct={ch.pyPct} label="vs PY" />
        </div>
      </div>

      {/* Secondary KPI strip — 4 columns */}
      <div style={{
        padding: '0 20px',
        display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)',
        borderBottom: `1px solid ${A_TOKENS.line}`,
      }}>
        {[
          { label: 'Ticket M.', value: ch.tm },
          { label: 'UPT', value: ch.upt },
          { label: 'Conv.', value: ch.conv },
          { label: 'Atrac.', value: ch.atrac || '—' },
        ].map((k, i) => (
          <div key={i} style={{
            padding: '14px 0',
            borderRight: i < 3 ? `1px solid ${A_TOKENS.line}` : 'none',
            paddingLeft: i > 0 ? 10 : 0,
            paddingRight: i < 3 ? 10 : 0,
          }}>
            <div style={{ fontSize: 9, color: A_TOKENS.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 4 }}>{k.label}</div>
            <div style={{ fontSize: 14, fontWeight: 700, color: A_TOKENS.ink, letterSpacing: '-0.01em' }}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* Sort / filter strip */}
      <div style={{
        padding: '14px 20px 8px',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: A_TOKENS.ink }}>
          {items.length ? `${ch.itemLabel.charAt(0).toUpperCase()}${ch.itemLabel.slice(1)}` : 'Detalle'}
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          <button style={{
            display: 'inline-flex', alignItems: 'center', gap: 4,
            padding: '6px 10px', borderRadius: 100,
            background: A_TOKENS.cream, border: `1px solid ${A_TOKENS.line}`,
            fontSize: 11, fontWeight: 600, color: A_TOKENS.body,
            cursor: 'pointer',
          }}>
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M1 2 H9 M2 5 H8 M3 8 H7" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round"/></svg>
            Ventas
            <svg width="8" height="8" viewBox="0 0 8 8" fill="none"><path d="M2 3 L4 5 L6 3" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </button>
          <button style={{
            display: 'inline-flex', alignItems: 'center', gap: 4,
            padding: '6px 10px', borderRadius: 100,
            background: A_TOKENS.cream, border: `1px solid ${A_TOKENS.line}`,
            fontSize: 11, fontWeight: 600, color: A_TOKENS.body,
            cursor: 'pointer',
          }}>
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M1 1 L9 1 L6 5 V9 L4 8 V5 Z" stroke="currentColor" strokeWidth="1.3" strokeLinejoin="round"/></svg>
            Filtrar
          </button>
        </div>
      </div>

      {/* List of items */}
      {items.length > 0 ? (
        <div style={{ padding: '0 20px' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {items.map((t) => {
              const status = statusFromPct(t.objPct);
              const barColor = status === 'pos' ? A_TOKENS.pos : status === 'warn' ? A_TOKENS.warn : A_TOKENS.neg;
              return (
                <div key={t.id} style={{
                  background: A_TOKENS.paper,
                  border: `1px solid ${A_TOKENS.line}`,
                  borderRadius: 10,
                  padding: '12px 14px',
                  display: 'flex', flexDirection: 'column', gap: 9,
                  cursor: 'pointer',
                }}>
                  {/* Row 1: name + chevron + amount */}
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
                    <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, flex: 1 }}>
                      <div style={{ fontSize: 13, fontWeight: 700, color: A_TOKENS.ink, lineHeight: 1.2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {t.nombre}
                      </div>
                      <div style={{ fontSize: 10, color: A_TOKENS.mute, marginTop: 3, fontFamily: "'JetBrains Mono', monospace" }}>
                        {t.completado}% obj · OBJ € {t.obj}
                      </div>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <div style={{ textAlign: 'right' }}>
                        <div style={{ fontSize: 10, color: A_TOKENS.mute, marginBottom: 1 }}>€</div>
                        <div style={{ fontSize: 17, fontWeight: 700, color: A_TOKENS.ink, letterSpacing: '-0.01em', lineHeight: 1 }}>{t.ventas}</div>
                      </div>
                      <svg width="10" height="14" viewBox="0 0 10 14" fill="none" style={{ color: A_TOKENS.mute, flexShrink: 0 }}><path d="M3 2 L8 7 L3 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
                    </div>
                  </div>

                  {/* Progress bar */}
                  <div style={{ height: 3, background: A_TOKENS.line, borderRadius: 2, overflow: 'hidden' }}>
                    <div style={{ width: Math.min(100, t.completado) + '%', height: '100%', background: barColor }}></div>
                  </div>

                  {/* Pills */}
                  <div style={{ display: 'flex', gap: 6 }}>
                    <StatusPill pct={t.objPct} label="vs OBJ" />
                    <StatusPill pct={t.pyPct} label="vs PY" />
                  </div>
                </div>
              );
            })}
          </div>
          <div style={{ textAlign: 'center', padding: '20px 0 0', fontSize: 11, color: A_TOKENS.mute }}>
            Mostrando {items.length} de {items.length} {ch.itemLabel}
          </div>
        </div>
      ) : (
        <div style={{
          margin: '0 20px',
          padding: '40px 20px',
          background: A_TOKENS.cream,
          borderRadius: 10,
          textAlign: 'center',
          fontSize: 12, color: A_TOKENS.body,
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.1em', textTransform: 'uppercase', color: A_TOKENS.mute, marginBottom: 6 }}>Sin desglose</div>
          Este canal no tiene desglose por {ch.itemLabel} en este periodo.
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// Single store detail (3rd level)
// ─────────────────────────────────────────────────────────────
function StoreDetailA({ storeId = 'illa' }) {
  const t = TIENDAS.find(x => x.id === storeId) || TIENDAS[2];
  const c = A_TOKENS;

  return (
    <div style={{
      width: 390, minHeight: 1500, background: c.paper,
      fontFamily: "'Manrope', system-ui, sans-serif", color: c.ink,
      paddingTop: 62, paddingBottom: 80,
    }}>
      {/* Header */}
      <div style={{
        background: c.pink, color: '#fff',
        padding: '10px 16px 12px',
        display: 'flex', alignItems: 'center', gap: 12,
      }}>
        <button style={{
          width: 28, height: 28, borderRadius: 14,
          background: 'rgba(255,255,255,0.18)', border: 'none', color: '#fff',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 0, cursor: 'pointer', flexShrink: 0,
        }}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M9 2 L4 7 L9 12" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
        <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 9, opacity: 0.7, fontWeight: 600, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: 1 }}>Tienda · Detalle</div>
          <div style={{ fontSize: 13, fontWeight: 800, letterSpacing: '0.02em', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{t.nombre}</div>
        </div>
      </div>

      {/* Period */}
      <div style={{
        padding: '12px 20px',
        display: 'flex', alignItems: 'center', gap: 10,
        background: c.cream, borderBottom: `1px solid ${c.line}`,
      }}>
        <div style={{ fontSize: 10, color: c.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>Periodo</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: c.ink, fontWeight: 600 }}>
          <span>01/12/2025</span>
          <span style={{ color: c.mute }}>→</span>
          <span>12/12/2025</span>
        </div>
      </div>

      {/* Hero */}
      <div style={{ background: c.navy, color: '#fff', padding: '22px 20px 20px' }}>
        <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.18em', textTransform: 'uppercase', color: '#fff', opacity: 0.55, marginBottom: 6 }}>Venta Neta</div>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 14 }}>
          <span style={{ fontSize: 14, opacity: 0.7 }}>€</span>
          <span style={{ fontSize: 42, fontWeight: 800, letterSpacing: '-0.02em', lineHeight: 1 }}>{t.ventas}</span>
        </div>

        {/* progress */}
        <div style={{ marginBottom: 12 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, opacity: 0.7, marginBottom: 6 }}>
            <span>vs Objetivo · {t.completado}% completado</span>
            <span style={{ fontFamily: "'JetBrains Mono', monospace" }}>€ {t.obj}</span>
          </div>
          <div style={{ height: 4, background: 'rgba(255,255,255,0.15)', borderRadius: 2, overflow: 'hidden' }}>
            <div style={{ width: Math.min(100, t.completado) + '%', height: '100%', background: c.pink }}></div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <StatusPill pct={t.objPct} label="vs OBJ" />
          <StatusPill pct={t.pyPct} label="vs PY" />
        </div>
      </div>

      {/* KPI grid */}
      <div style={{
        padding: '0 20px',
        display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)',
        borderBottom: `1px solid ${c.line}`,
      }}>
        {[
          { label: 'Ticket Medio', value: '€ 142,30' },
          { label: 'UPT', value: '1,6' },
          { label: '% Conversión', value: '16,2%' },
          { label: '% Atracción', value: '2,8%' },
        ].map((k, i) => (
          <div key={i} style={{
            padding: '16px 0',
            borderRight: i % 2 === 0 ? `1px solid ${c.line}` : 'none',
            borderBottom: i < 2 ? `1px solid ${c.line}` : 'none',
            paddingLeft: i % 2 === 1 ? 18 : 0,
            paddingRight: i % 2 === 0 ? 18 : 0,
          }}>
            <div style={{ fontSize: 10, color: c.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 6 }}>{k.label}</div>
            <div style={{ fontSize: 22, fontWeight: 700, color: c.ink, letterSpacing: '-0.01em' }}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* Evolution mini chart placeholder */}
      <div style={{ padding: '24px 20px 0' }}>
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 14 }}>
          <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: c.ink }}>Evolución · 12 días</div>
          <div style={{ fontSize: 10, color: c.mute }}>Ventas diarias</div>
        </div>
        <div style={{
          background: c.paper, border: `1px solid ${c.line}`, borderRadius: 10,
          padding: 16, height: 160,
          display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 4,
        }}>
          {[40, 55, 35, 70, 85, 60, 90, 75, 88, 65, 95, 80].map((h, i) => (
            <div key={i} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', flex: 1, gap: 4 }}>
              <div style={{
                width: '100%', maxWidth: 18,
                height: h + 'px',
                background: i === 10 ? c.pink : c.navy,
                borderRadius: 2,
              }}></div>
              <div style={{ fontSize: 8, color: c.mute, fontFamily: "'JetBrains Mono', monospace" }}>{String(i + 1).padStart(2, '0')}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Last year comparison */}
      <div style={{ padding: '24px 20px 0' }}>
        <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: c.ink, marginBottom: 14 }}>Comparativa</div>
        <div style={{
          background: c.paper, border: `1px solid ${c.line}`, borderRadius: 10,
          padding: 16,
        }}>
          <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            paddingBottom: 14, marginBottom: 14, borderBottom: `1px solid ${c.line}`,
          }}>
            <div>
              <div style={{ fontSize: 10, color: c.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>Periodo actual</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: c.ink, marginTop: 4 }}>€ {t.ventas}</div>
            </div>
            <StatusPill pct={t.pyPct} label="vs PY" />
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div style={{ fontSize: 10, color: c.mute, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>Mismo periodo 2024</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: c.mute, marginTop: 4 }}>€ {t.py}</div>
            </div>
            <div style={{ fontSize: 10, color: c.mute, fontFamily: "'JetBrains Mono', monospace" }}>LastYear</div>
          </div>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { ChannelDetailA, StoreDetailA, CHANNEL_INFO, TIENDAS });
