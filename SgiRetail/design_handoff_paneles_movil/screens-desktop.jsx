// Desktop dashboard mockups · Conservadora
// Reuses tokens, StatusPill, statusFromPct from screens.jsx

const { StatusPill: SP, statusFromPct: spStatus, A_TOKENS: AT, ChannelIcon: ChIcon } = window;

const dFont = "'Manrope', -apple-system, system-ui, sans-serif";
const dMono = "'JetBrains Mono', 'SF Mono', ui-monospace, monospace";

// Filter chip
function FilterChip({ label, value, active = false }) {
  return (
    <div style={{
      display: 'inline-flex', alignItems: 'center', gap: 8,
      padding: '8px 14px', borderRadius: 8,
      background: AT.paper,
      border: `1px solid ${active ? AT.pink : AT.line}`,
      cursor: 'pointer',
      fontFamily: dFont, minWidth: 0,
    }}>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <span style={{ fontSize: 9, color: AT.mute, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', lineHeight: 1, whiteSpace: 'nowrap' }}>{label}</span>
        <span style={{ fontSize: 12, color: AT.ink, fontWeight: 600, marginTop: 3, lineHeight: 1, whiteSpace: 'nowrap' }}>{value}</span>
      </div>
      <svg width="10" height="10" viewBox="0 0 10 10" fill="none" style={{ color: AT.mute, marginLeft: 6 }}><path d="M2 3.5 L5 6.5 L8 3.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
    </div>
  );
}

// Hero KPI card (desktop)
function HeroKPI({ label, value, prefix, suffix, primary = false, pillPct, pillLabel }) {
  return (
    <div style={{
      flex: 1, minWidth: 0,
      background: primary ? AT.navy : AT.paper,
      color: primary ? '#fff' : AT.ink,
      border: primary ? 'none' : `1px solid ${AT.line}`,
      borderRadius: 12,
      padding: '20px 22px 18px',
      position: 'relative',
    }}>
      <div style={{
        fontSize: 11, fontWeight: 700, letterSpacing: '0.12em',
        textTransform: 'uppercase',
        color: primary ? '#fff' : AT.mute,
        opacity: primary ? 0.6 : 1,
        marginBottom: 10,
      }}>{label}</div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 4, marginBottom: pillPct !== undefined ? 12 : 0 }}>
        {prefix && <span style={{ fontSize: 16, opacity: primary ? 0.7 : 0.5 }}>{prefix}</span>}
        <span style={{
          fontSize: primary ? 36 : 28, fontWeight: 800,
          letterSpacing: '-0.02em', lineHeight: 1,
          color: primary ? '#fff' : AT.ink,
        }}>{value}</span>
        {suffix && <span style={{ fontSize: 14, opacity: 0.7, marginLeft: 2 }}>{suffix}</span>}
      </div>
      {pillPct !== undefined && (
        <SP pct={pillPct} label={pillLabel || ''} />
      )}
    </div>
  );
}

// Channel summary mini card (right rail)
function ChannelMini({ id, name, ventas, objPct, pyPct, share, completado }) {
  const status = spStatus(objPct);
  const barColor = status === 'pos' ? AT.pos : status === 'warn' ? AT.warn : AT.neg;
  return (
    <div style={{
      background: AT.paper, border: `1px solid ${AT.line}`,
      borderRadius: 10, padding: '14px 16px',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{
            width: 24, height: 24, borderRadius: 6,
            background: AT.pinkSoft, color: AT.pink,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <ChIcon id={id} size={13} />
          </div>
          <div style={{ fontSize: 13, fontWeight: 700, color: AT.ink }}>{name}</div>
        </div>
        <div style={{ fontSize: 10, color: AT.mute, fontFamily: dMono }}>{share}%</div>
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 4, marginBottom: 10 }}>
        <span style={{ fontSize: 11, color: AT.mute }}>€</span>
        <span style={{ fontSize: 20, fontWeight: 700, color: AT.ink, letterSpacing: '-0.01em' }}>{ventas}</span>
      </div>
      <div style={{ height: 3, background: AT.line, borderRadius: 2, overflow: 'hidden', marginBottom: 10 }}>
        <div style={{ width: Math.min(100, completado) + '%', height: '100%', background: barColor }}></div>
      </div>
      <div style={{ display: 'flex', gap: 6 }}>
        <SP pct={objPct} label="vs OBJ" />
        <SP pct={pyPct} label="vs PY" />
      </div>
    </div>
  );
}

// Main desktop screen — Vista TOTAL
function DesktopTotalA() {
  const rows = [
    { id: 'corner',  nombre: 'Corner ECI',   ventas: '85.991', obj: '93.520', objPct: -8,  py: '90.860', pyPct: -5,  devol: '11,9%', nVentas: '941',   evolN: -9,   unidades: '1.054', evolU: -17, tm: '91,38',  evolTm: -3,  upt: '1,1', evolUpt: -9, share: 49, completado: 92 },
    { id: 'mkt',     nombre: 'Marketplaces', ventas: '13.053', obj: '24.237', objPct: -46, py: '16.685', pyPct: -22, devol: '61,8%', nVentas: '460',   evolN: 5,    unidades: '360',   evolU: -2,  tm: '114,15', evolTm: 7,   upt: '0,8', evolUpt: -7, share: 7,  completado: 54 },
    { id: 'online',  nombre: 'Online',       ventas: '17.783', obj: '25.000', objPct: -29, py: '21.168', pyPct: -16, devol: '11,4%', nVentas: '159',   evolN: -62,  unidades: '227',   evolU: -61, tm: '138,37', evolTm: 17,  upt: '1,4', evolUpt: 5,  share: 10, completado: 71 },
    { id: 'tienda',  nombre: 'Tienda',       ventas: '57.914', obj: '59.800', objPct: -3,  py: '52.575', pyPct: 10,  devol: '5,5%',  nVentas: '427',   evolN: -7,   unidades: '637',   evolU: -6,  tm: '135,63', evolTm: 16,  upt: '1,5', evolUpt: 0,  share: 34, completado: 97 },
  ];
  const totals = { ventas: '174.741', obj: '202.557', objPct: -14, py: '181.287', pyPct: -4, devol: '18,0%', nVentas: '1.987', evolN: -15, unidades: '2.278', evolU: -21, tm: '109,33', evolTm: 4, upt: '1,1', evolUpt: -7 };

  const cellNum = (v, fontSize = 13) => (
    <td style={{ padding: '14px 12px', textAlign: 'right', fontSize, fontWeight: 600, color: AT.ink, fontFamily: dMono, whiteSpace: 'nowrap' }}>{v}</td>
  );
  const cellPct = (pct) => {
    const status = spStatus(pct);
    const c = status === 'pos' ? AT.pos : status === 'warn' ? AT.warn : AT.neg;
    const bg = status === 'pos' ? AT.posBg : status === 'warn' ? AT.warnBg : AT.negBg;
    const sign = pct >= 0 ? '+' : '';
    return (
      <td style={{ padding: '14px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
        <span style={{
          display: 'inline-flex', alignItems: 'center', gap: 4,
          padding: '3px 7px', borderRadius: 6,
          background: bg, color: c,
          fontSize: 11, fontWeight: 700, fontFamily: dMono,
        }}>
          <span style={{ width: 5, height: 5, borderRadius: 3, background: c }}></span>
          {sign}{pct}%
        </span>
      </td>
    );
  };
  const cellText = (v, weight = 700) => (
    <td style={{ padding: '14px 12px', fontSize: 13, fontWeight: weight, color: AT.ink, whiteSpace: 'nowrap' }}>{v}</td>
  );
  const cellHeader = (v) => (
    <th style={{
      padding: '14px 12px', textAlign: 'right',
      fontSize: 10, fontWeight: 700, color: AT.mute,
      letterSpacing: '0.08em', textTransform: 'uppercase',
      borderBottom: `1px solid ${AT.line}`,
      whiteSpace: 'nowrap', background: AT.cream,
    }}>{v}</th>
  );

  return (
    <div style={{
      width: 1440, minHeight: 980, background: '#F4F1EA',
      fontFamily: dFont, color: AT.ink, padding: 0,
    }}>
      {/* ─── Top header ─── */}
      <div style={{
        background: AT.paper, borderBottom: `1px solid ${AT.line}`,
        padding: '14px 36px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <div style={{
              width: 36, height: 36, borderRadius: 8,
              background: AT.pink, color: '#fff',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontWeight: 800, fontSize: 16, letterSpacing: '-0.02em',
            }}>LC</div>
            <div>
              <div style={{ fontSize: 14, fontWeight: 700, color: AT.ink, letterSpacing: '0.02em' }}>Lola Casademunt</div>
              <div style={{ fontSize: 11, color: AT.mute, marginTop: 1 }}>Dashboard de ventas · Vista global</div>
            </div>
          </div>
          <div style={{ width: 1, height: 28, background: AT.line, marginLeft: 8 }}></div>
          {/* Breadcrumb */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12 }}>
            <span style={{ color: AT.mute }}>Ventas</span>
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none" style={{ color: AT.mute }}><path d="M3.5 2 L6.5 5 L3.5 8" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
            <span style={{ color: AT.ink, fontWeight: 700 }}>Total</span>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <button style={{
            display: 'inline-flex', alignItems: 'center', gap: 8,
            padding: '8px 14px', borderRadius: 8,
            background: AT.paper, border: `1px solid ${AT.line}`,
            fontSize: 12, color: AT.ink, fontWeight: 600,
            fontFamily: dFont, cursor: 'pointer',
          }}>
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="1" y="2.5" width="10" height="8.5" rx="1" stroke="currentColor" strokeWidth="1.2"/><path d="M1 5.5 H11 M4 1 V4 M8 1 V4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/></svg>
            <span style={{ fontFamily: dMono }}>01/12/2025 – 12/12/2025</span>
          </button>
          <button style={{
            display: 'inline-flex', alignItems: 'center', gap: 6,
            padding: '8px 12px', borderRadius: 8,
            background: AT.paper, border: `1px solid ${AT.line}`,
            fontSize: 12, color: AT.body, fontWeight: 600, cursor: 'pointer',
          }}>
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M2 4 L6 8 L10 4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
            Exportar
          </button>
          <div style={{
            width: 32, height: 32, borderRadius: 16,
            background: AT.cream, border: `1px solid ${AT.line}`,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 11, fontWeight: 700, color: AT.body,
          }}>MR</div>
        </div>
      </div>

      {/* ─── Filter bar ─── */}
      <div style={{
        padding: '14px 36px',
        display: 'flex', alignItems: 'center', gap: 10,
        borderBottom: `1px solid ${AT.line}`, background: AT.paper,
      }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: AT.mute, letterSpacing: '0.08em', textTransform: 'uppercase', marginRight: 6 }}>Desempeño por</div>
        <FilterChip label="Tipo tienda" value="Todos" active />
        <FilterChip label="Tienda" value="Todas" />
        <FilterChip label="Galga" value="Todas" />
        <FilterChip label="Galga" value="Todas" />
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
          <button title="Sort ascending" style={{ width: 32, height: 32, borderRadius: 8, background: AT.paper, border: `1px solid ${AT.line}`, color: AT.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M7 2 V12 M4 5 L7 2 L10 5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </button>
          <button style={{ width: 32, height: 32, borderRadius: 8, background: AT.paper, border: `1px solid ${AT.line}`, color: AT.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M7 2 V12 M4 9 L7 12 L10 9" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </button>
          <button style={{ width: 32, height: 32, borderRadius: 8, background: AT.paper, border: `1px solid ${AT.line}`, color: AT.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M2 3 H12 M4 7 H10 M6 11 H8" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round"/></svg>
          </button>
          <button style={{ width: 32, height: 32, borderRadius: 8, background: AT.paper, border: `1px solid ${AT.line}`, color: AT.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M2 2 H12 L8 7 V11 L6 12 V7 Z" stroke="currentColor" strokeWidth="1.4" strokeLinejoin="round"/></svg>
          </button>
        </div>
      </div>

      {/* ─── Body ─── */}
      <div style={{ padding: '24px 36px', display: 'flex', flexDirection: 'column', gap: 24 }}>
        {/* Hero KPI row */}
        <div style={{ display: 'flex', gap: 12 }}>
          <HeroKPI label="Venta Neta · Total" prefix="€" value="174.741" primary pillPct={-14} pillLabel="vs OBJ" />
          <HeroKPI label="Objetivo" prefix="€" value="202.557" pillPct={-4} pillLabel="vs PY" />
          <HeroKPI label="Ticket Medio" prefix="€" value="109,33" />
          <HeroKPI label="UPT" value="1,1" />
          <HeroKPI label="% Conversión" value="13,85" suffix="%" />
          <HeroKPI label="% Atracción" value="2,23" suffix="%" />
        </div>

        {/* Main grid */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 24, alignItems: 'flex-start' }}>
          {/* Table */}
          <div style={{
            background: AT.paper, border: `1px solid ${AT.line}`,
            borderRadius: 12, overflow: 'hidden',
          }}>
            <div style={{
              padding: '16px 20px',
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              borderBottom: `1px solid ${AT.line}`,
            }}>
              <div>
                <div style={{ fontSize: 14, fontWeight: 700, color: AT.ink, letterSpacing: '-0.01em' }}>Desempeño por canal</div>
                <div style={{ fontSize: 11, color: AT.mute, marginTop: 2 }}>4 canales · ordenado por ventas</div>
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button style={{
                  fontSize: 11, fontWeight: 600, color: AT.body,
                  padding: '6px 10px', borderRadius: 6,
                  background: AT.cream, border: `1px solid ${AT.line}`, cursor: 'pointer',
                }}>Ver detalle</button>
              </div>
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ borderCollapse: 'collapse', width: '100%', fontFamily: dFont }}>
                <thead>
                  <tr>
                    <th style={{ padding: '14px 16px', textAlign: 'left', fontSize: 10, fontWeight: 700, color: AT.mute, letterSpacing: '0.08em', textTransform: 'uppercase', borderBottom: `1px solid ${AT.line}`, background: AT.cream }}>Canal</th>
                    {cellHeader('Ventas')}
                    {cellHeader('Objetivo')}
                    {cellHeader('Evol. %')}
                    {cellHeader('Per. Ant.')}
                    {cellHeader('vs PY')}
                    {cellHeader('Tasa Dev.')}
                    {cellHeader('Nº Ventas')}
                    {cellHeader('Evol.')}
                    {cellHeader('Ticket M.')}
                    {cellHeader('UPT')}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => (
                    <tr key={r.id} style={{ borderBottom: i < rows.length - 1 ? `1px solid ${AT.line}` : 'none' }}>
                      <td style={{ padding: '14px 16px' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                          <div style={{ width: 24, height: 24, borderRadius: 6, background: AT.pinkSoft, color: AT.pink, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                            <ChIcon id={r.id} size={13} />
                          </div>
                          <div>
                            <div style={{ fontSize: 13, fontWeight: 700, color: AT.ink, letterSpacing: '0.02em' }}>{r.nombre}</div>
                            <div style={{ fontSize: 10, color: AT.mute, marginTop: 1, fontFamily: dMono }}>{r.share}% del total</div>
                          </div>
                        </div>
                      </td>
                      {cellNum('€ ' + r.ventas, 13)}
                      {cellNum('€ ' + r.obj, 12)}
                      {cellPct(r.objPct)}
                      {cellNum('€ ' + r.py, 12)}
                      {cellPct(r.pyPct)}
                      {cellNum(r.devol, 12)}
                      {cellNum(r.nVentas, 13)}
                      {cellPct(r.evolN)}
                      {cellNum('€ ' + r.tm, 12)}
                      {cellNum(r.upt, 12)}
                    </tr>
                  ))}
                  {/* Total row */}
                  <tr style={{ background: AT.cream, borderTop: `2px solid ${AT.ink}` }}>
                    <td style={{ padding: '14px 16px', fontSize: 13, fontWeight: 800, color: AT.ink, letterSpacing: '0.06em', textTransform: 'uppercase' }}>Total</td>
                    {cellNum('€ ' + totals.ventas, 14)}
                    {cellNum('€ ' + totals.obj, 12)}
                    {cellPct(totals.objPct)}
                    {cellNum('€ ' + totals.py, 12)}
                    {cellPct(totals.pyPct)}
                    {cellNum(totals.devol, 12)}
                    {cellNum(totals.nVentas, 13)}
                    {cellPct(totals.evolN)}
                    {cellNum('€ ' + totals.tm, 12)}
                    {cellNum(totals.upt, 12)}
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {/* Right rail */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: AT.ink, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Resumen por canal</div>
            {rows.slice().sort((a, b) => b.share - a.share).map(r => (
              <ChannelMini key={r.id} id={r.id} name={r.nombre} ventas={r.ventas} objPct={r.objPct} pyPct={r.pyPct} share={r.share} completado={r.completado} />
            ))}
          </div>
        </div>

        {/* Bottom: evolution chart */}
        <div style={{
          background: AT.paper, border: `1px solid ${AT.line}`,
          borderRadius: 12, padding: '20px 24px',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 18 }}>
            <div>
              <div style={{ fontSize: 14, fontWeight: 700, color: AT.ink }}>Evolución diaria · 12 días</div>
              <div style={{ fontSize: 11, color: AT.mute, marginTop: 2 }}>Ventas netas con IVA, todos los canales</div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 16, fontSize: 11 }}>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: AT.body }}>
                <span style={{ width: 10, height: 10, borderRadius: 2, background: AT.navy }}></span> Periodo actual
              </span>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: AT.body }}>
                <span style={{ width: 10, height: 10, borderRadius: 2, background: AT.pink }}></span> Hoy
              </span>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: AT.body }}>
                <span style={{ width: 10, height: 2, background: AT.mute, borderRadius: 1 }}></span> PY (line)
              </span>
            </div>
          </div>
          <div style={{ height: 160, display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 10, position: 'relative' }}>
            {/* PY reference line (faked) */}
            <svg style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }} viewBox="0 0 1200 160" preserveAspectRatio="none">
              <path d="M40 80 L140 100 L240 70 L340 95 L440 65 L540 90 L640 55 L740 80 L840 60 L940 75 L1040 50 L1140 65" stroke={AT.mute} strokeWidth="1.5" strokeDasharray="4 4" fill="none" />
            </svg>
            {[55, 75, 50, 90, 110, 80, 120, 100, 115, 85, 130, 105].map((h, i) => (
              <div key={i} style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6 }}>
                <div style={{
                  width: '100%', maxWidth: 56,
                  height: h + 'px',
                  background: i === 10 ? AT.pink : AT.navy,
                  borderRadius: 4,
                }}></div>
                <div style={{ fontSize: 10, color: AT.mute, fontFamily: dMono }}>{String(i + 1).padStart(2, '0')}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// Channel detail desktop (Tienda)
function DesktopChannelTiendaA() {
  const tiendas = window.TIENDAS || [];
  const cellNum = (v) => <td style={{ padding: '14px 12px', textAlign: 'right', fontSize: 13, fontWeight: 600, color: AT.ink, fontFamily: dMono, whiteSpace: 'nowrap' }}>{v}</td>;
  const cellPct = (pct) => {
    const status = spStatus(pct);
    const c = status === 'pos' ? AT.pos : status === 'warn' ? AT.warn : AT.neg;
    const bg = status === 'pos' ? AT.posBg : status === 'warn' ? AT.warnBg : AT.negBg;
    const sign = pct >= 0 ? '+' : '';
    return (
      <td style={{ padding: '14px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '3px 7px', borderRadius: 6, background: bg, color: c, fontSize: 11, fontWeight: 700, fontFamily: dMono }}>
          <span style={{ width: 5, height: 5, borderRadius: 3, background: c }}></span>
          {sign}{pct}%
        </span>
      </td>
    );
  };
  const cellHeader = (v) => <th style={{ padding: '14px 12px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: AT.mute, letterSpacing: '0.08em', textTransform: 'uppercase', borderBottom: `1px solid ${AT.line}`, whiteSpace: 'nowrap', background: AT.cream }}>{v}</th>;

  return (
    <div style={{ width: 1440, minHeight: 980, background: '#F4F1EA', fontFamily: dFont, color: AT.ink }}>
      {/* Header */}
      <div style={{
        background: AT.paper, borderBottom: `1px solid ${AT.line}`,
        padding: '14px 36px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <button style={{ width: 32, height: 32, borderRadius: 8, background: AT.cream, border: `1px solid ${AT.line}`, color: AT.body, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M9 2 L4 7 L9 12" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/></svg>
            </button>
            <div style={{
              width: 36, height: 36, borderRadius: 8,
              background: AT.pink, color: '#fff',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontWeight: 800, fontSize: 16, letterSpacing: '-0.02em',
            }}>LC</div>
            <div>
              <div style={{ fontSize: 14, fontWeight: 700, color: AT.ink }}>Canal · Tienda</div>
              <div style={{ fontSize: 11, color: AT.mute, marginTop: 1 }}>{tiendas.length} tiendas físicas</div>
            </div>
          </div>
          <div style={{ width: 1, height: 28, background: AT.line, marginLeft: 8 }}></div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12 }}>
            <span style={{ color: AT.mute }}>Ventas</span>
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none" style={{ color: AT.mute }}><path d="M3.5 2 L6.5 5 L3.5 8" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
            <span style={{ color: AT.mute }}>Total</span>
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none" style={{ color: AT.mute }}><path d="M3.5 2 L6.5 5 L3.5 8" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/></svg>
            <span style={{ color: AT.ink, fontWeight: 700 }}>Tienda</span>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <button style={{ display: 'inline-flex', alignItems: 'center', gap: 8, padding: '8px 14px', borderRadius: 8, background: AT.paper, border: `1px solid ${AT.line}`, fontSize: 12, color: AT.ink, fontWeight: 600, fontFamily: dFont, cursor: 'pointer' }}>
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="1" y="2.5" width="10" height="8.5" rx="1" stroke="currentColor" strokeWidth="1.2"/><path d="M1 5.5 H11 M4 1 V4 M8 1 V4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/></svg>
            <span style={{ fontFamily: dMono }}>01/12/2025 – 12/12/2025</span>
          </button>
        </div>
      </div>

      {/* Filters */}
      <div style={{ padding: '14px 36px', display: 'flex', alignItems: 'center', gap: 10, borderBottom: `1px solid ${AT.line}`, background: AT.paper }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: AT.mute, letterSpacing: '0.08em', textTransform: 'uppercase', marginRight: 6 }}>Filtrar tiendas por</div>
        <FilterChip label="Zona" value="Todas" />
        <FilterChip label="Tipo tienda" value="Todos" />
        <FilterChip label="Galga" value="Todas" />
        <FilterChip label="Estado vs OBJ" value="Todos" />
      </div>

      {/* Body */}
      <div style={{ padding: '24px 36px', display: 'flex', flexDirection: 'column', gap: 24 }}>
        {/* Hero row */}
        <div style={{ display: 'flex', gap: 12 }}>
          <HeroKPI label="Venta · Tienda" prefix="€" value="57.914" primary pillPct={-3} pillLabel="vs OBJ" />
          <HeroKPI label="Objetivo" prefix="€" value="59.800" pillPct={10} pillLabel="vs PY" />
          <HeroKPI label="Ticket Medio" prefix="€" value="135,63" />
          <HeroKPI label="UPT" value="1,5" />
          <HeroKPI label="% Conversión" value="15,38" suffix="%" />
          <HeroKPI label="% Atracción" value="2,23" suffix="%" />
        </div>

        {/* Table */}
        <div style={{ background: AT.paper, border: `1px solid ${AT.line}`, borderRadius: 12, overflow: 'hidden' }}>
          <div style={{
            padding: '16px 20px',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            borderBottom: `1px solid ${AT.line}`,
          }}>
            <div>
              <div style={{ fontSize: 14, fontWeight: 700, color: AT.ink }}>Tiendas · {tiendas.length}</div>
              <div style={{ fontSize: 11, color: AT.mute, marginTop: 2 }}>Ordenado por ventas descendentes</div>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button style={{ fontSize: 11, fontWeight: 600, color: AT.body, padding: '6px 10px', borderRadius: 6, background: AT.cream, border: `1px solid ${AT.line}`, cursor: 'pointer' }}>Exportar CSV</button>
            </div>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', width: '100%', fontFamily: dFont }}>
              <thead>
                <tr>
                  <th style={{ padding: '14px 16px', textAlign: 'left', fontSize: 10, fontWeight: 700, color: AT.mute, letterSpacing: '0.08em', textTransform: 'uppercase', borderBottom: `1px solid ${AT.line}`, background: AT.cream, width: 280 }}>Tienda</th>
                  {cellHeader('Ventas')}
                  {cellHeader('Objetivo')}
                  {cellHeader('% Compl.')}
                  {cellHeader('vs OBJ')}
                  {cellHeader('Per. Ant.')}
                  {cellHeader('vs PY')}
                  {cellHeader('Progreso')}
                </tr>
              </thead>
              <tbody>
                {tiendas.map((t, i) => {
                  const status = spStatus(t.objPct);
                  const barColor = status === 'pos' ? AT.pos : status === 'warn' ? AT.warn : AT.neg;
                  return (
                    <tr key={t.id} style={{ borderBottom: i < tiendas.length - 1 ? `1px solid ${AT.line}` : 'none' }}>
                      <td style={{ padding: '14px 16px' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                          <div style={{ width: 24, height: 24, borderRadius: 6, background: AT.pinkSoft, color: AT.pink, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                            <ChIcon id="tienda" size={13} />
                          </div>
                          <div>
                            <div style={{ fontSize: 13, fontWeight: 700, color: AT.ink }}>{t.nombre}</div>
                            <div style={{ fontSize: 10, color: AT.mute, marginTop: 1, fontFamily: dMono }}>id · {t.id}</div>
                          </div>
                        </div>
                      </td>
                      {cellNum('€ ' + t.ventas)}
                      {cellNum('€ ' + t.obj)}
                      <td style={{ padding: '14px 12px', textAlign: 'right', fontSize: 13, fontWeight: 700, color: AT.ink, fontFamily: dMono }}>{t.completado}%</td>
                      {cellPct(t.objPct)}
                      {cellNum('€ ' + t.py)}
                      {cellPct(t.pyPct)}
                      <td style={{ padding: '14px 12px', width: 160 }}>
                        <div style={{ height: 4, background: AT.line, borderRadius: 2, overflow: 'hidden' }}>
                          <div style={{ width: Math.min(100, t.completado) + '%', height: '100%', background: barColor }}></div>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { DesktopTotalA, DesktopChannelTiendaA });
