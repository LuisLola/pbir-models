// Design system notes / quick reference artboards

function SystemNotes() {
  const Section = ({ title, children }) => (
    <div style={{ marginBottom: 28 }}>
      <div style={{
        fontSize: 10, fontWeight: 700, letterSpacing: '0.14em', textTransform: 'uppercase',
        color: '#8089A2', marginBottom: 10,
      }}>{title}</div>
      {children}
    </div>
  );

  return (
    <div style={{
      width: 520, minHeight: 1400, background: '#fff',
      padding: '32px 30px', boxSizing: 'border-box',
      fontFamily: "'Manrope', sans-serif", color: '#0A0F1E',
    }}>
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 11, color: '#8089A2', fontWeight: 600, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 4 }}>Mobile dashboard · PowerBI</div>
        <div style={{ fontSize: 28, fontWeight: 800, letterSpacing: '-0.02em' }}>Guía para móvil</div>
        <div style={{ fontSize: 13, color: '#3A4055', marginTop: 6, lineHeight: 1.5 }}>
          Principios para trasladar el panel de escritorio a una vista móvil con jerarquía clara.
        </div>
      </div>

      <Section title="Problemas que resuelve">
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {[
            ['Demasiados datos en pantalla', 'Solo 1 KPI hero + 4 secundarios visibles a la vez. El resto requiere scroll.'],
            ['Jerarquía poco clara', 'Tipografía con 3 niveles: hero 42–56px, sección 22–32px, dato 11–13px.'],
            ['Cabecera ocupa demasiado', 'Bajada a 38–56px (vs ~120px actual). Branding mínimo.'],
            ['Falta sensación de marca', 'Tipografía + paleta cuidadas. Detalles editoriales.'],
          ].map(([t, d], i) => (
            <div key={i} style={{ display: 'flex', gap: 10, padding: '10px 0', borderBottom: '1px solid #E4E7F0' }}>
              <div style={{ width: 18, height: 18, borderRadius: 9, background: '#E11A6F', color: '#fff', fontSize: 10, fontWeight: 700, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, marginTop: 1 }}>{i + 1}</div>
              <div>
                <div style={{ fontWeight: 700, fontSize: 13 }}>{t}</div>
                <div style={{ fontSize: 12, color: '#3A4055', marginTop: 2 }}>{d}</div>
              </div>
            </div>
          ))}
        </div>
      </Section>

      <Section title="Paleta de estados (ambas variaciones)">
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8 }}>
          {[
            ['Positivo', '#1F8A5B', '#E3F4EB', '+10% vs PY', 'pct ≥ 0'],
            ['Atención', '#B57A0E', '#FAF1DD', '-5% vs PY',  '−10% ≤ pct < 0'],
            ['Crítico',  '#C73838', '#FBE6E6', '-22% vs PY', 'pct < −10%'],
          ].map(([n, c, bg, ej, regla], i) => (
            <div key={i} style={{ background: bg, borderRadius: 10, padding: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
                <span style={{ width: 8, height: 8, borderRadius: 4, background: c }}></span>
                <span style={{ fontSize: 11, fontWeight: 700, color: c }}>{n}</span>
              </div>
              <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 10, color: c, fontWeight: 600, marginBottom: 4 }}>{ej}</div>
              <div style={{ fontSize: 10, color: '#3A4055', lineHeight: 1.3 }}>{regla}</div>
            </div>
          ))}
        </div>
        <div style={{ fontSize: 11, color: '#3A4055', marginTop: 10, lineHeight: 1.5, fontStyle: 'italic' }}>
          Reemplaza los emojis 🔴🟡🟢 actuales por píldoras con punto de color + texto. Más legible, más profesional.
        </div>
      </Section>

      <Section title="Anatomía de la pantalla">
        <div style={{ background: '#F7F6F2', borderRadius: 12, padding: 16, fontSize: 12, lineHeight: 1.7, color: '#3A4055' }}>
          <div><strong style={{ color: '#0A0F1E' }}>1 · Header</strong> · Marca + fecha. Slim, 56–80px.</div>
          <div><strong style={{ color: '#0A0F1E' }}>2 · Hero KPI</strong> · Venta neta total. El número más grande de la pantalla.</div>
          <div><strong style={{ color: '#0A0F1E' }}>3 · Estado vs OBJ / PY</strong> · 2 píldoras de color + barra de progreso.</div>
          <div><strong style={{ color: '#0A0F1E' }}>4 · Indicadores secundarios</strong> · TM, UPT, Conv%, Atrac% (grid 2×2).</div>
          <div><strong style={{ color: '#0A0F1E' }}>5 · Por canal</strong> · 4 tarjetas iguales con su mini-hero y progreso.</div>
        </div>
      </Section>

      <Section title="Reglas que aplican a PowerBI">
        <div style={{ fontSize: 12, color: '#3A4055', lineHeight: 1.6 }}>
          <ul style={{ paddingLeft: 16, margin: 0 }}>
            <li>Texto mínimo 11px (preferible 12–13px) — nada bajo eso.</li>
            <li>Número hero ≥ 32px. El resto de KPIs ≥ 22px.</li>
            <li>Máx. 7 KPIs visibles arriba del scroll. El resto, debajo.</li>
            <li>No mezcles iconos emoji con iconografía propia. Elige uno.</li>
            <li>Etiquetas de columnas en MAYÚSCULAS + tracking 0.06–0.16em.</li>
            <li>Espaciado vertical 24px entre secciones, 10–12px entre tarjetas.</li>
          </ul>
        </div>
      </Section>
    </div>
  );
}

window.SystemNotes = SystemNotes;
