# Handoff: Paneles · Dashboard Lola Casademunt

> **¿Vas a usar Claude Code?**
> - Para móvil → abre **`CLAUDE_CODE_PROMPT_MOVIL.md`**.
> - Para escritorio (Desempeño de Ventas) → abre **`powerbi/desempeno_ventas/CLAUDE_CODE_PROMPT_DESKTOP.md`**.
> - Si el resultado no coincide visualmente → **`CLAUDE_CODE_PROMPT_ITERAR.md`**.

---

## 0 · Mapa de archivos

```
design_handoff_paneles_movil/
├── README.md                          ← este archivo (tokens + estructura)
├── CLAUDE_CODE_PROMPT_MOVIL.md        ← prompt SOLO móvil
├── CLAUDE_CODE_PROMPT_ITERAR.md       ← prompt para iterar visualmente
├── powerbi/
│   ├── theme.json                     ← tema global (móvil + escritorio)
│   ├── measures.dax                   ← medidas móvil (semáforo OBJ/PY)
│   ├── MOBILE_LAYOUT_GUIDE.md         ← guía móvil paso a paso
│   └── desempeno_ventas/              ← TODO sobre la página de escritorio
│       ├── README.md                  ← intro + estructura
│       ├── CLAUDE_CODE_PROMPT_DESKTOP.md ← prompt enfocado en esta página
│       ├── DESKTOP_LAYOUT_GUIDE.md    ← guía manual paso a paso
│       └── measures-desempeno.dax     ← medidas de time-buckets + semáforos
├── screenshots/
│   ├── all-3-levels.png               ← referencia móvil (3 niveles)
│   └── desempeno-ventas-desktop.png   ← referencia escritorio
├── screens.jsx                        ← referencia React móvil nivel ①
├── screens-detail.jsx                 ← referencia React móvil niveles ② y ③
├── screens-desktop-unified.jsx        ← referencia React escritorio (BI2GO style)
├── screens-desktop.jsx                ← referencia React escritorio (variante navegada)
├── ios-frame.jsx                      ← marco iPhone (sólo presentación)
├── design-canvas.jsx                  ← canvas de comparación
├── Paneles Movil PowerBI.html         ← preview de las dos variaciones móvil
├── Flujo Navegacion Conservadora.html ← preview del flujo móvil completo
├── Variacion A Conservadora.html      ← preview móvil pantalla principal
└── Escritorio Vista Unificada.html    ← preview escritorio BI2GO style
```

---

## 1 · Resumen

Rediseño de la **vista TOTAL** del panel de Power BI en formato móvil para Lola Casademunt. El panel muestra el rendimiento de ventas global y desglosado por canal (Tienda, Corner ECI, Online, Marketplaces) frente a objetivo y año anterior.

Se entregan **2 variaciones** completas, ambas listas para implementación:

| | A · Conservadora | B · Moderna |
|---|---|---|
| Enfoque | Refina el lenguaje visual actual (fucsia + navy). | Editorial fashion brand. Más aire, tipografía mixta. |
| Paleta base | Blanco + cream cálido + navy + fucsia | Cream + papel + navy + fucsia más sobria |
| Tipografía | Sans (Manrope) en toda la pantalla | Sans (Manrope) + serif itálica (Instrument Serif) en momentos de marca |
| Densidad | Alta-media. Tarjetas compactas con borde fino. | Media. Tarjetas más generosas, más respiración. |
| Recomendado para | Migración rápida desde el diseño actual sin perder usuarios. | Refresco de marca, alineado con un look más editorial. |

---

## 2 · Sobre estos archivos

Los archivos `.jsx` y `.html` son **referencias de diseño en React**, no código de producción. La misión es:

1. Estudiar los `.jsx` para extraer **layout exacto, jerarquía, espaciados, colores, tipografía e iconografía**.
2. **Reimplementarlos en el codebase destino** (Power BI, React/React Native, Vue, SwiftUI, lo que sea) siguiendo los patrones y librerías ya establecidas en ese proyecto.
3. Si el destino es Power BI: usar Page View "Mobile layout" con bloques de visualizaciones equivalentes; los `.jsx` sirven como **guía visual** para reproducir el look pixel a pixel.

**Fidelidad**: **Alta fidelidad (hifi)**. Colores, tipografía, espaciados y tamaños son los definitivos.

---

## 3 · Niveles de navegación

El panel tiene **3 niveles** de profundidad. Cada uno se entrega como una pantalla independiente, todas con el mismo lenguaje visual:

```
① VISTA TOTAL                  → resumen agregado de todos los canales
   ↓ (tap en tarjeta de canal)
② DETALLE DE CANAL             → KPIs del canal + lista de elementos
   ↓ (tap en elemento de la lista, sólo Tienda en este momento)
③ DETALLE DE ELEMENTO          → KPIs de la tienda concreta + gráficas
```

Cada nivel se construye con los **mismos bloques reutilizables** (header, period, hero, KPI grid, status pill, item row). Solo cambia el contenido.

### Archivos de referencia

| Nivel | Componente React | Archivo |
|---|---|---|
| ① Total | `<VariationA />` | `screens.jsx` |
| ② Canal | `<ChannelDetailA channelId="tienda" />` | `screens-detail.jsx` |
| ③ Elemento | `<StoreDetailA storeId="illa" />` | `screens-detail.jsx` |

`channelId` admite: `'tienda'` · `'corner'` · `'online'` · `'mkt'`. Sólo Tienda muestra lista de elementos; los demás canales muestran un placeholder "Sin desglose por X" (cambiar cuando haya datos).

---

## 4 · Estructura de la pantalla (vista TOTAL)

Una sola pantalla scrollable verticalmente. De arriba abajo:

```
1 · Header                  → marca + (back / acciones)
2 · Period chip             → fechas del periodo
3 · Hero KPI                → Venta neta total (cifra más grande)
4 · Estado vs OBJ / PY      → 2 píldoras de estado + barra de progreso vs objetivo
5 · Grid KPIs secundarios   → Ticket Medio, UPT, Conversión, Atracción (2×2)
6 · Sección "Por canal"     → 4 tarjetas idénticas, una por canal
7 · Footer info             → fecha de actualización
```

**Datos de ejemplo** (extraídos del PowerBI actual):

```js
TOTAL = {
  ventas: '174.741',        // €
  objetivo: '202.557',      // €
  objPct: -14,              // % vs objetivo
  pyPct: -4,                // % vs año anterior
  py: '181.287',            // €
  ticketMedio: '109,33',    // €
  upt: '1,1',
  conversion: '13,85%',
  atraccion: '2,23%',
  fechaIni: '01/12/2025',
  fechaFin: '12/12/2025',
}

CANALES = [
  { id: 'tienda',     nombre: 'Tienda',       ventas: '57.914', obj: '59.800', objPct: -3,  pyPct: 10,  py: '52.575', share: 33 },
  { id: 'corner',     nombre: 'Corner ECI',   ventas: '85.991', obj: '93.520', objPct: -8,  pyPct: -5,  py: '90.860', share: 49 },
  { id: 'online',     nombre: 'Online',       ventas: '17.783', obj: '25.000', objPct: -29, pyPct: -16, py: '21.168', share: 10 },
  { id: 'mkt',        nombre: 'Marketplaces', ventas: '13.053', obj: '24.237', objPct: -46, pyPct: -22, py: '16.685', share: 8  },
]
```

---

## 4 · Design tokens

### 4.1 Variación A · Conservadora

```css
/* Colores */
--a-pink:       #E11A6F;   /* primario marca */
--a-pink-soft:  #FBE6EF;   /* fondo de iconos */
--a-navy:       #10182F;   /* fondo hero */
--a-navy-2:     #1B2546;
--a-ink:        #0A0F1E;   /* texto principal */
--a-body:       #3A4055;   /* texto secundario */
--a-mute:       #8089A2;   /* texto terciario / labels */
--a-line:       #E4E7F0;   /* divisores y bordes de tarjeta */
--a-paper:      #FFFFFF;   /* fondo */
--a-cream:      #F7F6F2;   /* fondo del bloque de fecha */

/* Estados */
--pos:    #1F8A5B;  --pos-bg:  #E3F4EB;   /* % ≥ 0 */
--warn:   #B57A0E;  --warn-bg: #FAF1DD;   /* −10% ≤ % < 0 */
--neg:    #C73838;  --neg-bg:  #FBE6E6;   /* % < −10% */
```

### 4.2 Variación B · Moderna

```css
--b-pink:       #D8336B;
--b-pink-soft:  #F7E2EA;
--b-navy:       #1A1F36;
--b-ink:        #0B0F1F;
--b-body:       #3F4458;
--b-mute:       #8F8A82;
--b-line:       #E8E2D8;
--b-paper:      #FAF6EF;   /* fondo cremoso */
--b-paper-2:    #FFFFFF;   /* tarjetas y chips */

/* Estados (más apagados, tono editorial) */
--pos:  #2D7A4F;  --pos-bg:  #E1EFE6;
--warn: #A56B14;  --warn-bg: #F4E9D3;
--neg:  #B83232;  --neg-bg:  #F4DCDC;
```

### 4.3 Tipografía (ambas)

```css
/* Familias */
--font-sans:   'Manrope', -apple-system, system-ui, sans-serif;
--font-mono:   'JetBrains Mono', 'SF Mono', ui-monospace, monospace;
--font-serif:  'Instrument Serif', 'Cormorant Garamond', Georgia, serif;
/* Serif sólo se usa en Variación B (momentos de marca: wordmark, “Por canal”,
   “Ventas del periodo”, footer). */

/* Escala (px) */
hero-num         42 (A) / 56 (B)   weight 800/700   tracking -0.02/-0.035em
channel-num      26 (A) / 32 (B)   weight 700        tracking -0.02em
kpi-num          22 (A) / 24 (B)   weight 700        tracking -0.01/-0.02em
section-title    11 (A) / 22 (B)   weight 700 (A) / serif italic (B)
label / eyebrow  10–11             weight 600/700    tracking 0.06–0.18em UPPERCASE
body             12–13             weight 400/500
data-mono        10–12 mono                            usado en fechas y €
```

### 4.4 Espaciado y radios

```css
--radius-card-a: 10px;
--radius-card-b: 16px;
--radius-pill:   100px;      /* píldoras de estado, chip de fecha */

--gap-section:   24px;        /* entre bloques mayores */
--gap-cards:     10–12px;     /* entre tarjetas de canal */
--pad-screen:    20px (A) / 24px (B);   /* padding horizontal de la pantalla */
--pad-card:      14px (A) / 18px (B);
```

---

## 5 · Componentes (anatomía)

### 5.1 Header
- **A**: barra fucsia (`var(--a-pink)`) de 38–44px alto. Botón back circular semi-transparente a la izquierda, wordmark "LOLA CASADEMUNT" centrado en mayúsculas, 13px, tracking 0.18em. Botón "más" a la derecha (sólo Variación B).
- **B**: fondo cream, sin color de fondo. Botón back con borde fino, wordmark "Lola Casademunt" en serif itálica 19px, botón de menú (3 puntos) a la derecha.
- Altura total recomendada: **56–64px**.

### 5.2 Period chip
- **A**: bloque horizontal con etiqueta "PERIODO" (10px uppercase, mute) + dos fechas separadas por flecha → en mono. Fondo cream, separador inferior `var(--a-line)`.
- **B**: chip redondeado (`border-radius: 100px`) con icono de calendario + rango `01/12/2025 – 12/12/2025` en mono 11px. A la derecha texto auxiliar "12 días" en mute.

### 5.3 Hero KPI (Venta neta)
- **A**: bloque rectangular con fondo `--a-navy`, texto blanco. Eyebrow "VENTA NETA · TOTAL" (10px uppercase opacidad 0.55). Número 42px weight 800 con € pequeño antepuesto. Debajo barra de progreso fina (4px alto, fondo blanco 15%, fill fucsia) con etiqueta "vs Objetivo · 86% completado" + monto objetivo a la derecha en mono. Debajo, 2 píldoras de estado (vs OBJ, vs PY).
- **B**: sin fondo, sólo aire. Eyebrow en serif itálico 15px "Venta neta total". Número 56px weight 700, tracking muy negativo (-0.035em), line-height 0.95. Debajo, píldoras de estado, luego progress bar de 6px con borde y radius 100px, fill fucsia.

### 5.4 Grid KPIs secundarios (Ticket Medio, UPT, Conv%, Atrac%)
- **Layout**: `grid-template-columns: repeat(2, 1fr)` ⇒ 4 celdas (2×2).
- **A**: sin gaps, separadas por `border-right`/`border-bottom` 1px `--a-line`. Cada celda: eyebrow uppercase 10px + número 22px weight 700.
- **B**: `gap: 8px`. Cada celda es una tarjeta independiente (`--b-paper-2` con borde `--b-line`, radius 14px, padding 14px). Mismo contenido pero más tarjeta.

### 5.5 Status pill (componente clave, **reutilizable**)
Píldora horizontal usada para mostrar variaciones porcentuales contra objetivo y año anterior.

```jsx
<StatusPill pct={-14} label="vs OBJ" />
```

Anatomía:
- Punto de color (6×6 circular) + texto `+10% vs PY` o `-14% vs OBJ`.
- Color y fondo según la regla:
  - `pct ≥ 0`        → verde     (`--pos` sobre `--pos-bg`)
  - `−10% ≤ pct < 0` → amarillo  (`--warn` sobre `--warn-bg`)
  - `pct < −10%`     → rojo      (`--neg` sobre `--neg-bg`)
- Padding: `4px 8px`, radius 6px, font 11px weight 700, tracking 0.02em.

**Sustituye a los emojis 🔴🟡🟢 actuales** — más legible y profesional.

### 5.6 Tarjeta de canal
4 tarjetas idénticas (Tienda, Corner ECI, Online, Marketplaces), apiladas verticalmente con gap 10–12px.

**Estructura** (de arriba abajo):
1. **Header**: icono geométrico + nombre del canal a la izquierda; share del total (e.g. "49% del total") en mono 10px a la derecha.
2. **Número de ventas**: € pequeño + cifra grande (26px A / 32px B).
3. **Barra de progreso vs objetivo**: con label `XX% del objetivo` a la izquierda y `OBJ € XX.XXX` en mono a la derecha. Color de la barra = mismo semáforo que las píldoras.
4. **Píldoras de estado**: una fila con `vs OBJ` y `vs PY`.

**Variación B** añade:
- Pequeño badge numerado (01, 02, 03, 04) a la derecha del header.
- Cita en serif itálica "Ventas del periodo" bajo el número grande.
- Separador interno horizontal antes de las píldoras, que se muestran en grid 2 columnas con sus propios eyebrows ("vs Objetivo" / "vs Año Anterior").

### 5.7 Iconos de canal (geométricos, NO emojis)
Cuatro SVG simples en `screens.jsx`:
- `tienda` → fachada de tienda con puerta
- `corner` → dos rectángulos de distinto alto
- `online` → globo terráqueo
- `mkt` → casa/triángulo (marketplace)

Stroke 1.3px, 14–16px tamaño. **No mezclar con emojis** bajo ningún concepto.

### 5.8 Pantalla ② · Detalle de canal (`screens-detail.jsx`)

**Diferencias respecto a la vista TOTAL:**
- Header con breadcrumb: eyebrow "LOLA CASADEMUNT · CANAL" + título grande del canal (TIENDA, ONLINE, etc.) + badge a la derecha con conteo de items (e.g. "8 tiendas") o número de ventas ("427 ventas").
- Hero KPI igual pero etiquetado "Venta Neta · {Canal}".
- Grid de KPIs en **4 columnas** (no 2×2) para ahorrar vertical — TM, UPT, Conv, Atrac uno al lado del otro.
- **Sort/Filter strip** debajo de los KPIs: dos chips ("Ventas ▼", "Filtrar") flotantes a la derecha del título de la lista.
- **Lista de elementos** (sólo Tienda): tarjetas más compactas que las del nivel ①. Cada fila tiene: nombre + completado%/OBJ a la izquierda, cifra grande + chevron a la derecha, barra de progreso, par de píldoras de estado. Toda la tarjeta es tappable.
- Para canales sin desglose (Corner, Online, Marketplaces): bloque "Sin desglose por {corners/fuentes/marketplaces}" centrado, fondo cream, radius 10.

**Datos por canal**: ver objeto `CHANNEL_INFO` en `screens-detail.jsx`.

### 5.9 Pantalla ③ · Detalle de elemento (tienda concreta)

**Bloques de arriba abajo:**
1. **Header**: eyebrow "TIENDA · DETALLE" + nombre de la tienda (truncado con ellipsis si es largo).
2. **Period chip** (igual que en otros niveles).
3. **Hero KPI**: cifra de ventas de esa tienda, barra de progreso vs objetivo, píldoras de estado.
4. **Grid KPIs 2×2**: TM, UPT, Conversión, Atracción específicos de la tienda.
5. **Mini bar chart de evolución** (12 días): barras verticales navy + última barra fucsia para destacar "hoy". Eje X con días numerados 01–12. Altura ~160px.
6. **Bloque de comparativa**: dos filas (periodo actual / mismo periodo 2024) con sus cifras y la píldora de % vs PY.

### 5.10 Componente reutilizable: `<StoreRow>` (item de la lista de canal)

```jsx
<StoreRow
  nombre="LC Barcelona L'Illa Diagonal"
  ventas="2.095"     // €
  obj="2.220"
  completado={94}    // %
  objPct={-6}
  pyPct={20}
  onClick={() => navegar('illa')}
/>
```

Alto típico: 100–110px. Padding interno: `12px 14px`. Border 1px, radius 10. Sombra: ninguna en estado normal, `box-shadow: 0 2px 8px rgba(10,15,30,0.06)` al hover (escritorio).

---

## 6 · Lógica e interacción

- **Scroll**: la pantalla entera es scroll vertical nativo. No hay tabs ni filtros en esta primera versión.
- **Tap en tarjeta de canal** (futuro): navega a vista detalle de ese canal (no incluido en este handoff).
- **Tap en chip de fecha** (futuro): abre selector de rango.
- **Botón back** del header: vuelve al menú principal.
- **No hay animaciones complejas**, sólo transiciones nativas de scroll.

### Reglas de cálculo
- `completado = round((ventas / objetivo) * 100)` → usado para la barra de progreso.
- Color de la barra = misma regla de semáforo que `StatusPill`, pero basada en `objPct`.
- Si el valor falta o es 0, la barra se queda vacía y la píldora se omite.

---

## 7 · Implementación en Power BI (si es el destino)

Power BI Desktop ofrece "Mobile layout" (icono de móvil arriba). Equivalencias recomendadas:
- **Header**: imagen + texto.
- **Hero KPI** + barra: usar visualización "Card" + "KPI" o un "Bar chart" minimizado.
- **Grid KPIs**: 4 "Card" o "Multi-row card" en grid.
- **Tarjetas de canal**: una "Table" estilizada o 4 visuales independientes apilados.
- **StatusPill**: usar campo calculado con SWITCH para asignar color de fondo a una "Card" pequeña.

**Tipografía Power BI**: usa "Segoe UI" o sube fuentes personalizadas vía tema JSON (theme file) para igualar Manrope/Instrument Serif si la organización lo permite.

**Recomendación**: implementa primero la **Variación A** (más fiel al lenguaje actual y por tanto más fácil de aprobar con stakeholders) y deja la B como propuesta de evolución.

---

## 8 · Estados y casos límite

| Caso | Comportamiento |
|---|---|
| Sin datos en un canal | Mostrar "—" en lugar de la cifra, ocultar barra y píldoras. |
| Conversión / Atracción sin valor | Mostrar "—" en la celda del grid. |
| Objetivo = 0 | Ocultar barra y texto de progreso del bloque hero / tarjeta. |
| `pyPct` o `objPct` no disponible | Omitir la píldora correspondiente. |
| Cifra muy grande (>1.000.000) | Reducir a 36px (A) / 48px (B) o usar abreviación "1,2 M €". |
| Modo claro / oscuro | Sólo se entrega modo claro. Si se necesita oscuro, mantener fucsia y verde/amarillo/rojo como están e invertir paper / ink. |

---

## 9 · Cómo previsualizar los `.jsx`

1. Abre `Paneles Movil PowerBI.html` en cualquier navegador → muestra ambas variaciones en un canvas pan-zoom.
2. O abre `_preview.html` para ver las dos pantallas una al lado de la otra a tamaño real.

---

## 10 · Archivos en este handoff

| Archivo | Qué contiene |
|---|---|
| `README.md` | Este documento. |
| `screens.jsx` | Las dos variaciones A y B implementadas en React. **Es la referencia visual canónica.** |
| `ios-frame.jsx` | Marco de iPhone para previsualización (no se implementa, sólo es chrome de presentación). |
| `notes.jsx` | Tarjeta lateral con notas del sistema y principios. |
| `Paneles Movil PowerBI.html` | Página de preview con design canvas. |
| `_preview.html` | Preview alternativa, dos pantallas lado a lado. |

---

## 11 · Checklist para Claude Code

- [ ] Identifica el codebase destino (Power BI / React / React Native / Vue / SwiftUI / otro).
- [ ] Localiza el sistema de design tokens existente (theme, tokens, variables CSS, etc.). Mapea los tokens de este handoff a los del proyecto.
- [ ] Si la marca ya tiene tokens definidos en el codebase, **úsalos** y sólo añade los nuevos que falten (fucsia exacto, semáforo).
- [ ] Implementa primero el componente `StatusPill` — es reutilizable y aparece 12+ veces.
- [ ] Implementa la tarjeta de canal como componente único parametrizado.
- [ ] Hardcodea los datos del JSON de ejemplo en una primera versión, luego conecta al dataset real.
- [ ] Verifica con los pesos exactos de Manrope (400/500/600/700/800) cargados.
- [ ] Test de regresión visual contra `_preview.html`.
