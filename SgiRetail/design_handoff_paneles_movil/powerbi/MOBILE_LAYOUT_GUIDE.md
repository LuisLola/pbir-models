# Guía paso a paso · Power BI Mobile Layout

Este documento es la receta exacta para construir el panel móvil en Power BI Desktop. Cada bloque del diseño HTML se traduce a uno o varios visuales de Power BI.

> **Atajo mental**: Power BI Mobile NO reflowa como CSS. Es un canvas de tamaño fijo donde cada visual se coloca a mano. Para que se vea bien:
> 1. Construye todos los visuales en una **página desktop estrecha** (390×1500 px).
> 2. Ajusta cada visual al milímetro (font, padding, fondo, borde).
> 3. Activa **Mobile Layout** y arrastra los visuales a su sitio en el lienzo de 320×640.

---

## 0 · Preparación del archivo

### 0.1 Importar el tema
1. `View → Themes → Browse for themes…`
2. Selecciona `theme.json` (en esta misma carpeta).
3. Verifica que la paleta cambie a fucsia + navy + cream.

### 0.2 Cargar las medidas DAX
1. Abre `measures.dax` (en esta misma carpeta).
2. Por cada bloque separado por línea en blanco:
   - `Modeling → New measure`
   - Pega el código
   - Enter
3. Verifica que aparezcan en el panel de campos sin error.

### 0.3 Crear la página móvil
1. `Insert → New page` → renombra a `Móvil – Total`.
2. `Format → Canvas settings → Type: Custom`.
3. Pon **Width: 390 · Height: 1500**.
4. `Page background: #FFFFFF` (ya viene del tema).

---

## 1 · Página ① · Vista TOTAL

### 1.1 Header rosa (alto 64px)
| Elemento | Tipo de visual | Propiedades clave |
|---|---|---|
| Fondo rosa | **Shape › Rectangle** | Fill `#E11A6F`, sin borde, position `0,0`, size `390×64` |
| Botón back | **Shape › Round rectangle** | Fill `rgba(255,255,255,0.18)`, size `28×28`, x=12, y=18 |
| Icono ‹ | **Shape › Arrow** (line) o un Text Box "‹" | color blanco, fontSize 16 |
| Wordmark | **Text Box** | Texto `LOLA CASADEMUNT`, font Segoe UI Bold 13, letter-spacing 0.18em (no existe en PBI — usa MAYÚSCULAS y espacios entre letras), color blanco, alineación centrada |

> ⚠ Power BI no soporta `letter-spacing`. Para el tracking simula con espacios: `L O L A   C A S A D E M U N T`.

### 1.2 Tira de período (alto 40px, y=64)
| Elemento | Tipo de visual | Propiedades clave |
|---|---|---|
| Fondo cream | Shape rectangle | Fill `#F7F6F2`, position `0,64`, size `390×40` |
| Borde inferior | Shape rectangle | Fill `#E4E7F0`, position `0,103`, size `390×1` |
| Label "PERIODO" | Text Box | font Segoe UI Semibold 10, color `#8089A2`, MAYÚSCULAS |
| Fechas | **Slicer › Between (Date)** | Style: Tile o Dropdown según prefieras. Fuente mono no nativa — usa Consolas 11 |

Alternativa más limpia: usa **Slicer "Relative date"** o tu propia Card con texto.

### 1.3 Hero KPI Venta Neta (alto 200px, y=104)
**Capas (z-index bajo a alto):**

1. **Shape rectangle** – fill `#10182F`, position `0,104`, size `390×200`. Este es el fondo navy.
2. **Text Box** "VENTA NETA · TOTAL" – Segoe UI Semibold 10, color `#FFFFFF` con opacity 55% (o usa `#8089A2`), MAYÚSCULAS, x=20, y=128.
3. **Card** con la medida `[Venta Neta]`:
   - Format → Callout value → Font Size **42**, color `#FFFFFF`, Family Segoe UI Bold.
   - Display unit: None. Decimal places: 0.
   - Category label: Off.
   - Background: Off.
   - Title: Off.
   - Position: x=20, y=146, size auto.
   - Prefijo "€" se gestiona con un Text Box pequeño aparte (Power BI no soporta sufijos custom de moneda nativos en Card).
4. **Barra de progreso vs objetivo**:
   - Opción A (recomendada): dos shapes superpuestos:
     - Fondo: Shape rectangle, fill `rgba(255,255,255,0.15)`, size `350×4`, x=20, y=220.
     - Fill: Shape rectangle, fill `#E11A6F`, size dependiente. Como Power BI no calcula ancho dinámico de shapes, **usa un Bar chart 100% Stacked** con:
       - Dos columnas: `[Venta Neta]` y `[Objetivo] - [Venta Neta]`.
       - Sin ejes, sin labels, sin leyenda, sin title.
       - Color serie 1 = `#E11A6F`, serie 2 = `rgba(255,255,255,0.15)`.
       - Height: 4px (puede no ser exactamente 4 — ajusta al mínimo que PBI permita, ≈10px).
5. **Píldoras de estado vs OBJ / vs PY**:
   - Cada píldora = **Card** con la medida `[Texto vs Obj]` (o `[Texto vs PY]`).
   - Format:
     - Callout value: Font Size 11, Font Family Segoe UI Bold, color condicional usando `[Color vs Obj]` (Field value).
     - Background: Conditional formatting con `[Bg vs Obj]`.
     - Padding/border: en Format → Effects → Visual border → Rounded corners 6.
   - Posición: x=20, y=250, size ≈110×24. Segunda píldora x=140, y=250.

### 1.4 Grid de 4 KPIs secundarios (alto 80px, y=304)
| Visual | Medida | Posición |
|---|---|---|
| Card | `[Ticket Medio]` con format "€ #.##0,00" | x=20, y=304, size 165×80 |
| Card | `[UPT]` con format "0,0" | x=205, y=304, size 165×80 |
| Card | `[% Conversion]` con format "0,00%" | x=20, y=384, size 165×80 |
| Card | `[% Atraccion]` con format "0,00%" | x=205, y=384, size 165×80 |

Para cada Card:
- Callout value: Segoe UI Bold 22, color `#0A0F1E`.
- Category label: Segoe UI Semibold 10, color `#8089A2`, MAYÚSCULAS.
- Background: Off.
- Title: Off.
- Border lateral derecho/inferior: Shape line de 1px `#E4E7F0` (manual).

### 1.5 Sección "Por canal" (y=480 en adelante)

Para CADA uno de los 4 canales (filtrados con un **Visual filter** por `DimCanal[Canal]`):

**Tarjeta de canal** (alto 130px, padding interno 14):
1. **Shape rectangle** – fill blanco, border `#E4E7F0` 1px, radius 10, size `350×130`. Position x=20, y=(480 + i×140).
2. **Text Box** – nombre del canal en Segoe UI Bold 14.
3. **Card** – `[Venta Neta]` filtrado al canal, font 26, color `#0A0F1E`.
4. **Card pequeña** – `[% Completado Obj]` formato "0%", font 10, color mute.
5. **Bar chart 100% Stacked** (la barra de progreso) – mismo recipe que el hero, pero color depende de `[Color vs Obj]` del canal.
6. **2 Cards** – píldoras `[Texto vs Obj]` y `[Texto vs PY]`.

> **Truco para no repetir**: si tienes pocos canales, duplica el grupo de visuales 4 veces y aplica un filtro distinto a cada copia (`Tienda`, `Corner ECI`, `Online`, `Marketplaces`). Es la forma más fiable.

---

## 2 · Página ② · Detalle de canal (ej: Tienda)

Mismo recipe que la página ①, con dos diferencias:

### 2.1 Header con breadcrumb
- Eyebrow pequeño "LOLA CASADEMUNT · CANAL" + título grande del canal "TIENDA".
- Badge a la derecha con `[Num Ventas]` o conteo de tiendas (un Text Box "8 tiendas").

### 2.2 Grid de 4 KPIs en una fila (no 2×2)
Mismas Cards pero en 4 columnas horizontales, height 60px, separadas por líneas verticales 1px.

### 2.3 Lista de tiendas (Table visual)
1. Inserta una **Table** con columnas:
   - `DimTienda[Tienda]`
   - `[Venta Neta]`
   - `[% Completado Obj]`
   - `[Texto vs Obj]`
   - `[Texto vs PY]`
2. Format:
   - Row height: 80 (en `Format → Row → Row height`).
   - Column headers: Off (o muy discreta — color `#8089A2`).
   - Grid → outline color `#E4E7F0`, vertical grid Off.
   - Values font Segoe UI 11.
3. **Conditional formatting** en `[Texto vs Obj]` y `[Texto vs PY]`:
   - Background color → Field value → `[Bg vs Obj]`.
   - Font color → Field value → `[Color vs Obj]`.

> Power BI Table no permite mostrar barras de progreso al estilo móvil. Como aproximación usa la columna `[% Completado Obj]` con **Data bars** (Conditional formatting → Data bars), color fucsia.

---

## 3 · Página ③ · Detalle de tienda concreta

Usa **Drillthrough**:
1. En la página ②, click derecho en la Table → "Drillthrough → Detalle Tienda".
2. Crea una página `Detalle Tienda` con campo de Drillthrough = `DimTienda[Tienda]`.
3. En esa página construye:
   - Hero con `[Venta Neta]` filtrado a la tienda
   - Grid 2×2 de KPIs
   - **Line chart** o **Column chart** con `[Venta Neta]` por día (12 últimos días) — sin ejes, sin título, color navy `#10182F`, columna del día actual `#E11A6F`.
   - **Bloque comparativa**: dos Cards (`Venta Neta` y `Venta PY`) + píldora `Texto vs PY`.

---

## 4 · Activar Mobile Layout

Una vez todas las páginas están construidas en sus canvas de 390×1500:

1. `View → Mobile layout`.
2. Aparece un lienzo móvil pequeño (≈320×568) a la derecha.
3. **Arrastra cada visual de la lista lateral** al sitio que le corresponde en el lienzo móvil.
4. Power BI auto-redimensiona, pero **ajusta a mano** para que respete el orden visual del diseño.
5. Asegúrate de que el orden vertical sea: header → period → hero → KPIs 2×2 → tarjetas de canal.

> Si algún visual queda demasiado pequeño en mobile y el texto se corta, **vuelve a la página desktop y reduce el tamaño de fuente de ese visual** hasta que entre. Power BI no escala fuentes automáticamente.

---

## 5 · Cosas que Power BI NO puede hacer (y workarounds)

| Limitación | Workaround |
|---|---|
| `letter-spacing` en Text Box | Usa MAYÚSCULAS con espacios manuales: `L O L A` |
| `border-radius` real en visuales | Sólo Cards admiten rounded corners en "Visual border". El resto: usa Shape Rounded Rectangle de fondo |
| Padding interno por lado independiente | No existe — sólo padding global por visual |
| Tipografías custom como Manrope | Usa Segoe UI (default). Para Manrope: instálala en máquina del usuario; PBI Service NO la verá si no es de la lista oficial |
| `linear-gradient` en fondo | Imagen PNG con el degradado como fondo del visual |
| Sombras CSS | Format → Effects → Shadow (sólo Cards, KPI, Multi-row) |
| Mostrar varios % con colores diferentes en un mismo visual | Cada % = un Card independiente con conditional formatting |
| Mini barras de progreso de altura 4px exacta | Bar chart 100% Stacked al mínimo permitido (~10px). Alternativa: imagen de fondo |
| Componentes reutilizables | No existen — duplica visuales y aplica filtros |

---

## 6 · Checklist final

- [ ] Tema `theme.json` importado.
- [ ] Las 17 medidas DAX creadas sin error.
- [ ] Página `Móvil – Total` 390×1500 con todos los visuales colocados.
- [ ] Página `Móvil – Canal Tienda` lista con drillthrough activado.
- [ ] Página `Detalle Tienda` configurada como destino de drillthrough.
- [ ] Conditional formatting verde/amarillo/rojo funciona en todas las píldoras.
- [ ] Mobile Layout activado y visuales reordenados.
- [ ] Probado en Power BI Mobile (app) — no sólo en desktop.
