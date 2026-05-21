# Prompt para Claude Code · Página "Desempeño de Ventas" (escritorio)

> Pega este prompt en Claude Code cuando ya tengas la versión móvil terminada
> y quieras empezar la página de escritorio.

---

## Antes de empezar

**Sube a Claude Code estas imágenes**:
1. `screenshots/desempeno-ventas-desktop.png` ← referencia visual definitiva
2. La captura de tu BI2GO actual (la que tiene la tabla expandible TIPO TIENDA → TIENDA → CATEGORÍA → FAMILIA)

---

## Prompt (cópialo tal cual)

```
Ya tienes implementado el Panel Móvil (página ① con drilldown a ② y ③).
Ahora pasamos a la SEGUNDA página: "Desempeño de Ventas" (vista escritorio).

CONTEXTO IMPORTANTE:
- Esta página NO es móvil. Es la vista de sobremesa que usan dirección y
  responsables de área desde Power BI Desktop / Service en pantalla grande.
- Replica el comportamiento de su BI2GO actual: UNA SOLA pantalla con una
  tabla expandible jerárquica (Tipo Tienda → Tienda → Categoría → Familia).
- NO hay drillthrough a otras páginas. Todo en una.
- Las medidas DAX base (Venta Neta, Objetivo, Venta PY, Ticket Medio, UPT,
  % Conversión, % Atracción) ya existen en mi modelo. NO las recrees.
- SÍ necesito que crees las medidas adicionales del archivo
  `powerbi/desempeno_ventas/measures-desempeno.dax` — son helpers para los
  time-buckets (Año/Mes/Semana/Día) y los semáforos.

DOCUMENTACIÓN A LEER (en este orden):
1. `screenshots/desempeno-ventas-desktop.png` (referencia visual)
2. `powerbi/desempeno_ventas/DESKTOP_LAYOUT_GUIDE.md` (paso a paso)
3. `powerbi/desempeno_ventas/measures-desempeno.dax` (medidas nuevas)
4. `screens-desktop-unified.jsx` (referencia React si necesitas ver detalles)

CONCEPTO CLAVE — MATRIX, NO TABLE:
La tabla expandible se construye con un visual MATRIX (no Table). La Matrix
permite jerarquías encadenadas en `Rows` y muestra los iconos `+`/`−` para
expandir/colapsar cada nivel. La Table NO permite esto.

  Rows de la Matrix (en orden):
    1. DimCanal[Canal]         (nivel 0: TIENDA, CORNER ECI, ONLINE, OUTLET)
    2. DimTienda[Tienda]        (nivel 1: tiendas físicas o webs)
    3. DimCategoria[Categoria]  (nivel 2: Prenda, Accesorio...)
    4. DimFamilia[Familia]      (nivel 3: subcategorías finales)

Si los nombres reales de mis dimensiones difieren, pregúntame.

PROCESO (espera mi confirmación entre pasos):

PASO 1 — Pregúntame:
  - Nombres exactos de las dimensiones de mi modelo (Canal, Tienda,
    Categoría, Familia, Fecha).
  - Si ya tengo medidas equivalentes a `% Evol Año/Mes/Semana/Día`.
  - Si he aplicado `powerbi/theme.json`.

PASO 2 — Crea las medidas DAX adicionales que falten.
  Importante: para `% Evol Semana` y `% Evol Día`, el cálculo es vs MISMO
  PERIODO AÑO ANTERIOR (no semana/día anterior). Si el .dax que te paso
  no calza con cómo está estructurada mi DimFecha, AVÍSAME antes de
  inventar lógica.

PASO 3 — Construye la página `Desempeño de Ventas` en el .pbip. Hazlo
en este orden, pausando tras cada sub-paso:

  3.1 Crear la página con canvas 1440×900
  3.2 Header (logo + título + ♥ + selector tipo período + navegador fecha)
  3.3 Strip de 4 mini KPIs (Venta Neta navy, Ticket Medio, UPT, % Conversión)
       Cada uno con: Card + sparkline Line chart + 4 píldoras de bucket
  3.4 Filter chain con los 4 slicers (Tipo Tienda, Tienda, Categoría, Familia)
  3.5 Matrix visual con:
       - 4 niveles de jerarquía en Rows
       - Todas las columnas de Values
       - Conditional formatting verde/amarillo/rojo en cada Evol %
       - Data bars en columna Share
       - Drill mode "Expand" activado
  3.6 Footer con leyenda del semáforo

PASO 4 — Verifica:
  - Que al click en `+` de TIENDA expanda y muestre las tiendas físicas.
  - Que al click en `+` de una tienda muestre categorías.
  - Que al click en `+` de una categoría muestre familias.
  - Que los colores del semáforo se apliquen.
  - Que los slicers de la filter chain filtren la Matrix.

REGLAS:
- NO uses Table — usa Matrix. Es lo único que soporta jerarquías expandibles.
- NO inventes medidas. Si una del .dax no funciona con mi modelo, pregunta.
- NO toques el theme.json — sólo medidas y visuales de esta página.
- NO toques otras páginas (Panel Móvil, Canal Tienda, etc.).
- Después de cada paso, dame un screenshot SI puedes (probablemente no
  puedas — entonces dame la lista de visuales creados/modificados con sus
  IDs, posiciones y propiedades clave para que YO valide abriendo el .pbip).

LIMITACIONES CONOCIDAS DE POWER BI (no intentes superarlas):
- Matrix no soporta puntos de color dentro de las píldoras. Acepta sólo
  fondo + fuente coloreados.
- Sparklines en Card no existen; usa Line chart pequeño al lado.
- Bordes redondeados en Matrix headers no soportado. Acepta esquinas rectas.
- Tipografía Manrope no carga en Service. Usa Segoe UI (default).

Empieza por el PASO 1.
```

---

## Si Claude Code se atasca

### Síntoma: la Matrix no muestra los `+` para expandir
**Causa**: Drill mode no está activado.
**Fix**: Click en la Matrix → panel Visualizations → Drill icon (flecha doble abajo) → activar.

### Síntoma: el conditional formatting no aplica
**Causa**: La medida `[Color Evol Año]` no existe o devuelve formato incorrecto.
**Fix**: Verifica que la medida devuelve un HEX válido (#RRGGBB). PBI exige formato HEX, no nombres CSS.

### Síntoma: la matrix muestra todos los niveles desplegados de golpe
**Causa**: Stepped layout activado.
**Fix**: Format → Row headers → Stepped layout → **Off**.

### Síntoma: el slicer "Tipo Período" no afecta a los KPIs
**Causa**: Slicer no está sincronizado con todos los visuales de la página.
**Fix**: Click slicer → menú `...` → `Sync slicers` → activar para esta página en todos los visuales.

### Síntoma: la flecha ← de navegación no hace nada
**Causa**: No tiene action asignada.
**Fix**: Usa **Bookmarks** — crea un bookmark "Periodo Anterior" con el slicer de fecha en el periodo previo. Asocia el botón ← a ese bookmark vía `Action → Bookmark`.

Si el bookmark dinámico es muy complejo, **simplifica**: deja el slicer entre fechas estándar y omite las flechas ← →. El usuario puede arrastrar el slicer manualmente. Es una concesión razonable.

---

## Validación final
  
Cuando Claude Code declare "página terminada", abre el `.pbip` en Power BI Desktop y verifica:

| Test | Esperado |
|------|----------|
| Tabla expandible click + en TIENDA | Muestra tiendas físicas hijas |
| Doble click + en una tienda | Muestra categorías |
| Doble click + en categoría | Muestra familias |
| Click - en una fila expandida | Colapsa |
| Semáforo en columna Evol vs OBJ | Verde si +%, amarillo si entre -10 y 0, rojo si <-10 |
| Slicer Tipo Tienda | Al seleccionar "TIENDA", Matrix solo muestra esa rama |
| Selector Año / Mes / Semana / Día | Cambia el contexto de los 4 buckets de los KPI cards |

Si algún test falla → manda screenshot a Claude Code con el problema concreto.

Si TODO pasa → exporta a Power BI Service y verifica en navegador (web).
