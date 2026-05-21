# Guía paso a paso · Página "Desempeño de Ventas" (escritorio)

Esta página es la **vista canónica de sobremesa** del dashboard. Replica el comportamiento BI2GO actual (tabla expandible jerárquica) con el lenguaje visual de Lola Casademunt.

> **Referencia visual**: `screenshots/desempeno-ventas-desktop.png`

---

## 0 · Lo que debe quedar al final

```
┌──────────────────────────────────────────────────────────────────────┐
│ [LC] Desempeño de ventas ♥          [AÑO|MES|SEMANA|DIA] [← 19 may ─]│  Header
├──────────────────────────────────────────────────────────────────────┤
│ ╔══════════╗ ╔══════════╗ ╔══════════╗ ╔══════════╗                  │
│ ║ V.NETA   ║ ║ T.MEDIO  ║ ║ UPT      ║ ║ %CONV    ║   ← 4 mini KPIs  │
│ ║ 55.013   ║ ║ 108,61   ║ ║ 1,2      ║ ║ 15,9%    ║     con          │
│ ║ ▁▃▅▇▅▇▃  ║ ║ ▃▅▅▆▇▆▇  ║ ║ ▅▃▅▄▅▄▅  ║ ║ ▄▅▆▆▇▇▇  ║     sparklines  │
│ ║[Año][Mes]║ ║[Año][Mes]║ ║[Año][Mes]║ ║[Año][Mes]║                  │
│ ║[Sem][Día]║ ║[Sem][Día]║ ║[Sem][Día]║ ║[Sem][Día]║                  │
│ ╚══════════╝ ╚══════════╝ ╚══════════╝ ╚══════════╝                  │
├──────────────────────────────────────────────────────────────────────┤
│ Desempeño por [Tipo Tienda▼] [Tienda▼] [Categoría▼] [Familia▼]  [⤡][⤢│
│ ╔════════════╤══════╤═══════╤═══════╤═════╤═══════╤════╤═══════════╗ │
│ ║ TIPO/TDA/CAT│SHARE │ VENTA │ OBJ   │ EVO │ V.PY  │EVO │ ... etc   ║ │
│ ╠════════════╪══════╪═══════╪═══════╪═════╪═══════╪════╪═══════════╣ │
│ ║ TOTALES    │100%  │55.013 │96.000 │-43% │79.840 │-31%│ ...       ║ │
│ ║ + TIENDA   │40%   │22.015 │37.950 │-42% │28.571 │-23%│ ...       ║ │
│ ║ + CORNER   │36%   │19.745 │33.350 │-41% │26.082 │-24%│ ...       ║ │
│ ║ − ONLINE   │18%   │ 9.983 │13.520 │-26% │17.088 │-42%│ ...       ║ │
│ ║   − T.Web  │73%   │ 7.280 │10.400 │-30% │13.487 │-46%│ ...       ║ │
│ ║      Prenda│66%   │ 4.789 │     0 │ +0% │ 9.685 │-51%│ ...       ║ │
│ ║      Acces.│34%   │ 2.491 │     0 │ +0% │ 3.802 │-34%│ ...       ║ │
│ ║   ECI Onl. │27%   │ 2.703 │ 3.120 │-13% │ 3.601 │-25%│ ...       ║ │
│ ║ + OUTLET   │ 6%   │ 3.270 │11.180 │-71% │ 8.099 │-60%│ ...       ║ │
│ ╚════════════╧══════╧═══════╧═══════╧═════╧═══════╧════╧═══════════╝ │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 1 · Preparación

### 1.1 Crear la página
1. `Insert → New page` → renombra a **`Desempeño de Ventas`**.
2. `Format → Canvas settings`:
   - Type: **Custom**
   - Width: **1440**, Height: **900** (mínimo; el lienzo crece con el contenido)
3. `Page background`: `#FFFFFF`.
4. `Wallpaper / outspace color`: `#F4F1EA`.

### 1.2 Cargar medidas adicionales
Abre `measures-desempeno.dax` y crea las medidas:
- `% Evol Año`, `% Evol Mes`, `% Evol Semana`, `% Evol Día`
- `Color Evol [bucket]` y `Bg Evol [bucket]`
- `Txt Evol [bucket]` y `Lbl [bucket]`
- `% Share Total`, `% Share Bar`

> Repite los `% Evol` para Ticket Medio, UPT y % Conversión cambiando la medida base.

---

## 2 · Header (alto 64px, y=0)

| Visual | Tipo | Propiedades |
|---|---|---|
| Logo box | **Shape › Rectangle** | Fill `#E11A6F`, position `36, 18`, size `36×36`, rounded corners 8. |
| Texto "LC" | **Text Box** | Font Segoe UI Black 16, color blanco, encima del logo box. |
| Título | **Text Box** | Texto "Desempeño de ventas", Segoe UI Bold 14, color `#0A0F1E`, x=88, y=18. |
| Icono ♥ | **Image** o Text Box "♥" | Color `#E11A6F`, x=240, y=22. |
| Subtítulo | **Text Box** | "Vista jerárquica · expande por dimensión", Segoe UI Regular 11, color `#8089A2`, x=88, y=38. |

### 2.1 Selector de tipo de período (Año/Mes/Semana/Día)
- Inserta un **Slicer** con la columna `DimFecha[TipoPeriodo]` (una columna calculada con valores "Año", "Mes", "Semana", "Día").
- Format → Slicer settings → Style: **Tile** (botones en línea).
- Position: x=970, y=14, size `230×40`.
- Tema:
  - Background unselected: `#F7F6F2`, text `#8089A2`.
  - Background selected: `#FFFFFF` con sombra leve, text `#0A0F1E`, font Segoe UI Bold 11.
  - Border: 1px `#E4E7F0`, rounded 6.

### 2.2 Navegador de fecha (← 19 may. 2026 →)
Power BI no tiene un slicer "end-date con flecha hacia atrás". Replícalo con **3 visuales**:

1. **Botón ← (back)**: `Insert → Buttons → Blank`.
   - Action: Page navigation / **Bookmark**.
   - Crea un bookmark `Periodo_Anterior` que cambia el slicer de fecha a un periodo anterior. Para múltiples saltos, crea bookmarks `Sem_Anterior`, `Mes_Anterior`, etc. y vincúlalos al selector de tipo de período.
   - Alternativa pragmática: usa **Slicer entre fechas** y deja la flecha como decorativa.
2. **Card** con la medida: `MAX ( DimFecha[Fecha] )` formateado "dd MMM. yyyy".
3. **Botón → (forward)**: igual pero deshabilitado/gris cuando no hay datos futuros.

Position del grupo: x=1210, y=14, size `220×40`.

---

## 3 · Strip de 4 mini KPIs (y=80, alto 180px)

Cada KPI = **un grupo de visuales** dentro de un Shape rectangle de fondo. Replica 4 veces:

### 3.1 Anatomía de un mini KPI
```
┌─────────────────────────────────────────┐
│ VENTA NETA CON IVA            ▁▃▅▇▅▇▃▁   │  ← label + sparkline
│ € 55.013                                 │  ← Card grande
├─────────────────────────────────────────┤
│  AÑO     MES      SEM     DÍA            │  ← 4 labels
│  +24%    +11%    -18%    -31%            │  ← 4 píldoras de color
└─────────────────────────────────────────┘
```

| Elemento | Tipo | Propiedades |
|---|---|---|
| Fondo card | Shape rectangle | Fill `#FFFFFF`, border `#E4E7F0` 1px, rounded corners 12, position+size por KPI (ver tabla abajo). |
| Eyebrow | Text Box | Segoe UI Semibold 10 UPPERCASE letter-spacing simulado, color `#8089A2`. |
| Card valor | Card | Medida (`[Venta Neta]` / `[Ticket Medio]` / `[UPT]` / `[% Conversion]`). Callout 28pt Bold. |
| Sparkline | **Line chart** | Datos: `[Venta Neta]` por `DimFecha[Fecha]` últimos 12 puntos. Sin ejes, sin labels, sin título. Color línea `#0A0F1E`. Height 56px, position alineada arriba-derecha. |
| Línea PY | Misma Line chart, segunda serie | Color `#8089A2`, dashed 3-3 (stroke-dasharray). |
| Píldora bucket × 4 | Card pequeña | Texto `[Txt Evol Año]`. Conditional formatting: Fontcolor=`[Color Evol Año]`, Background=`[Bg Evol Año]`. Rounded 4. |
| Label bucket × 4 | Text Box o Card | Texto `[Lbl Año]`. Segoe UI Semibold 9 UPPERCASE, color `#8089A2`. |

**El primero (Venta Neta) tiene fondo navy** `#10182F` con texto blanco — es el hero. Los demás son fondo blanco.

### 3.2 Posiciones (relativas a y=80)
| KPI | x | y | w | h |
|---|---|---|---|---|
| Venta Neta (navy) | 36   | 80 | 332 | 180 |
| Ticket Medio    | 380  | 80 | 332 | 180 |
| UPT             | 724  | 80 | 332 | 180 |
| % Conversión    | 1068 | 80 | 336 | 180 |

---

## 4 · Filter chain (y=280, alto 56px)

Una barra horizontal con los 4 niveles de jerarquía a desplegar.

| Visual | Tipo | Propiedades |
|---|---|---|
| Fondo | Shape rectangle | Fill `#FFFFFF`, border `#E4E7F0`, rounded 12 arriba (radius solo top), position `36, 280, 1368×56`. |
| Label "DESEMPEÑO POR" | Text Box | Segoe UI Semibold 10 UPPERCASE, color `#8089A2`, x=52, y=298. |
| Slicer Tipo Tienda | **Slicer (Dropdown)** | Field: `DimCanal[Canal]` (o `DimTipoTienda[Tipo]`). Pill style, fondo `#FBE6EF`, color `#E11A6F`. Position x=200, y=294. |
| Slicer Tienda | Slicer | Field: `DimTienda[Tienda]`. Pill style, fondo transparente. |
| Slicer Categoría | Slicer | Field: `DimCategoria[Categoria]`. |
| Slicer Familia | Slicer | Field: `DimFamilia[Familia]`. |
| Botones acción | Buttons (×4) | Expandir, Colapsar, Filtros, Exportar — al estilo del mockup, alineados a la derecha. |

> Power BI no tiene "chain de slicers" nativo. Pero los 4 slicers ya filtran la Matrix por debajo de ellos, así que el efecto es equivalente.

---

## 5 · Matrix expandible (y=336)

**Este es el componente CLAVE de la página.** No uses Table — usa **Matrix**.

### 5.1 Crear la Matrix
1. `Insert → Visual → Matrix`.
2. **Rows** (en orden):
   - `DimCanal[Canal]`         (nivel 0)
   - `DimTienda[Tienda]`        (nivel 1)
   - `DimCategoria[Categoria]`  (nivel 2)
   - `DimFamilia[Familia]`      (nivel 3)
3. **Values** (en orden):
   - `[% Share Total]` con format "0%" + **data bars** condicionales fucsia.
   - `[Venta Neta]` con format "#,##0".
   - `[Objetivo]` con format "#,##0".
   - `[% vs Objetivo]` con format "+0%;-0%;0%" + conditional formatting de fuente y fondo.
   - `[Venta PY]` con format "#,##0".
   - `[% vs PY]` + conditional formatting.
   - `[Tasa Devolucion]` con format "0,0%".
   - `[Evol Devolucion]` + conditional formatting.
   - `[Num Ventas]`.
   - `[Evol Num Ventas]` + conditional formatting.
   - `[Ticket Medio]`.
   - `[Evol Ticket Medio]` + conditional formatting.
   - `[UPT]`.
   - `[Evol UPT]` + conditional formatting.
   - `[% Atraccion]` + conditional formatting de evol.
   - `[% Conversion]` + conditional formatting de evol.

### 5.2 Estilo de la Matrix
| Propiedad | Valor |
|---|---|
| Style preset | Minimal |
| Grid → vertical grid | Off |
| Grid → horizontal grid | On, color `#E4E7F0`, weight 1 |
| Column headers → background | `#F7F6F2` |
| Column headers → font | Segoe UI Semibold 10, color `#8089A2`, UPPERCASE |
| Row headers → background | Off (transparente) |
| Row headers → font | Segoe UI Semibold 12 (parents), Segoe UI 12 (children) |
| Values → font | Segoe UI 12 (regular), Segoe UI Bold para totales |
| Values → font family for numbers | Cambia a Consolas si quieres look mono (PBI no tiene JetBrains Mono) |
| Stepped layout | **OFF** (queremos que cada nivel tenga su propia columna de indentación visual con icono de expandir) |
| Subtotals (Row) | On para nivel 0; el de "Totales" se muestra arriba |

### 5.3 Activar drill-down expandible
1. Click en la Matrix.
2. En el panel derecho `Visualizations → Drill mode`, activa el icono **"Expand"** (flecha doble hacia abajo).
3. Verifica que aparezcan los `+` / `−` al lado de cada fila para expandir.

### 5.4 Conditional formatting de las píldoras de evolución
Para cada columna `% vs [...]`:
1. Click columna en `Values`.
2. `Conditional formatting → Background color → Field value → [Bg Evol Año]` (o la medida correspondiente).
3. `Conditional formatting → Font color → Field value → [Color Evol Año]`.

### 5.5 Data bars en columna Share
1. Click columna `% Share Total`.
2. `Conditional formatting → Data bars → On`.
3. Color positivo: `#E11A6F`. Mostrar números: On.

### 5.6 Tamaño y posición
- Position: x=36, y=336
- Size: 1368 × 540 (ajusta height según contenido)

---

## 6 · Footer leyenda (y=900, alto 40px)

Una barra inferior con la leyenda del semáforo.

| Elemento | Tipo |
|---|---|
| Fondo | Shape rectangle, fill `#FFFFFF`, border-top `#E4E7F0` 1px. |
| Leyenda verde | Text Box "● Positivo · ≥ 0", color `#1F8A5B`, Segoe UI 11. |
| Leyenda amarilla | Text Box "● Atención · entre −10% y 0", color `#B57A0E`. |
| Leyenda roja | Text Box "● Crítico · < −10%", color `#C73838`. |
| Marca | Text Box "Lola Casademunt · Cuadro de mando comercial", derecha. |

---

## 7 · Filtros sincronizados

- Los 4 slicers de la filter chain (Tipo Tienda, Tienda, Categoría, Familia) **deben sincronizarse** entre sí (cuando seleccionas un Tipo Tienda, el slicer Tienda solo muestra esas tiendas).
- Power BI lo hace automáticamente porque las dimensiones están relacionadas en el modelo.
- El slicer de fecha del header filtra TODOS los visuales (sin sync con los KPIs ya que ya filtra todo).

---

## 8 · Checklist final

- [ ] Página `Desempeño de Ventas` creada con canvas 1440×900.
- [ ] 9 medidas DAX nuevas creadas (`% Evol Año/Mes/Semana/Día` × ventas, TM, UPT, conversión).
- [ ] 8 medidas helper (`Color Evol`, `Bg Evol`, `Txt Evol`, `Lbl`).
- [ ] Header con logo + título + ♥ + selector tipo período + navegador fecha.
- [ ] 4 mini KPI cards (Venta Neta navy hero, otros 3 blancos) con sparkline + 4 buckets de evolución.
- [ ] Filter chain con 4 slicers pill encadenados.
- [ ] Matrix visual con 4 niveles de jerarquía expandibles (Canal → Tienda → Categoría → Familia).
- [ ] Conditional formatting verde/amarillo/rojo en TODAS las columnas de evolución.
- [ ] Data bars fucsia en columna Share.
- [ ] Footer con leyenda del semáforo.
- [ ] Probado: expandir TIENDA muestra las tiendas físicas. Expandir una tienda muestra categorías. Expandir categoría muestra familias.

---

## 9 · Cosas que Power BI NO puede hacer en esta página (y workarounds)

| Limitación | Workaround |
|---|---|
| Sparkline en Card visual | Usa Line chart pequeño al lado del Card. PBI 2023+ permite sparkline dentro de Matrix cells. |
| Bordes redondeados en cabeceras de Matrix | No soportado. Acepta esquinas rectas. |
| Iconos custom en la columna de jerarquía | Difícil. Acepta los `+`/`−` nativos de PBI. |
| Píldoras con punto de color delante del % | No soportado en Matrix. Usa solo fondo + fuente coloreados. Pierdes el punto pero conservas la legibilidad. |
| "Tipo Período" como segmented (Año/Mes/Sem/Día) en bonito | Slicer Tile estilo botones se acerca. No será idéntico. |
| Botones ← → de navegación de fecha con lógica dinámica | Requiere bookmarks por cada periodo, o un slicer entre fechas que el usuario ajuste manualmente. |

---

## 10 · Verificación visual

Una vez todo montado, compara con `screenshots/desempeno-ventas-desktop.png`. Las diferencias inevitables serán:
- Fuentes (Segoe UI vs Manrope): aceptable.
- Esquinas de Matrix (rectas vs redondeadas): aceptable.
- Punto de color en píldoras: ausente, aceptable.

**Lo que DEBE coincidir**:
- ✅ Layout general (header → KPIs → filtros → tabla → footer).
- ✅ Colores: rosa `#E11A6F`, navy `#10182F`, semáforo verde/amarillo/rojo.
- ✅ Matrix expandible con 4 niveles funcionando.
- ✅ Sparklines en los KPIs.
- ✅ 4 buckets de evolución en cada KPI.
