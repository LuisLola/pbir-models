# Plan de Servicio B2B — Documentación del modelo

*Última revisión: 24 de noviembre de 2026*

Panel de **plan de servicio B2B** de Lola Casademunt. Mide, sobre la cartera de pedidos de venta **abiertos**, cuánto importe/unidades se puede **servir** y **cuándo** (1ª quincena, 2ª quincena, +30 días) según la fecha de disponibilidad de stock de cada línea, comparándolo con lo pendiente de entregar y el estado de cobertura (picking / asignado / tránsito).

- **Proyecto:** [ServicePlan/Plan de Servicio B2B](../ServicePlan/)
- **Modelo:** `Plan de Servicio B2B.SemanticModel` · **Informe:** `Plan de Servicio B2B.Report`
- **Cultura:** es-ES · **Modo:** Import
- **Tamaño:** 1 tabla de hechos, 5 tablas auxiliares propias, **51 medidas** (todas alojadas en `LOL_PLANMATERIAL`).

> Para consultar el modelo en vivo (DAX) con Power BI Desktop abierto, ver la nota al final.

---

## Origen de datos

| Tabla | Origen | Tipo |
|---|---|---|
| `LOL_PLANMATERIAL` | `SapHana.Database("192.168.0.231:30015")` vía `Value.NativeQuery(...)` | Hechos (import) |
| `Calendario` | Tabla calculada (`ADDCOLUMNS(CALENDAR(...))`) | Dimensión fecha (ligada a *Fecha Entrega pedido venta*) |
| `Horizonte` | Tabla calculada `SELECTCOLUMNS(CALENDAR(2023..2030), "Fecha", [Date])` | Segmentador de fecha de entrega (ver abajo) |
| `RangoDisponibilidad` | Tabla calculada `SELECTCOLUMNS(CALENDAR(2008..2030), "Fecha", [Date])` | **Tabla desconectada**: ventana de fecha de disponibilidad (ver abajo) |
| `VistaTipo` | Tabla calculada `DATATABLE("Vista", {"Importe"}, {"Unidades"})` | Parámetro de UI |
| `UltimaActualizacion` | Consulta M sin origen externo (`DateTimeZone.UtcNow()`) | Sello de refresco (oculta) |

### ⚠️ Pendiente de sacar de HANA

`LOL_PLANMATERIAL` es la **única tabla que lee de SAP HANA** y lo hace **directamente**, sin pasar por el ETL de SQL Server que usan los demás modelos. Es el **modelo pendiente de migrar fuera de HANA**.

Importa de **SAP HANA** (`192.168.0.231:30015`) mediante una **consulta nativa** larga con CTEs (`PICK`, `PACK_HOLD`, `RDR1_CALCULOS`, `DESTALLADOS`, `TOTAL_LOOK`, `PEDIDOS_COMPRA_REL`, `SOL_TRASLADO_REL`) sobre el esquema `SBO_LOLA_SUCURSAL` (SAP Business One). La consulta ya calcula en HANA los importes por línea (con descuentos de línea y cabecera) para picking, asignado, tránsito, pendiente, original y entregado. Filtra a **pedidos abiertos** (`DocStatus='O'`, `LineStatus='O'`).

Consecuencias mientras siga así: la lógica de negocio (cobertura, bloqueo, lanzamiento de picking, servicio con retraso, P&H) vive en el SQL de la partición y no en DAX; cualquier migración debe reproducir esas columnas tal cual. El resto de tablas son calculadas o M sin origen y no dependen de HANA.

---

## Tabla de hechos: `LOL_PLANMATERIAL`

Un registro por **línea de pedido de venta abierta** (artículo + talla + color). Columnas principales:

- **Identificación / dimensiones:** `Nº Pedido Ventas`, `Código Cliente`, `CardName`, `Prioridad Cliente`, `Riesgo Disponible`, `Representante`, `País`, `Almacén`, `Temporada`, `ItemCode`, `EAN`, `Ref. Modelo`, `Color`, `Talla`, `Descripción Artículo`, `Grupo total look`, `Proveedor`, `Orden de compra`, `NºEntrada mcias.`, `Solicitud traslado`, `TipoPedido` (Inicial / Reposición), `P&H` (Pack & Hold).
- **Cantidades:** `Cantidad Original`, `Cantidad Entregada`, `Unidades pendientes Entregar`, `Uniades en Piciking` *(sic)*, `Unidades pte. picking`, `Unidades asignadas`, `Unidades asignadas Transito`.
- **Importes (€):** `Importe Original`, `Importe Entregado`, `Importe pendiente Entregar`, `Import en Piciking` *(sic)*, `Imporet pte. picking` *(sic)*, `Importe asignado`, `AssignTransitLineTotal` (importe asignado en tránsito).
- **Fechas (hitos):**
  - `Fecha pedido venta` — fecha del pedido.
  - `Fecha Entrega pedido venta` (`ShipDate`) — **fecha comprometida de entrega** de la línea. `Semana Entrega` = `WEEK(ShipDate)`.
  - `Fecha Fin pedido venta` = `ShipDate + 15` (fin de la 1ª quincena). **Ancla de las ventanas de las medidas de quincena.**
  - `Fecha disponibilidad (línea del articulo)` (`U_GSP_LOLTRANSITDATE`) — **fecha en que el stock en tránsito estará disponible** para servir la línea. Puede venir **NULL** (≈40% de las líneas).
  - Otras: `Fecha entrega/llegada orden de compra`, `Fecha llegada entrada mcias.`, `Fecha llegada solicitud traslado`.
- **Estado de cobertura:** `Cartera cubierta` (SI/NO), `Cartera 60%`, `Porcentaje Cartera Cubierta`, `Faltas`, `Lanzamiento Picking`, `Servicio con retraso`, `Bloqueado` / `Tipo Bloqueo`, `Autorizado`.

> **Cobertura de una línea = "cubierto" = `Import en Piciking` + `Importe asignado` + `AssignTransitLineTotal`** (lo que ya está en picking + asignado de stock + asignado en tránsito). Es el importe que realmente se puede servir. En unidades, el equivalente es `Uniades en Piciking` + `Unidades asignadas` + `Unidades asignadas Transito`.

### Columna calculada

| Columna | Qué hace |
|---|---|
| **`Clasificación Entrega`** | Clasifica cada línea en `Fecha Servicio 1º Quincena` (`fd ≤ entrega+15`), `2º Quincena` (`≤ entrega+30`) o `> 30 días` (`> entrega+30`), donde `fd` = fecha de disponibilidad. **Las líneas sin fecha de disponibilidad devuelven BLANK()** (primera rama del `SWITCH`: `ISBLANK(FechaDisponible), BLANK()`). |

> Ojo: esta columna conserva los cortes antiguos (`entrega+15` / `entrega+30`); las **medidas** usan desde agosto las ventanas ancladas a `Fecha Fin` con margen de 4 días (ver *Medidas*). Pueden no coincidir línea a línea.

---

## Tabla `Horizonte` (segmentador de fecha de entrega)

- **Definición:** `SELECTCOLUMNS(CALENDAR(DATE(2023,1,1), DATE(2030,12,31)), "Fecha", [Date])` — una columna `Fecha`.
- **Relación:** `Horizonte[Fecha]` (1) → `LOL_PLANMATERIAL[Fecha Entrega pedido venta]` (*), un solo sentido. El segmentador de `Horizonte` **filtra los pedidos** por fecha de entrega.
- **Ya no actúa como tope en las medidas.** El antiguo `VAR Tope = MAX('Horizonte'[Fecha])` se retiró (commit `ce8a488`): con las ventanas ancladas a la fecha fin de cada línea se solapaba y vaciaba el bucket de +30 días. Hoy solo la usan el segmentador y la píldora `Chip Fecha`.

> **Ojo al crear tablas calculadas a mano (TMDL):** el nombre de la columna debe coincidir con el que produce el DAX (por eso `SELECTCOLUMNS(..., "Fecha", [Date])` y no renombrar `[Date]`→`Fecha`), o al **publicar** el servicio da `Missing_References`.

## Tabla `RangoDisponibilidad` (ventana de disponibilidad, desconectada)

- **Definición:** `SELECTCOLUMNS(CALENDAR(DATE(2008,1,1), DATE(2030,12,31)), "Fecha", [Date])`, **sin relaciones**.
- El segmentador de "fecha de disponibilidad" apunta aquí, así que **no filtra** el listado de pedidos ni las tarjetas: solo entra en las medidas de quincena como `VAR RangoIni = MIN('RangoDisponibilidad'[Fecha])` / `VAR RangoFin = MAX(...)`, y la condición `fd >= RangoIni && fd <= RangoFin` se aplica al **tránsito**. Sin selección → 2008–2030 → sin recorte.

## Tabla `UltimaActualizacion` (sello de refresco)

Tabla oculta de una fila generada en M: `DateTimeZone.UtcNow()` + desfase horario de España (UTC+2 entre el último domingo de marzo y el último de octubre, UTC+1 el resto). Alimenta la medida `Última actualización`.

---

## Medidas

**51 medidas**, todas en `LOL_PLANMATERIAL`. Las de negocio están en la raíz (sin carpeta); las añadidas en noviembre van en las carpetas `0 Pildoras` (12) y `0 Contexto` (3).

La lógica común de las medidas de quincena (commit `ce8a488`):
- **Ancla = `Fecha Fin pedido venta`** (`FechaFin` = entrega + 15), con un margen de 4 días:
  - **1ª quincena:** `fd ≤ FechaFin − 4`
  - **2ª quincena:** `FechaFin − 4 < fd ≤ FechaFin + 11`
  - **+30 días:** `fd > FechaFin + 11`
- **Solo el tránsito depende de la fecha.** El material ya en almacén (picking + asignado) se suma **siempre a la 1ª quincena**, tenga o no fecha de disponibilidad. El tránsito solo cuenta en una ventana si `NOT ISBLANK(fd)`, `NOT ISBLANK(FechaFin)` y `fd` cae dentro de `RangoDisponibilidad`.
- En las `(Importe Real)` el almacén es solo `Importe asignado` (el picking se reporta aparte en `en picking`); en los % y en unidades es picking + asignado.

### Servicio por quincena — Valor en importe (€)
| Medida | Lógica | Propósito |
|---|---|---|
| `Valor Servicio 1º Quincena (Importe Real)` | `SUMX`: `Importe asignado` + tránsito si `fd ≤ FechaFin−4` (y en rango) | € servibles en la 1ª quincena. |
| `Valor Servicio 2º Quincena (Importe Real)` | tránsito si `FechaFin−4 < fd ≤ FechaFin+11` | € servibles en la 2ª quincena. |
| `Valor Servicio > 30 días (Importe Real)` | tránsito si `fd > FechaFin+11` | € servibles a >30 días. |
| `Valor Servicio con retraso (Importe Real)` | tránsito si `fd > FechaFin+11` (misma condición que `> 30 días` sin almacén) | € en tránsito que llegan tarde. |

### Servicio por quincena — % sobre pendiente
| Medida | Lógica | Propósito |
|---|---|---|
| `Fecha Servicio 1º Quincena(Importe)` | `DIVIDE(picking + asignado + tránsito_1ªQ, SUM(Importe pendiente Entregar))` | % del pendiente servible en 1ª quincena. |
| `Fecha Servicio 2º Quincena(Importe)` | ídem con tránsito 2ª quincena | % del pendiente servible en 2ª quincena. |
| `Fecha Servicio > 30 días (Importe)` | ídem con tránsito >30 días | % del pendiente servible a >30 días. |

### Servicio por quincena — Unidades
| Medida | Lógica | Propósito |
|---|---|---|
| `Fecha Servicio 1º Quincena` | `DIVIDE(picking + asignadas + tránsito_1ªQ, SUM(Unidades pendientes Entregar))` | % de unidades servibles en 1ª quincena. |
| `Fecha Servicio 2º Quincena` | ídem 2ª quincena (solo tránsito) | % de unidades en 2ª quincena. |
| `Fecha Servicio > 30 días` | ídem >30 días (solo tránsito) | % de unidades a >30 días. |
| `Valor Servicio 1º Quincena` | `SUMX` unidades picking + asignadas + tránsito_1ªQ (formato `#,0`, sin €) | Unidades servibles en 1ª quincena. |
| `Valor Servicio con retraso` | unidades en tránsito con `fd > FechaFin+11` | Unidades que llegan tarde. |

Las medidas de unidades aplican ya las mismas ventanas y el mismo `RangoDisponibilidad` que las de importe.

### Cuadre y reparto de la cobertura
| Medida | Lógica | Propósito |
|---|---|---|
| `Importe cubierto` | `SUM(Import en Piciking) + SUM(Importe asignado) + SUM(AssignTransitLineTotal)` | Cobertura total en €. |
| `Unidades cubiertas` | ídem en unidades | Cobertura total en unidades. |
| `en picking` | `SUM(Import en Piciking)` | € ya en picking. |
| `Importe faltas` | `SUM(Importe pendiente Entregar) − [Importe cubierto]` | € pendientes sin cobertura (el origen no trae columna de importe de faltas). |
| `Importe retrasado` | `[Importe cubierto] − Q1 − Q2 − >30 − [en picking]` (Importe Real) | Tránsito que no cae en ninguna ventana (sin fecha o fuera del rango). |
| `ServicioDentroRango` | `Q1 + Q2 + >30` (Importe Real) | € servibles dentro de las tres ventanas. |
| `Cartera pte. + en picking` | `SUM(Imporet pte. picking) + SUM(Import en Piciking)` | Cartera pendiente más lo que ya está en picking. |
| `% en picking` / `% retrasado` / `% faltas` | `DIVIDE(<medida>, SUM(Importe pendiente Entregar))` | Reparto sobre el pendiente. |
| `% Total` | `DIVIDE(picking + Q1 + Q2 + >30 + retrasado, pendiente)` | Control del cuadre (formato `0.00 %`). |

> **Cuadre:** picking + Q1 + Q2 + >30 + retrasado = `Importe cubierto`; y con `Importe faltas`, los seis componentes suman el 100% del `Importe pendiente Entregar`.

### Cartera / cantidad
| Medida | Lógica | Propósito |
|---|---|---|
| `% Cartera Cubierta Temporada?` | `ROUND(DIVIDE(cubierto, OpenQty), 2)` | % de la cartera abierta cubierta (unidades). |
| `Cantidad Original por Pedido` / `...por Pedido2` | `SUMX` sobre `VALUES/SUMMARIZE(Nº Pedido)` de `Cantidad Original` | Cantidad original agregada evitando duplicar por línea. |

### Indicadores de filtro (`Filtro_*`)
Diez medidas `IF(ISFILTERED(<columna>), "✅", "")` que muestran un check cuando el usuario ha filtrado por esa dimensión: `Filtro_PH_Activo` (P&H), `Filtro_PH_Autorizado`, `Filtro_Bloquead`, `Filtro_Representante`, `Filtro_Pais`, `Filtro_product`, `Filtro_LanzPick`, `Filtro_Temporada`, `Filtro_servretard`, `Filtro_Alm`.

### Carpeta `0 Pildoras` — píldoras de filtro (12)
Seis parejas texto + color, con la plantilla común de los paneles:
- **`Chip <campo>`** = `IF(ISFILTERED(<columna>), <valor seleccionado o "N sel."> & "  ✕")` — vacío si no hay filtro.
- **`Chip <campo> Color`** = `IF(ISFILTERED(<columna>), "#DFA0C9", "#6F263D")`.

| Pareja | Columna |
|---|---|
| `Chip Autorizado` | `LOL_PLANMATERIAL[Autorizado]` |
| `Chip Semana Entrega` | `LOL_PLANMATERIAL[Semana Entrega]` |
| `Chip Servicio con retraso` | `LOL_PLANMATERIAL[Servicio con retraso]` |
| `Chip Fecha` | `Horizonte[Fecha]` |
| `Chip Fecha disponibilidad (línea del articulo)` | `LOL_PLANMATERIAL[Fecha disponibilidad (línea del articulo)]` |
| `Chip Temporada` | `LOL_PLANMATERIAL[Temporada]` |

Solo las tres primeras están colocadas en el informe (página *Agrupación PlanMaterial*): el hueco del cajón de menú no da para más. Las otras tres quedan definidas para cuando haya sitio.

### Carpeta `0 Contexto` — cabecera (3)
| Medida | Lógica | Propósito |
|---|---|---|
| `Última actualización` | `"Actualizado dd/MM/yyyy a las HH:mm"` desde `MAX(UltimaActualizacion[Fecha])` | Sello de último refresco (hora de España). |
| `Rango de fechas` | `MIN`–`MAX` de `Fecha disponibilidad (línea del articulo)` en contexto | Rango que está mirando la página. |
| `Rango y actualización` | `[Rango de fechas] & " · " & [Última actualización]` | Una sola tarjeta de cabecera. |

---

## Relaciones

- `LOL_PLANMATERIAL[Fecha Entrega pedido venta]` → `Calendario[Date]` (dimensión fecha principal).
- `LOL_PLANMATERIAL[Fecha Entrega pedido venta]` → `Horizonte[Fecha]` (un sentido) — segmentador de fecha de entrega.
- `RangoDisponibilidad`, `VistaTipo` y `UltimaActualizacion` **sin relaciones** (desconectadas a propósito).
- Relaciones auto-generadas a `LocalDateTable_*` (seis, una por columna de fecha, jerarquías automáticas) — candidatas a limpieza.

---

## Informe

| Página | Visible |
|---|---|
| Plan de Servicio | Sí (página de inicio) |
| Detalle | Oculta |
| Agrupación PlanMaterial | Oculta |
| DetalleTodo | Oculta |
| Plan Material Cliente | Oculta |
| Original | Oculta |
| Plan de Material | Sí |

Cambios de presentación desde agosto (comunes a los paneles):
- **Menú lateral** (carril + cajón) con marcadores de abrir/cerrar; el contenido se ensancha con el menú cerrado y se desplaza al abrirlo (dos juegos de visuales alternados por marcador).
- **Tema único** `LolaCasademunt.json` (fondo crema `#FFF6ED`, Montserrat también en `visualStyles`, `baseTheme` CY26SU05), ahora declarado en `report.json → resourcePackages` (antes faltaba y el informe no cogía los colores).
- **Escala tipográfica** {8, 9, 10, 11, 12, 16, 19} pt (había 29 usos a 13 pt).
- **Píldoras de filtro** y tarjeta `Rango y actualización` en la cabecera.

---

## Reglas de negocio clave

1. **"Cubierto", no "pendiente".** El valor de servicio suma lo realmente cubierto (picking + asignado + tránsito), no el importe pendiente completo. Coincide con el numerador del % de servicio.
2. **Las líneas sin fecha de disponibilidad no entran en ninguna quincena por su tránsito.** ≈40% de las líneas traen `Fecha disponibilidad` NULL. En DAX `BLANK()` se coacciona a una fecha muy antigua, así que sin protección `BLANK ≤ fecha` es TRUE y esas líneas se colaban en la **1ª quincena** (llegó a inflarla en ≈883.000 €). Arreglo: `NOT ISBLANK(fd)` en las clasificaciones; **verificado en el código actual**: lo llevan las 11 medidas de ventana (unidades, importe, % y "con retraso") y la columna `Clasificación Entrega`. Ese tránsito sin fecha acaba en `Importe retrasado`.
3. **El almacén no depende de la fecha.** Lo que ya está en picking/asignado cuenta en la 1ª quincena aunque la línea no tenga fecha de disponibilidad. (Una versión intermedia lo excluía con la misma guarda `NOT ISBLANK` y perdía 4,84 M€, el 46,6% de la cobertura.)
4. **Quincena por línea, relativa a su fecha fin** (`entrega + 15`), con margen de 4 días: 1ª = `fd ≤ FechaFin−4`; 2ª = `FechaFin−4 < fd ≤ FechaFin+11`; +30 = `fd > FechaFin+11`.
5. **La ventana de disponibilidad sale de `RangoDisponibilidad`, no de `Horizonte`.** El segmentador de disponibilidad solo recorta el tránsito dentro de las medidas; no filtra pedidos ni tarjetas.

> **Historial:** hasta agosto las medidas de importe aplicaban un tope `MAX('Horizonte'[Fecha])` y cortes `entrega+15/+30`. El tope se retiró porque, con las ventanas ancladas a la fecha fin, vaciaba el bucket de +30 días; la ventana pasó a `RangoDisponibilidad`.

---

## Notas y mejoras pendientes

- **Sacar `LOL_PLANMATERIAL` de HANA** (ver *Origen de datos*): es el único modelo que sigue con consulta nativa directa a SAP HANA.
- **`Calendario` solo cubre 2025** (`CALENDAR(DATE(2025,1,1), DATE(2025,12,31))`): las líneas con fecha de entrega fuera de 2025 no encuentran fila en el calendario. Conviene ampliarlo o hacerlo dinámico.
- **`Clasificación Entrega` desalineada** con las medidas (cortes `entrega+15/+30` frente a `FechaFin−4/+11`).
- **`Valor Servicio con retraso` duplica la condición de `> 30 días`** (solo tránsito, `fd > FechaFin+11`); revisar si debe medir otra cosa (p. ej. la columna `Servicio con retraso`).
- **Color de píldora inactiva `#6F263D`** está fuera de la paleta estricta de marca (rosa / negro / blanco).
- **Fecha centinela `01/01/2050`** en las fechas de pedidos sin fecha real de envío: queda fuera de `Horizonte` (2023–2030) y no distorsiona; sin selección no recorta nada.
- **Auto fecha/hora activada:** seis `LocalDateTable_*` ocultas; con `Calendario`, `Horizonte` y `RangoDisponibilidad` propios podrían desactivarse (cambio estructural, revisar el informe).
- **`Import en Piciking` / `Imporet pte. picking` / `Uniades`** son erratas heredadas de los nombres de columna del origen; se mantienen para no romper referencias.

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
