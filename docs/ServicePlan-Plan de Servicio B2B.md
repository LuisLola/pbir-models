# Plan de Servicio B2B — Documentación del modelo

Panel de **plan de servicio B2B** de Lola Casademunt. Mide, sobre la cartera de pedidos de venta **abiertos**, cuánto importe/unidades se puede **servir** y **cuándo** (1ª quincena, 2ª quincena, +30 días) según la fecha de disponibilidad de stock de cada línea, comparándolo con lo pendiente de entregar y el estado de cobertura (picking / asignado / tránsito).

- **Proyecto:** [ServicePlan/Plan de Servicio B2B](../ServicePlan/)
- **Modelo:** `Plan de Servicio B2B.SemanticModel` · **Informe:** `Plan de Servicio B2B.Report`
- **Cultura:** es-ES · **Modo:** Import

> Para consultar el modelo en vivo (DAX) con Power BI Desktop abierto, ver la nota al final.

---

## Origen de datos

| Tabla | Origen | Tipo |
|---|---|---|
| `LOL_PLANMATERIAL` | `SapHana.Database("192.168.0.231:30015")` vía `Value.NativeQuery(...)` | Hechos (import) |
| `Calendario` | Tabla calculada (`CALENDAR`) | Dimensión fecha (ligada a *Fecha Entrega pedido venta*) |
| `Horizonte` | Tabla calculada `SELECTCOLUMNS(CALENDAR(2023..2030), "Fecha", [Date])` | **Dimensión de horizonte** (ver abajo) |
| `VistaTipo` | Tabla auxiliar (selector) | Parámetro de UI |

A diferencia de otros modelos (SQL Server), este importa de **SAP HANA** (`192.168.0.231:30015`) mediante una **consulta nativa** larga con CTEs (`PICK`, `PACK_HOLD`, `RDR1_CALCULOS`, `DESTALLADOS`, `TOTAL_LOOK`, `PEDIDOS_COMPRA_REL`, `SOL_TRASLADO_REL`) sobre el esquema `SBO_LOLA_SUCURSAL` (SAP Business One). La consulta ya calcula en HANA los importes por línea (con descuentos de línea y cabecera) para picking, asignado, tránsito, pendiente, original y entregado. Filtra a **pedidos abiertos** (`DocStatus='O'`, `LineStatus='O'`).

---

## Tabla de hechos: `LOL_PLANMATERIAL`

Un registro por **línea de pedido de venta abierta** (artículo + talla + color). Columnas principales:

- **Identificación / dimensiones:** `Nº Pedido Ventas`, `Código Cliente`, `CardName`, `Representante`, `País`, `Almacén`, `Temporada`, `ItemCode`, `EAN`, `Ref. Modelo`, `Color`, `Talla`, `Descripción Artículo`, `Proveedor`, `Orden de compra`, `TipoPedido` (Inicial / Reposición), `P&H` (Pack & Hold).
- **Cantidades:** `Cantidad Original`, `Cantidad Entregada`, `Unidades pendientes Entregar`, `Uniades en Piciking` *(sic)*, `Unidades pte. picking`, `Unidades asignadas`, `Unidades asignadas Transito`.
- **Importes (€):** `Importe Original`, `Importe Entregado`, `Importe pendiente Entregar`, `Import en Piciking` *(sic)*, `Imporet pte. picking` *(sic)*, `Importe asignado`, `AssignTransitLineTotal` (importe asignado en tránsito).
- **Fechas (hitos):**
  - `Fecha pedido venta` — fecha del pedido.
  - `Fecha Entrega pedido venta` (`ShipDate`) — **fecha comprometida de entrega** de la línea. Base de la clasificación por quincena.
  - `Fecha Fin pedido venta` = `ShipDate + 15` (fin de la 1ª quincena). **Campo del slicer de fecha del informe.**
  - `Fecha disponibilidad (línea del articulo)` (`U_GSP_LOLTRANSITDATE`) — **fecha en que el stock estará disponible/en tránsito** para servir la línea. Puede venir **NULL** (≈40% de las líneas).
  - Otras: `Fecha entrega/llegada orden de compra`, `Fecha llegada entrada mcias.`, `Fecha llegada solicitud traslado`.
- **Estado de cobertura:** `Cartera cubierta` (SI/NO), `Cartera 60%`, `Porcentaje Cartera Cubierta`, `Faltas`, `Lanzamiento Picking`, `Servicio con retraso`, `Bloqueado` / `Tipo Bloqueo`, `Autorizado`.

> **Cobertura de una línea = "cubierto" = `Import en Piciking` + `Importe asignado` + `AssignTransitLineTotal`** (lo que ya está en picking + asignado de stock + asignado en tránsito). Es el importe que realmente se puede servir. En unidades, el equivalente es `Uniades en Piciking` + `Unidades asignadas` + `Unidades asignadas Transito`.

### Columna calculada

| Columna | Qué hace |
|---|---|
| **`Clasificación Entrega`** | Clasifica cada línea en `Fecha Servicio 1º Quincena` (`fd ≤ entrega+15`), `2º Quincena` (`≤ entrega+30`) o `> 30 días` (`> entrega+30`), donde `fd` = fecha de disponibilidad. **Las líneas sin fecha de disponibilidad devuelven BLANK()** (no se clasifican en ninguna quincena). |

---

## Tabla `Horizonte` (dimensión de corte de fecha)

Tabla **de calendario desconectada** creada para acotar las medidas de servicio a la **fecha seleccionada** en el informe, sin que el contexto de fila de la matriz "colapse" el cálculo.

- **Definición:** `SELECTCOLUMNS(CALENDAR(DATE(2023,1,1), DATE(2030,12,31)), "Fecha", [Date])` — una columna `Fecha`.
- **Relación:** `Horizonte[Fecha]` (1) → `LOL_PLANMATERIAL[Fecha Fin pedido venta]` (*), **un solo sentido**. Así el slicer de `Horizonte` **filtra los pedidos** por fecha fin **y** alimenta el tope de las medidas.
- **Uso en las medidas:** `VAR Tope = MAX('Horizonte'[Fecha])` y condición extra `&& FechaDisponible <= Tope`. Sin selección en el slicer → `Tope` = máximo de la tabla (2030) → **sin tope** (valores completos).

> **Por qué existe:** el slicer de "Fecha Fin pedido venta" está a la vez en el slicer y en las filas de la matriz. Leer su máximo dentro de la medida (`ALLSELECTED`/`MAX`) **colapsaba** al valor de la fila (la fecha fin del propio pedido), dejando la 2ª quincena y +30 días siempre a 0. Una tabla **desconectada** (que nunca está en las filas) resuelve el tope de forma robusta.
>
> **Ojo al crear tablas calculadas a mano (TMDL):** el nombre de la columna debe coincidir con el que produce el DAX (por eso `SELECTCOLUMNS(..., "Fecha", [Date])` y no renombrar `[Date]`→`Fecha`), o al **publicar** el servicio da `Missing_References`.

---

## Medidas

La lógica común de todas las medidas de servicio:
- **Importe** = *cubierto* = picking + asignado + tránsito (no el pendiente).
- **Excluye** las líneas sin fecha de disponibilidad (`NOT ISBLANK`).
- Las de **Importe** aplican el **tope de `Horizonte`** (`fd ≤ Tope`); las de **unidades** aún **no** (pendiente).

### Servicio por quincena — Valor en importe (€)
| Medida | Lógica | Propósito |
|---|---|---|
| `Valor Servicio 1º Quincena (Importe Real)` | `SUMX` cubierto donde `NOT ISBLANK(fd) && fd ≤ entrega+15 && fd ≤ Tope` | € servibles en la 1ª quincena. |
| `Valor Servicio 2º Quincena (Importe Real)` | igual con `entrega+15 < fd ≤ entrega+30 && fd ≤ Tope` | € servibles en la 2ª quincena. |
| `Valor Servicio > 30 días (Importe Real)` | igual con `fd > entrega+30 && fd ≤ Tope` | € servibles a >30 días. |

### Servicio por quincena — % sobre pendiente
| Medida | Lógica | Propósito |
|---|---|---|
| `Fecha Servicio 1º Quincena(Importe)` | `DIVIDE(cubierto_1ªQ, SUM(Importe pendiente Entregar))` (mismo filtro + Tope) | % del pendiente servible en 1ª quincena. |
| `Fecha Servicio 2º Quincena(Importe)` | ídem 2ª quincena | % del pendiente servible en 2ª quincena. |
| `Fecha Servicio > 30 días (Importe)` | ídem >30 días | % del pendiente servible a >30 días. |

### Servicio por quincena — Unidades (%) *(sin tope de horizonte)*
| Medida | Lógica | Propósito |
|---|---|---|
| `Fecha Servicio 1º Quincena` | `DIVIDE(unidades_cubiertas_1ªQ, SUM(Unidades pendientes Entregar))`, `NOT ISBLANK(fd)` | % de unidades servibles en 1ª quincena. |
| `Fecha Servicio 2º Quincena` | ídem 2ª quincena | % de unidades en 2ª quincena. |
| `Fecha Servicio > 30 días` | ídem >30 días | % de unidades a >30 días. |
| `Valor Servicio 1º Quincena` | `SUMX` unidades cubiertas donde `NOT ISBLANK(fd) && FechaFin ≥ fd` (formato €) | Unidades servibles en 1ª quincena. |

### Cartera / cantidad
| Medida | Lógica | Propósito |
|---|---|---|
| `% Cartera Cubierta Temporada?` | `ROUND(DIVIDE(cubierto, OpenQty), 2)` | % de la cartera abierta cubierta (unidades). |
| `Cantidad Original por Pedido` / `...por Pedido2` | `SUMX` sobre `VALUES/SUMMARIZE(Nº Pedido)` de `Cantidad Original` | Cantidad original agregada evitando duplicar por línea. |

### Indicadores de filtro (`Filtro_*`)
Diez medidas `IF(ISFILTERED(<columna>), "✅", "")` que muestran un check cuando el usuario ha filtrado por esa dimensión: `Filtro_PH_Activo` (P&H), `Filtro_PH_Autorizado`, `Filtro_Bloquead`, `Filtro_Representante`, `Filtro_Pais`, `Filtro_product`, `Filtro_LanzPick`, `Filtro_Temporada`, `Filtro_servretard`, `Filtro_Alm`.

---

## Relaciones

- `LOL_PLANMATERIAL[Fecha Entrega pedido venta]` → `Calendario[Date]` (dimensión fecha principal).
- **`Horizonte[Fecha]` → `LOL_PLANMATERIAL[Fecha Fin pedido venta]`** (un sentido) — slicer de horizonte que filtra pedidos y alimenta el tope.
- Relaciones auto-generadas a `LocalDateTable_*` (una por columna de fecha, jerarquías automáticas) — candidatas a limpieza.

---

## Reglas de negocio clave

1. **"Cubierto", no "pendiente".** El valor de servicio suma lo realmente cubierto (picking + asignado + tránsito), no el importe pendiente completo. Coincide con el numerador del % de servicio.
2. **Las líneas sin fecha de disponibilidad no cuentan.** ≈40% de las líneas traen `Fecha disponibilidad` NULL. En DAX `BLANK()` se coacciona a una fecha muy antigua, así que sin protección `BLANK ≤ entrega+15` es TRUE y esas líneas se colaban en la **1ª quincena**, inflando el importe. Todas las clasificaciones llevan `NOT ISBLANK(fd)` (y la columna `Clasificación Entrega` devuelve BLANK para ellas).
3. **Quincena por línea, relativa a su fecha de entrega.** 1ª = `fd ≤ entrega+15`; 2ª = `entrega+15 < fd ≤ entrega+30`; +30 = `fd > entrega+30`.
4. **Tope de horizonte.** Con el slicer de `Horizonte` en una fecha (p. ej. 31/07), solo cuenta la disponibilidad **hasta esa fecha**. La 2ª quincena y +30 días caen mucho respecto al comportamiento antiguo, que mostraba la ventana completa de cada pedido **ignorando** la fecha seleccionada.

> **Cambio de comportamiento (antes → ahora):** la medida antigua (a) no aplicaba el corte de fecha del filtro y (b) contaba las líneas sin fecha. Por eso daba importes **más altos**. La actual refleja lo realmente servible dentro de la fecha seleccionada. Ejemplo (pedido 264006092, entrega 15/07, horizonte 31/07): 1ª 38.940€→38.090€, 2ª 19.947€→1.994€, +30 18.252€→0€.

---

## Notas y mejoras pendientes

- **Medidas de unidades sin tope.** Las de Importe (valor y %) ya respetan el horizonte; las de **unidades** (`Fecha Servicio 1º/2º/>30 Quincena`, `Valor Servicio 1º Quincena`) tienen la exclusión de blancos pero **no** el tope. Si se usa la vista "Unidades", conviene aplicarles el mismo `VAR Tope = MAX('Horizonte'[Fecha])`.
- **Fecha centinela `01/01/2050`** en `Fecha Fin pedido venta` (pedidos sin fecha real de envío): quedan como *blank* frente a `Horizonte` y no distorsionan el tope; sin selección no recortan nada.
- **Auto fecha/hora activada:** múltiples `LocalDateTable_*` ocultas; con `Calendario` y `Horizonte` propios podrían desactivarse (cambio estructural, revisar el informe).
- **`Import en Piciking` / `Imporet pte. picking` / `Uniades`** son erratas heredadas de los nombres de columna del origen; se mantienen para no romper referencias.

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
