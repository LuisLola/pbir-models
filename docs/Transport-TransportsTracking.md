# TransportsTracking — Documentación del modelo

Última revisión: 24 de noviembre de 2026

Panel de **seguimiento de transportes / envíos** de Lola Casademunt. Sigue el ciclo de vida de cada envío (albarán) desde su lanzamiento hasta la entrega, con métricas de volumen, tiempos de entrega, incidencias y devoluciones por transportista, segmento y destino.

Desde septiembre de 2026 el modelo cubre también el **tramo de almacén** (picking: estados 20 → 40 → 60, expedición y enlace con el envío), con un **embudo operativo de picking** que cuadra en los dos sentidos, el **origen del picking** (pedido de cliente vs reposición a tienda), el **inbound** (transferencias de stock) y, en el modelo, la cadena **pedido B2B → albarán**.

- **Proyecto:** [Transport/TransportsTracking](../Transport/)
- **Modelo:** `TransportsTracking.SemanticModel` · **Informe:** `TransportsTracking.Report`
- **Cultura:** es-ES · **Modo:** Import
- **RLS:** ninguno

> Para consultar el modelo en vivo (DAX) con Power BI Desktop abierto, ver la nota al final.

---

## Origen de datos

| Tabla | Origen | Tipo |
|---|---|---|
| `LOL_TRANSPORT_TRACKING` | `Sql.Database("192.168.0.232", "hana_etl_admin")` → `dbo.LOL_TRANSPORT_TRACKING` | Hechos (import) |
| `LOL_PBITIMEWHS` | SQL ETL → `dbo.LOL_PBITIMEWHS` | Hechos de almacén: un registro por picking (fechas de estado 20/40/60, expedición, cliente) |
| `LOL_PBIPICKINGORIGEN` | **HANA directo**: `Value.NativeQuery(SapHana.Database("192.168.0.231:30015"), …)` sobre `SBO_LOLA_SUCURSAL."PKL1"` agrupado por `AbsEntry` | Dimensión: origen de cada picking (pedido de cliente / reposición / mixto) |
| `LOL_PBICLIENTES` | SQL ETL → `dbo.LOL_PBICLIENTES` | Dimensión clientes (`GroupName`, `RazonSocial`) |
| `LOL_PBISTOCKTRANSFERSTATUS` | SQL ETL → `dbo.LOL_PBISTOCKTRANSFERSTATUS` | Hechos de inbound (líneas de transferencia y su estado logístico) |
| `LOL_PBIMODELITEM` | SQL ETL → `dbo.LOL_PBIMODELITEM` | Dimensión artículo (`Season`, `Categoria`, `Descripcion_modelo`) |
| `LOL_PBIB2BSALESORDER` | SQL ETL → `dbo.LOL_PBIB2BSALESORDER` | Hechos: líneas de pedido B2B (incluye incidencias, `IsIncidence = "Y"`) |
| `LOL_PBISALESDELIVERY` | SQL ETL → `dbo.LOL_PBISALESDELIVERY` | Hechos: líneas de albarán de venta |
| `Festivos` | Tabla M en línea (festivos nacionales) | Dimensión auxiliar |
| `Calendario` | Tabla calculada (`CALENDAR` sobre min/max fechas de tracking, picking e inbound) | Dimensión fecha |
| `SLA Días` | `GENERATESERIES(1,7,1)` | Parámetro (slicer) |
| `DimensionesTransport` / `DimensionesTransport2` | Tablas calculadas (parámetros de campo, 10 opciones cada una) | Selectores de dimensión de los gráficos |
| `FaseDelPicking` | Tabla calculada (`UNION` de 12 condiciones sobre `LOL_PBITIMEWHS`) | Puente picking ↔ tarjeta del embudo (auditoría) |
| `UltimaActualizacion` | Tabla M: `DateTimeZone.UtcNow()` + desfase horario de España (verano/invierno) | Marca del último refresco (oculta) |
| `Tabla` | Tabla M en línea con una columna vacía (`Columna1`) | Residuo sin uso |
| `_Medidas` | Tabla de solo medidas (`{BLANK()}`) | Medidas |

Todas las tablas con origen externo leen del **SQL del ETL** (`192.168.0.232` / `hana_etl_admin`; las tablas nuevas, con navegación simple sin transformaciones) **salvo `LOL_PBIPICKINGORIGEN`, que es la única que lee directamente de HANA**. Es **provisional**: la vista `LOL_PBITIMEWHS` trae mezclados los pickings de pedido de cliente y los de solicitud de traslado sin la columna que los separa; lo definitivo es añadir `PKL1."BaseObject"` a la vista del ETL y retirar la consulta nativa. Clasificación: `BaseObject = 17` → pedido de cliente, `1250000001` → solicitud de traslado (reposición), más de uno → mixto (en la práctica ningún picking mezcla los dos).

La partición de `LOL_TRANSPORT_TRACKING` aplica **una única transformación M** — normalizar a mayúsculas iniciales `cliente` y `ciudad_destino` (`Text.Proper`), para que los nombres no lleguen en mayúsculas sostenidas del origen:

```m
let
    Origen = Sql.Database("192.168.0.232", "hana_etl_admin"),
    dbo_LOL_TRANSPORT_TRACKING = Origen{[Schema="dbo",Item="LOL_TRANSPORT_TRACKING"]}[Data],
    #"Poner En Mayúsculas Cada Palabra" = Table.TransformColumns(dbo_LOL_TRANSPORT_TRACKING,
        {{"cliente", Text.Proper, type text}, {"ciudad_destino", Text.Proper, type text}})
in
    #"Poner En Mayúsculas Cada Palabra"
```

Toda la lógica de negocio vive en **columnas calculadas DAX** dentro de `LOL_TRANSPORT_TRACKING`. El fichero [Transport/sql/vw_envios.sql](../Transport/sql/vw_envios.sql) replica parte de esta lógica en SQL pero **no está conectado** al modelo (es un artefacto separado).

> ⚠️ **Calidad del dato:** el origen debe entregar las fechas con **hora real** (no truncadas a `00:00`), porque las fases intradía y los huecos del detalle dependen de ello.
>
> ⚠️ **Ojo con `Text.Proper` y `ciudad_destino`.** La columna calculada `sla_objetivo` parsea `ciudad_destino` buscando el literal `" - AL "` para extraer el código postal. `Text.Proper` deja ese fragmento intacto, pero cualquier cambio en el formato del origen (o en la transformación) rompe la detección del código postal balear en silencio.

---

## Tabla de hechos: `LOL_TRANSPORT_TRACKING`

Un registro por envío (albarán). Columnas de origen relevantes:

- **Identificación:** `albaran`, `tracking`, `picking`, `codigo_cliente`, `cliente`, `segmento` (B2C/B2B), `transportista`.
- **Destino:** `ciudad_destino`, `pais_destino`.
- **Estado de origen:** `estado` (texto reportado por el transportista — **poco fiable / desincronizado**, ver columnas calculadas).
- **Hitos (fechas):** `fecha_lanzamiento`, `fecha_etiqueta`, `fecha_recogido`, `fecha_en_transito`, `fecha_agencia_destino`, `fecha_pendiente_llegada`, `fecha_en_reparto`, `fecha_incidencia`, `fecha_falta_definitiva`, `fecha_devuelto`, `fecha_entregado`, `fecha_entrega_prevista`, `fecha_actualizacion`.
- **Otros:** `api_error` (casi siempre vacío).

### Columnas calculadas (la lógica de negocio)

| Columna | Qué hace |
|---|---|
| `transportista_norm` | Normaliza `TIPSA*` → `TIPSA`; resto igual. |
| **`estado_norm`** | **Estado canónico** derivado por prioridad del hito MÁS avanzado según las fechas (no el texto `estado`, que viene obsoleto). Prioridad: Entregado > Devuelto > Incidencia > En reparto > En agencia destino > En tránsito > Recogido > Etiqueta creada > Error de tracking > Lanzado > Sin estado. |
| **`estado_grupo`** | Agrupación de alto nivel: **Entregado / En curso / Incidencia / Devuelto / Sin datos** (Error de tracking + Lanzado + Sin estado). **"En agencia destino" cuenta como Entregado** (antes era En curso); En curso = tránsito + reparto + recogido + etiqueta. |
| `es_entregado` | 1 si `estado_grupo = "Entregado"` (antes `estado_norm`). Incluye entregados mal etiquetados, los entregados que además tienen `fecha_devuelto` y los que acaban **En agencia destino**. |
| `es_devuelto` | 1 si `estado_norm = "Devuelto"` (mutuamente excluyente con entregado). |
| `es_incidencia_bloqueante` | 1 si `estado_norm = "Incidencia"` (incidencia y aún no entregado/devuelto). |
| `es_en_curso` | 1 si `estado_grupo = "En curso"`. |
| `es_error_tracking` | 1 si `estado_norm = "Error de tracking"`. |
| `es_operativo` | 1 si tiene seguimiento válido (excluye Error/Lanzado/Sin estado). **Denominador de las tasas.** |
| **`f_inicio`** | Punto de inicio del tiempo de entrega. **Depende del transportista** (ver abajo). Truncado a fecha. |
| `dias_entrega_natural` | Días naturales de `f_inicio` a `fecha_entregado`, válido si entregado, coherente y < 60 días. **Suelo de 1** (una entrega nunca es 0 días). |
| `dias_entrega_laborables` | Igual pero contando solo días laborables (vía `Calendario`, excluye findes y festivos), **hasta el día anterior a la entrega** (`DATESBETWEEN(…, s, eD - 1)`). **Suelo de 1.** |
| **`dias_entrega_laborables2`** | Variante que **incluye el día de la entrega** (`DATESBETWEEN(…, s, eD)`). Mismos filtros de validez y mismo suelo de 1. Coexiste con la anterior para poder comparar ambos criterios. |
| `dur_lanz_transito` / `dur_transito_reparto` / `dur_reparto_entrega` | Duración (días con decimales) entre hitos consecutivos. |
| `Tramo Tiempo` / `Tramo Orden` | Bucket del tiempo de entrega (`<1d`, `1-2d`, …, `>10d`) y su orden. |
| `ciudad_destino_limpia` | Ciudad + país normalizados (parsea `ciudad_destino`, mapea código país a nombre). `dataCategory: Place`. |
| `sla_objetivo` | Días objetivo de SLA según transportista + país (ver tabla abajo). |
| `LogoTransportista` | Chip del transportista como imagen en base64 (`dataCategory: ImageUrl`) para tablas/matrices nativas. No es el logo oficial. |
| **`fecha_entrega_real`** | `COALESCE ( fecha_entregado, fecha_agencia_destino si estado_grupo = "Entregado" )`. Fecha de entrega para los que acaban en agencia destino (no traen `fecha_entregado`). Se mira el grupo porque "En reparto" también puede tener `fecha_agencia_destino`. |
| `Días 20 a 40` | Días **naturales** con decimales entre `FechaLog20` y `FechaLog40` del picking (`RELATED ( LOL_PBITIMEWHS[…] )`, `DATEDIFF` en minutos / 1440). |
| `Días 40 a recogida` | De `FechaLog40` a `f_inicio`: lo que espera la mercancía hasta que la recoge el transportista. |
| `Días recogida a entrega` | De `f_inicio` a `fecha_entrega_real`, sin truncar a entero. |
| `Días 20 a entrega` | De `FechaLog20` a `fecha_entrega_real`. Es la suma exacta de los tres anteriores. |

> Las columnas de tiempo de entrega (`dias_entrega_*`, `Tramo Tiempo`) siguen usando **`fecha_entregado`**, no `fecha_entrega_real`: los entregados en agencia destino cuentan en `Entregados` pero no tienen tiempo de entrega ni entran en `% A Tiempo`.

#### `f_inicio` — el arranque del cronómetro depende del transportista

```dax
f_inicio =
VAR _start =
    IF ( LOL_TRANSPORT_TRACKING[transportista_norm] = "UPS",
         COALESCE ( [fecha_recogido],   [fecha_en_transito], [fecha_lanzamiento] ),   -- UPS
         COALESCE ( [fecha_en_transito], [fecha_recogido],    [fecha_lanzamiento] ) ) -- resto
RETURN IF ( ISBLANK ( _start ), BLANK (), DATE ( YEAR ( _start ), MONTH ( _start ), DAY ( _start ) ) )
```

Para el resto de transportistas el cronómetro arranca en `fecha_en_transito` (salida del almacén). **Para UPS no sirve**: UPS registra el "en tránsito" el mismo día del reparto (≈73% same-day), así que su tránsito medido saldría siempre ~0. Por eso UPS arranca en `fecha_recogido`, que sí refleja la salida real.

#### `sla_objetivo` — días objetivo por transportista y zona

| Transportista | Destino | SLA |
|---|---|---:|
| TIPSA | ES peninsular | **1** |
| TIPSA | ES Baleares (CP `07*`, detectado parseando `ciudad_destino`) | 3 |
| TIPSA | PT | 3 |
| TIPSA | AD | 4 |
| UPS | ES | 1 |
| UPS | PT, FR, AD, MC, BE, NL, LU, DE, CH, AT, IT | 2 |
| UPS | GB, IE, DK, PL, CZ, SK, HU, SI, HR | 3 |
| UPS | SE, FI, NO, EE, LV, LT, RO, BG, GR, RS, BA, ME, MK, AL, XK | 4 |
| UPS | MD, UA, TR, CY, MT, IS | 5 |
| — | Resto | 3 (por defecto) |

---

## Tabla de almacén: `LOL_PBITIMEWHS`

Un registro por picking (~330.000; los mismos que SAP). Columnas de origen: `Picking` (clave), `PickDate`, `FechaComunicacion`, `FechaLog20` / `FechaLog40` / `FechaLog60` (estados del almacén: 20 informado, 40 preparado, 60 cerrado), `Expedicion_Picking`, `DiasHabiles_20_40` (texto), `SLA_Objetivo`, `CantidadEnviada`, `CantidadLiberada`, `Cliente`, `DocumentoOrigen_Picking`, `FechaContabilizacion_Origen`, `Comentarios`, `Estado`.

| Columna calculada | Qué hace |
|---|---|
| `Horas Pick a Log20` / `Horas Log20 a Log40` / `Horas Log40 a Log60` | Horas entre hitos del almacén. |
| `Dias habiles 20-40` | Versión numérica de `DiasHabiles_20_40` (llega como texto). **Hábiles**: sale más baja que los días naturales de `Días 20 a 40`. |
| `Expedido` | "Sí" si tiene `Expedicion_Picking`, "No" si no. |
| `Motivo sin expedir` | En blanco si salió; si no, "Cerrado manualmente" (por el comentario), "Anulado" (estado `R`) o "Sin expedición". |
| `Seguimiento` | "Con seguimiento" si el picking aparece en `LOL_TRANSPORT_TRACKING`; "Sin seguimiento" si no. |
| `Motivo sin seguimiento` | En blanco si lo hay; si no: "Anterior al seguimiento" (`PickDate` anterior al primer lanzamiento de tracking, 04/09/2025), "Sin expedir", "Reposición a tienda" (vía `LOL_PBIPICKINGORIGEN`) o "Transportista sin seguimiento". |

### `FaseDelPicking` — puente para auditar las tarjetas

Tabla calculada con una fila por **(picking, tarjeta del embudo)**: 12 fases (Picking del periodo, Informados a almacén — 20, Preparados — 40, Cerrados en almacén — 60, Pendientes en almacén, Sin expedir, Salen por transporte, Salen sin transporte, Entregados, En reparto, Sin seguimiento, Incidencias y devueltos) con su `Orden`. Las tarjetas no son excluyentes (un picking está a la vez en varias), por eso es una tabla puente y no una columna. Relación **bidireccional** con `LOL_PBITIMEWHS[Picking]`: al elegir una fase, el detalle muestra exactamente las filas detrás del número. El estado de transporte se resuelve en una pasada concatenando los `estado_grupo` distintos de cada picking entre barras (`|Entregado|En curso|`).

### Inbound y pedido/entrega

| Tabla | Columnas calculadas |
|---|---|
| `LOL_PBISTOCKTRANSFERSTATUS` | `FechaEstado` (parsea `FechaEstadologistico`, texto ISO), `Días en estado` (`NETWORKDAYS` hasta hoy con `Festivos`), `Días embarque a disponible` (ciclo real en laborables; el resto de fechas son previsiones), `Desvío vs previsto` (días naturales de `FechaDeliveryActualizada` a `FechaDisponibilidad`). Estados logísticos: 20 tránsito, 30 RMT, 50 disponible. |
| `LOL_PBIB2BSALESORDER` | `KLinea` (`DocEntry-LineNum`, con sufijo `-INC-ItemCode` en incidencias, que comparten DocEntry/LineNum), `Picking` (de `PickIdNo`; en blanco si no tiene o es incidencia), `Cantidad pedida` / `Cantidad pendiente` (de `Qty` / `OpenQty`, que llegan como texto). |
| `LOL_PBISALESDELIVERY` | `KPedido` (`BaseEntry-BaseLine` si `BaseType = "17"`; en blanco en entregas sin pedido). |

---

## Medidas (147)

Reparto por carpeta: 0 Contexto (3), 0 Cabecera (2), 0 Pildoras (10), 1 Volumen (8), 1 Embudo operativo (28), 2 Entrega (2), 3 Tiempo (11), 3 Tiempo\20 a entrega (14), 4 Almacén (14), 4 Almacén\Embudo (8), 4 Incidencias (4), 5 Cliente (1), 5 Inbound (10), 6 Fases (3), 6 Pedido y entrega (10), 9 Detalle de envío (9), 9 HTML (6), 9 Origen (4).

### 0 Contexto, 0 Cabecera y 0 Pildoras
| Medida | Propósito |
|---|---|
| `Rango de fechas` | Texto "dd/MM/yyyy - dd/MM/yyyy" con min/max de `Calendario[Date]` (útil con segmentador relativo). |
| `Última actualización` | "Actualizado dd/MM/yyyy a las HH:mm" desde `UltimaActualizacion[Fecha]` (hora de España). |
| `Rango y actualización` | Las dos anteriores en una sola tarjeta de cabecera. |
| `Rango de picking` / `Rango y actualización (picking)` | Igual, pero con min/max de `LOL_PBITIMEWHS[PickDate]`: en las páginas de almacén manda la fecha de picking, no `Calendario`. |
| `Chip País` / `Chip Segmento` / `Chip Transportista` / `Chip Estado` / `Chip Expedido` | Píldora de filtro activo: valor seleccionado (o "n sel.") + " ✕"; BLANK si no hay filtro. |
| `Chip … Color` (5) | `#DFA0C9` (rosa) si el filtro está puesto, `#6F263D` (burdeos) si no. |

### 1 Volumen
| Medida | DAX | Propósito |
|---|---|---|
| `Envios Totales` | `COUNTROWS ( LOL_TRANSPORT_TRACKING )` | Volumen bruto de envíos. |
| `Envios Operativos` | `CALCULATE ( COUNTROWS (…), [es_operativo] = 1 )` | Envíos con seguimiento válido (excluye Error/Lanzado). Base de las tasas. |
| `En Reparto` | `CALCULATE ( COUNTROWS (…), [estado_norm] = "En reparto" )` | Envíos en reparto (estado canónico). |
| `En Transito` | `CALCULATE ( COUNTROWS (…), [estado_norm] = "En tránsito" )` | Envíos en tránsito (estado canónico). |
| `En Curso` | `CALCULATE ( COUNTROWS (…), [es_en_curso] = 1 )` | Activos en la red (tránsito+reparto+recogido+etiqueta). Ya **no** incluye agencia destino, aunque el comentario de la medida en el `.tmdl` aún lo diga. |
| `En Agencia Destino` | `CALCULATE ( COUNTROWS (…), [estado_norm] = "En agencia destino" )` | Envíos en agencia destino. |
| `Errores Tracking` | `CALCULATE ( COUNTROWS (…), [es_error_tracking] = 1 )` | Envíos cuyo seguimiento falló (estado "Error"). |
| `% Cobertura Seguimiento` | `DIVIDE ( [Envios Operativos], [Envios Totales] )` | KPI de calidad del dato. |

> `En Agencia Destino` cuenta `estado_norm = "En agencia destino"`; esos envíos están **también** dentro de `Entregados` (su grupo es Entregado).

### 1 Embudo operativo (picking)
Embudo del almacén al cliente **contado en pickings** (`DISTINCTCOUNTNOBLANK ( LOL_PBITIMEWHS[Picking] )`). Las medidas de almacén cortan la relación con tracking (`CROSSFILTER ( LOL_TRANSPORT_TRACKING[picking], LOL_PBITIMEWHS[Picking], NONE )`) para que el filtro de transportista no recorte el universo; las de transporte usan `ALLEXCEPT ( LOL_TRANSPORT_TRACKING, LOL_PBITIMEWHS )` + filtro por `estado_grupo`. Varias llevan `+ 0` para pintar 0 en vez de vacío.

| Medida | Propósito |
|---|---|
| `Picking del periodo` | Todo el picking de la ventana, tenga transporte o no. Total del que cuelga el resto. |
| `Informados a almacén` / `Preparados` / `Cerrados en almacén` | Pickings con `FechaLog20` / `FechaLog40` / `FechaLog60`. |
| `Pendientes en almacén` | `[Picking del periodo] - [Cerrados en almacén]`: carga viva del almacén. |
| `Salen por transporte` | Pickings con envío en tracking. |
| `Reposición a tienda` | `[Picking del periodo]` con `KEEPFILTERS ( LOL_PBIPICKINGORIGEN[TipoOrigen] = "Reposicion" )`. Va con logística propia; tiene tarjeta propia. |
| `Salen sin transporte` | `[Picking del periodo] - [Salen por transporte] - [Reposición a tienda]`: pedido de cliente por logística ajena (El Corte Inglés, marketplaces). |
| `% a transporte` | `[Salen por transporte]` sobre `[Picking del periodo] - [Reposición a tienda]`. |
| `Entregados por el transportista` / `Picking en reparto` / `Sin seguimiento del transportista` / `Con incidencia` / `Devueltos al origen` | Pickings por `estado_grupo` = Entregado / En curso / Sin datos / Incidencia / Devuelto. "Sin seguimiento del transportista" es casi todo SEUR. |
| `Incidencias y devueltos` | `[Con incidencia] + [Devueltos al origen]` (una sola tarjeta). |
| `Cuadre salida` | `Picking del periodo − Salen por transporte − Salen sin transporte`. Debe ser **0**. |
| `Cuadre seguimiento` | `Salen por transporte − Entregados − En reparto − Sin seguimiento − Incidencias y devueltos`. Debe ser **0**; si no, hay un estado nuevo en tracking. |
| `Picking expedido` / `Picking sin expedir` / `% sin expedir` | Por la columna `Expedido`. |
| `Picking cerrado manualmente` / `Picking anulado` | Por `Motivo sin expedir`. |
| `Picking con seguimiento` / `Picking sin seguimiento` / `% con seguimiento` | Por la columna `Seguimiento`. Mirar `Motivo sin seguimiento` antes de alarmarse: casi todo el "sin" es anterior al arranque del seguimiento. |
| `Envios con picking` / `Envios sin picking` | Envíos de tracking con / sin `picking` informado (los sin picking nunca cuadrarán con almacén; mayoritariamente TIPSA de enero a junio de 2026). |
| `Envios por picking` | `[Envios con picking] / [Picking con seguimiento]` (≈1,17): puente entre la pestaña de transporte (envíos) y la de almacén (pickings). |

### 2 Entrega
| Medida | DAX | Propósito |
|---|---|---|
| `Entregados` | `CALCULATE ( COUNTROWS (…), [es_entregado] = 1 )` | Envíos entregados (incluye los que acaban en agencia destino). |
| `% Entregados` | `DIVIDE ( [Entregados], [Envios Operativos] )` | % entregados sobre envíos operativos. |

### 3 Tiempo
| Medida | DAX | Propósito |
|---|---|---|
| `SLA Valor` | `SELECTEDVALUE ( 'SLA Días'[SLA Días], 3 )` | Valor del slicer de SLA (días), por defecto 3. |
| `Envios Con Tiempo Lab` | `CALCULATE ( COUNTROWS (…), NOT ISBLANK ( [dias_entrega_laborables] ) )` | Envíos con tiempo de entrega calculado. |
| `Tiempo Entrega Mediana (Lab)` | `MEDIANX ( FILTER (…), [dias_entrega_laborables] )` | Mediana de días laborables **sin** contar el día de entrega. |
| `Tiempo Entrega Media (Lab)` | `AVERAGEX ( FILTER (…), [dias_entrega_laborables] )` | Media, mismo criterio. |
| **`Tiempo Entrega Mediana (Lab)2`** | `MEDIANX ( FILTER (…), [dias_entrega_laborables2] )` | Mediana **incluyendo** el día de entrega. |
| **`Tiempo Entrega Media (Lab)2`** | `AVERAGEX ( FILTER (…), [dias_entrega_laborables2] )` | Media, mismo criterio. |
| `Tiempo Entrega P90 (Lab)` | `PERCENTILEX.INC ( FILTER (…), [dias_entrega_laborables], 0.9 )` | Percentil 90 de días laborables. |
| `Tiempo Entrega Mediana (Nat)` | `MEDIANX ( FILTER (…), [dias_entrega_natural] )` | Mediana de días naturales. |
| `A Tiempo` | `CALCULATE ( COUNTROWS (…), [dias_entrega_laborables] <= [sla_objetivo] )` | Envíos entregados dentro del SLA. |
| `% A Tiempo` | `DIVIDE ( [A Tiempo], [Envios Con Tiempo Lab] )` | % de entregas dentro de SLA. |
| `Fecha entrega al transporte` | `MINX` de la más temprana entre `fecha_recogido` y `fecha_en_transito` | Momento en que el transportista se lleva el picking (TIPSA casi nunca informa recogido, por eso vale tránsito). |

> **Las dos familias de tiempo conviven a propósito.** Sin sufijo = "días de tránsito completos"; con sufijo `2` = "días transcurridos hasta la entrega inclusive", que es como suele contarlo el cliente. `A Tiempo` / `% A Tiempo` usan la versión **sin** sufijo.

### 3 Tiempo\20 a entrega
Ciclo **puerta a puerta**: desde que el picking entra en estado 20 hasta `fecha_entrega_real`. Todas filtran la misma base: envíos con `fecha_entrega_real` y `FechaLog20` y entrega ≥ 20 (diferencias con hora, no truncadas).

| Medida | Propósito |
|---|---|
| `Envíos con ciclo 20 a entrega` | Tamaño de la base de toda la carpeta. |
| `Envíos con entrega anterior al 20` | Imposibles (entregado antes de entrar en almacén); fuera del ciclo, contados para que se vean. |
| `Días de 20 a entrega (mediana)` / `(media)` / `(P90)` | Días naturales con decimales (`DATEDIFF` en horas / 24). La media sube por la cola larga. |
| `Horas de 20 a entrega (mediana)` | Lo mismo en horas. |
| `Días laborables de 20 a entrega (mediana)` | Solo laborables (algo optimista: `Festivos` tiene pocos días cargados). |
| `Horas de 20 a 40 (mediana)` / `Horas de 40 a entrega (mediana)` | Las dos mitades (almacén / salida a entrega). Las medianas de las mitades **no** suman la del total. |
| `Tramo 20 a 40 (días)` / `Tramo 40 a 60 (días)` / `Tramo 60 a recogida (días)` / `Tramo recogida a entrega (días)` | Medias de cada tramo. `60 a recogida` sale **negativo** y es correcto: el transportista suele recoger antes de que se selle el 60 (cierre administrativo). |
| `Suma de tramos (control)` | Suma de los cuatro tramos; debe coincidir con `Días de 20 a entrega (media)`. Si no cuadra, falta una fecha en alguna pata. |

### 4 Almacén
Pickings contados **por su fecha de picking**: `USERELATIONSHIP ( LOL_PBITIMEWHS[PickDate], Calendario[Date] )` + `CROSSFILTER … NONE` sobre `fecha_lanzamiento` y sobre la relación tracking ↔ almacén.

| Medida | Propósito |
|---|---|
| `Pickings` | Pickings del periodo. |
| `Pickings en 20` / `en 40` / `en 60` | Los que han alcanzado cada estado. |
| `Pickings a transporte` / `Pickings entregados` / `Pickings con envío` | Los que están en manos del transportista / acaban entregados / existen en tracking. |
| `% llega a 40` / `% sale a transporte` / `% entregado` | Sobre `[Pickings]`. |
| `% entregado sobre expedido` | `[Pickings entregados] / [Pickings con envío]`. |
| `Pickings pendientes de almacén` | `[Pickings] - [Pickings en 20]`. |
| `Pickings finalizados sin envío` | En 60 y sin envío (logística ajena o reposición). |
| `Mediana días 20-40` | Mediana de `Dias habiles 20-40`, sin que la recorte el filtro de transportista. |

### 4 Almacén\Embudo
Versión anterior del embudo de pickings (sin fecha de picking forzada). `Pickings lanzados` (todo el picking de la ventana), `Pickings sin envío`, `Pickings fuera de filtro` (tienen envío pero los quita el filtro de transportista/estado), `Pickings en curso`, `Pickings con incidencia`, `Pickings devueltos`, `Pickings sin datos de transporte` y `Cuadre del embudo` (debe dar **0**). Ninguna está en el informe; el embudo vivo es **1 Embudo operativo**.

### 4 Incidencias
| Medida | DAX | Propósito |
|---|---|---|
| `Incidencias Bloqueantes` | `CALCULATE ( COUNTROWS (…), [es_incidencia_bloqueante] = 1 )` | Incidencias que bloquean la entrega. |
| `Tasa Incidencias` | `DIVIDE ( [Incidencias Bloqueantes], [Envios Operativos] )` | % de incidencias sobre operativos. |
| `Devueltos` | `CALCULATE ( COUNTROWS (…), [es_devuelto] = 1 )` | Envíos devueltos (no entregados). |
| `Tasa Devoluciones` | `DIVIDE ( [Devueltos], [Envios Operativos] )` | % de devoluciones sobre operativos. |

### 5 Cliente
| Medida | DAX | Propósito |
|---|---|---|
| `Clientes Unicos` | `DISTINCTCOUNT ( [codigo_cliente] )` | Clientes distintos. |

### 5 Inbound (`LOL_PBISTOCKTRANSFERSTATUS`)
| Medida | Propósito |
|---|---|
| `Líneas inbound` / `SKU inbound` | Líneas de transferencia / referencias distintas en el contexto. |
| `Líneas en tránsito` / `Líneas en RMT` / `Líneas disponibles` | Por `Estadologistico` = "20" / "30" / "50". |
| `Días en estado (mediana)` / `(máximo)` | Cuánto llevan paradas las líneas en su estado actual (laborables). |
| `Ciclo inbound (mediana)` | Mediana de `Días embarque a disponible`. |
| `Desvío medio vs previsto` | Media de `Desvío vs previsto` (+ retraso, − adelanto). |
| `Líneas paradas +30 días` | Líneas no disponibles (≠ 50) con más de 30 días laborables en el mismo estado. |

### 6 Fases
| Medida | DAX | Propósito |
|---|---|---|
| `Fase Lanz-Transito` | `MEDIANX ( FILTER ( entregados, [dur_lanz_transito] >= 0 ), [dur_lanz_transito] )` | Mediana de días de lanzamiento a tránsito. |
| `Fase Transito-Reparto` | `MEDIANX ( … [dur_transito_reparto] … )` | Mediana de tránsito a reparto. |
| `Fase Reparto-Entrega` | `MEDIANX ( … [dur_reparto_entrega] … )` | Mediana de reparto a entrega. |

### 6 Pedido y entrega (`LOL_PBIB2BSALESORDER` → `LOL_PBISALESDELIVERY`)
| Medida | Propósito |
|---|---|
| `Unidades pedidas` / `Líneas de pedido` | Suma de `Cantidad pedida` / recuento de líneas, **sin incidencias** (`IsIncidence = "N"`). |
| `Unidades entregadas` / `Líneas de albarán` | `SUM ( LOL_PBISALESDELIVERY[Cantidad] )` (en el albarán `Cantidad` ya es lo entregado; `CantidadEntregada` viene a cero) / recuento de líneas. |
| `Unidades por servir` / `% servido` | Pedido − entregado / entregado sobre pedido. |
| `Pedidos` / `Albaranes` / `Referencias pedidas` | `DISTINCTCOUNT` de `DocNum` / `DocNum` / `ItemCode`. |
| `Unidades en incidencia` | `Cantidad pedida` con `IsIncidence = "Y"`. Las incidencias no llevan picking: solo se ven sin filtro de picking. |

Ninguna de esta carpeta se usa todavía en el informe.

### 9 Detalle de envío
Medidas para la tabla de detalle por picking de las páginas de almacén. Son **medidas y no columnas** a propósito: agrupar por la columna cruda de tracking expulsa de la tabla los pickings sin seguimiento (reposiciones, entre otros), porque el lado "muchos" de la relación no tiene miembro en blanco.

| Medida | Propósito |
|---|---|
| `Transportista del envío` | `CONCATENATEX` de `transportista_norm` (une con " / " si hubiera dos). |
| `Entrega al cliente` | `MAX ( fecha_entregado )`. |
| `Días envío 20 a 40` / `40 a recogida` / `recogida a entrega` / `20 a entrega` | `AVERAGE` de las columnas `Días …` de tracking. |
| `Días de entrega naturales` / `SLA de entrega` | `AVERAGE` de `dias_entrega_natural` / `sla_objetivo`. |
| `Días de preparación` | `AVERAGE ( LOL_PBITIMEWHS[Dias habiles 20-40] )`. Es además el **ancla** de la tabla: nunca sale en blanco, así que mantiene la fila de las reposiciones (que tienen en blanco todo lo de seguimiento). |

### 9 Origen (`LOL_PBIPICKINGORIGEN`)
| Medida | Propósito |
|---|---|
| `Envíos` | `COUNTROWS ( LOL_TRANSPORT_TRACKING ) + 0`. |
| `Envíos de reposición` / `Envíos de pedido` | `[Envíos]` con `TipoOrigen` = "Reposicion" / "Pedido de cliente". |
| `% reposición` | Peso de la reposición sobre el total de envíos. |

### 9 HTML (visuales "HTML Content")
Medidas que devuelven HTML inline (tarjetas/tablas/embudo) para el visual *HTML Content*. No se inyectan imágenes externas (la lógica usa SVG/CSS inline).

| Medida | Propósito | En uso |
|---|---|---|
| `HTML Fila KPIs` | Fila de 6 tarjetas KPI (envíos, % entregados, tiempo, % a tiempo, incidencias, devoluciones). | — |
| `HTML Tabla Transportistas` | Tabla top-8 transportistas (volumen, mediana, % a tiempo, incidencias). | — |
| `HTML Comparativa Segmento` | Comparativa B2C vs B2B (barra + tabla). | — |
| `HTML Embudo Fases` | Embudo de hitos con recuento y medianas entre fases. | — |
| `HTML Ranking Geografico` | Top países y ciudades por volumen. | — |
| **`HTML Detalle Envio`** | **Ficha de detalle del envío** (cabecera con cliente/transportista/segmento/destino/tracking/estado/SLA/entrega prevista + línea de tiempo con todos los hitos y alerta de incidencia). | ✅ Página-tooltip (400×720) |

> La **página principal** del informe usa **visuales nativos** (donut, tarjetas, tablas, mapa Azure, gráficos), no las medidas HTML. La única medida HTML viva es **`HTML Detalle Envio`** (tooltip). El donut "Estado actual" usa `estado_grupo`.

---

## Páginas del informe

| Página | Tamaño | Visible | Contenido |
|---|---|---|---|
| Seguimiento Transportes | 1280×2000 | Sí | Página principal de transporte: tarjetas KPI, donuts, mapas Azure, gráficos con selectores `DimensionesTransport`/`DimensionesTransport2`, matrices, píldoras de filtro y segmentador de origen (`TipoOrigen`). |
| Página 1 | 400×720 | Oculta | Página-tooltip con `HTML Detalle Envio`. |
| Tiempos en almacén. | 1280×1000 | Oculta | Versión anterior de almacén (medidas de **4 Almacén**, detalle por picking). |
| Inbound | 1280×1000 | Oculta | Inbound: tarjetas de **5 Inbound**, matriz por almacén/estado, detalle por SKU (`LOL_PBIMODELITEM`). |
| Tiempos en almacén | 1600×900 | Sí | Embudo operativo de picking (**1 Embudo operativo**), ciclo 20 → entrega y tabla de detalle por picking (**9 Detalle de envío**). |

---

## Relaciones

- **Activa:** `LOL_TRANSPORT_TRACKING[fecha_lanzamiento]` → `Calendario[Date]` (eje temporal principal).
- **Inactiva:** `LOL_TRANSPORT_TRACKING[fecha_entregado]` → `Calendario[Date]` (disponible para `USERELATIONSHIP`).
- **Inactivas a `Calendario[Date]`:** `LOL_PBITIMEWHS[PickDate]` (la activan las medidas de **4 Almacén**) y `LOL_PBISTOCKTRANSFERSTATUS[FechaEmbarque]`. **Activa:** `LOL_PBISTOCKTRANSFERSTATUS[FechaDisponibilidad]` → `Calendario[Date]`.
- **Tracking ↔ almacén:** `LOL_TRANSPORT_TRACKING[picking]` (*) → `LOL_PBITIMEWHS[Picking]` (1), **bidireccional**. Muchas medidas de picking la anulan con `CROSSFILTER … NONE`.
- `FaseDelPicking[Picking]` (*) → `LOL_PBITIMEWHS[Picking]`, **bidireccional**.
- `LOL_PBITIMEWHS[Picking]` (*) → `LOL_PBIPICKINGORIGEN[Picking]` (1).
- `LOL_PBITIMEWHS[Cliente]` → `LOL_PBICLIENTES[CodeCliente]` (AutoDetected).
- `LOL_PBIB2BSALESORDER[Picking]` → `LOL_PBITIMEWHS[Picking]`; `LOL_PBISALESDELIVERY[KPedido]` → `LOL_PBIB2BSALESORDER[KLinea]`.
- `LOL_PBISTOCKTRANSFERSTATUS[SKU]` → `LOL_PBIMODELITEM[ItemCode]`; `LOL_PBIB2BSALESORDER[ItemCode]` → `LOL_PBIMODELITEM[ItemCode]` (AutoDetected).
- 31 relaciones auto-generadas a `LocalDateTable_*` (jerarquías de fecha automáticas, una por columna de fecha) — candidatas a limpieza (ver Notas). Total: 44 relaciones.

---

## Reglas de negocio clave

1. **Estado canónico sobre las fechas, no el texto.** `estado` (texto del transportista) viene desincronizado (p.ej. "Recogido" ya entregado, incidencias ocultas en "en tránsito"). `estado_norm`/`estado_grupo` reconstruyen el estado real por hito más avanzado.
2. **Tasas sobre `Envios Operativos`.** `% Entregados`, `Tasa Incidencias` y `Tasa Devoluciones` excluyen del denominador los envíos sin seguimiento (Error + Lanzado), para no diluir las tasas.
3. **Entregado vence a Devuelto.** Un envío con `fecha_entregado` y `fecha_devuelto` cuenta como **Entregado** (la devolución es logística inversa posterior); `es_entregado` y `es_devuelto` son excluyentes. **"En agencia destino" también cuenta como Entregado** (grupo), con `fecha_agencia_destino` como fecha de entrega real.
4. **El cronómetro arranca en la salida del almacén, con suelo de 1 día.** Para la mayoría de transportistas eso es `fecha_en_transito`; **para UPS es `fecha_recogido`**, porque su "en tránsito" llega el mismo día del reparto. Mínimo 1 día laborable (una entrega nunca es 0 días).
5. **Dos criterios de "días de entrega".** `dias_entrega_laborables` (hasta el día anterior a la entrega) y `dias_entrega_laborables2` (incluyendo el día de la entrega). Al comparar cifras entre visuales hay que saber cuál está debajo; el cumplimiento de SLA usa el primero.
6. **Transporte cuenta envíos; almacén cuenta pickings.** Las dos pestañas no pueden dar la misma cifra: un picking genera de media ~1,17 envíos y hay envíos sin picking informado. El puente es `Envios por picking`.
7. **El embudo de picking tiene que cuadrar.** `Cuadre salida` y `Cuadre seguimiento` (y `Cuadre del embudo` en la versión antigua) deben dar **0**. Si no, ha aparecido una vía de salida o un estado de tracking que el modelo no contempla.
8. **La reposición a tienda va aparte.** Los pickings de solicitud de traslado (`LOL_PBIPICKINGORIGEN[TipoOrigen] = "Reposicion"`) nunca salen por transportista: se descuentan de `Salen sin transporte` y de la base de `% a transporte`, y tienen tarjeta propia.

---

## Notas y mejoras pendientes

- **Dos métricas de tiempo en paralelo.** `dias_entrega_laborables` y `…2` (y sus cuatro medidas) conviven desde 2026-07-14. Conviene decidir cuál es la oficial y retirar la otra, o renombrarlas para que el nombre diga el criterio (`…(sin día entrega)` / `…(con día entrega)`); el sufijo `2` no se explica solo.
- **Auto fecha/hora:** hay 31 `LocalDateTable_*` auto-generadas (una por columna de fecha; eran 14 antes de añadir almacén, inbound y pedidos) + time-intelligence activado. Con un `Calendario` propio ya en el modelo, se podría desactivar la auto fecha/hora y eliminarlas para aligerar el modelo (cambio estructural, revisar referencias del informe).
- **`vw_envios.sql`** replica parte de la lógica pero no está cableado; mantener sincronizado o eliminar para evitar confusión.
- ~39 envíos sin `fecha_lanzamiento` no mapean al `Calendario` (no afecta a los tiempos, que arrancan en tránsito/recogida).
- **`sla_objetivo` cambió para TIPSA peninsular** (de 2 a 1 día) el 2026-07-14. Cualquier comparación de `% A Tiempo` con capturas anteriores a esa fecha no es homogénea.
- **"En agencia destino" pasó de En curso a Entregado** (septiembre de 2026). `Entregados`, `% Entregados` y `En Curso` no son comparables con capturas anteriores. Los comentarios `///` de `es_en_curso` y de la medida `En Curso` aún dicen que incluyen agencia: están desfasados.
- **Tiempos de entrega sin agencia destino.** `dias_entrega_*` usan `fecha_entregado`; los entregados en agencia destino no tienen tiempo ni entran en `% A Tiempo`. Valorar pasarlos a `fecha_entrega_real`.
- **`LOL_PBIPICKINGORIGEN` lee de HANA en directo** (provisional, ver Origen de datos). Requiere credenciales HANA en el refresco además de las del SQL del ETL; retirarla cuando la vista `LOL_PBITIMEWHS` traiga `BaseObject`.
- **Medidas sin uso en el informe:** toda la carpeta **6 Pedido y entrega**, **4 Almacén\Embudo** (sustituida por **1 Embudo operativo**), **9 Origen** y las de control (`Cuadre …`, `Suma de tramos (control)`). `FaseDelPicking` tampoco está todavía en ningún visual.
- **Residuos:** la tabla `Tabla` (una columna vacía, sin uso) y las relaciones AutoDetected (`Cliente` → `CodeCliente`, `ItemCode` → `ItemCode`) conviene revisarlas o eliminarlas. `DimensionesTransport` y `DimensionesTransport2` son idénticas (una por selector).
- **`Festivos`** tiene pocos días cargados: las medidas en laborables (`Días laborables de 20 a entrega`, `Días en estado`) salen algo optimistas.

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
