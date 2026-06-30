# TransportsTracking — Documentación del modelo

Panel de **seguimiento de transportes / envíos** de Lola Casademunt. Sigue el ciclo de vida de cada envío (albarán) desde su lanzamiento hasta la entrega, con métricas de volumen, tiempos de entrega, incidencias y devoluciones por transportista, segmento y destino.

- **Proyecto:** [Transport/TransportsTracking](../Transport/)
- **Modelo:** `TransportsTracking.SemanticModel` · **Informe:** `TransportsTracking.Report`
- **Cultura:** es-ES · **Modo:** Import
- **Rama git:** `transport`

> Para consultar el modelo en vivo (DAX) con Power BI Desktop abierto, ver la nota al final.

---

## Origen de datos

| Tabla | Origen | Tipo |
|---|---|---|
| `LOL_TRANSPORT_TRACKING` | `Sql.Database("192.168.0.232", "hana_etl_admin")` → `dbo.LOL_TRANSPORT_TRACKING` | Hechos (import) |
| `Festivos` | Tabla M en línea (festivos nacionales) | Dimensión auxiliar |
| `Calendario` | Tabla calculada (`CALENDAR` sobre min/max fechas) | Dimensión fecha |
| `SLA Días` | `GENERATESERIES(1,7,1)` | Parámetro (slicer) |
| `_Medidas` | Tabla de solo medidas (`{BLANK()}`) | Medidas |

La partición importa la tabla **cruda** sin transformar; toda la lógica de negocio vive en **columnas calculadas DAX** dentro de `LOL_TRANSPORT_TRACKING`. El fichero [Transport/sql/vw_envios.sql](../Transport/sql/vw_envios.sql) replica parte de esta lógica en SQL pero **no está conectado** al modelo (es un artefacto separado).

> ⚠️ **Calidad del dato:** el origen debe entregar las fechas con **hora real** (no truncadas a `00:00`), porque las fases intradía y los huecos del detalle dependen de ello.

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
| **`estado_grupo`** | Agrupación de alto nivel: **Entregado / En curso / Incidencia / Devuelto / Sin datos** (Error de tracking + Lanzado + Sin estado). |
| `es_entregado` | 1 si `estado_norm = "Entregado"`. Incluye entregados mal etiquetados y los entregados que además tienen `fecha_devuelto`. |
| `es_devuelto` | 1 si `estado_norm = "Devuelto"` (mutuamente excluyente con entregado). |
| `es_incidencia_bloqueante` | 1 si `estado_norm = "Incidencia"` (incidencia y aún no entregado/devuelto). |
| `es_en_curso` | 1 si `estado_grupo = "En curso"`. |
| `es_error_tracking` | 1 si `estado_norm = "Error de tracking"`. |
| `es_operativo` | 1 si tiene seguimiento válido (excluye Error/Lanzado/Sin estado). **Denominador de las tasas.** |
| **`f_inicio`** | Punto de inicio del tiempo de entrega = **salida del almacén** = `fecha_en_transito` (fallback: `fecha_recogido` > `fecha_lanzamiento`). Truncado a fecha. |
| `dias_entrega_natural` | Días naturales de `f_inicio` a `fecha_entregado`, válido si entregado, coherente y < 60 días. **Suelo de 1** (una entrega nunca es 0 días). |
| `dias_entrega_laborables` | Igual pero contando solo días laborables (vía `Calendario`, excluye findes y festivos). **Suelo de 1.** Es el "tiempo total" mostrado. |
| `dur_lanz_transito` / `dur_transito_reparto` / `dur_reparto_entrega` | Duración (días con decimales) entre hitos consecutivos. |
| `Tramo Tiempo` / `Tramo Orden` | Bucket del tiempo de entrega (`<1d`, `1-2d`, …, `>10d`) y su orden. |
| `ciudad_destino_limpia` | Ciudad + país normalizados (parsea `ciudad_destino`, mapea código país a nombre). `dataCategory: Place`. |
| `sla_objetivo` | Días objetivo de SLA según transportista + país (TIPSA/UPS, zonas). |

---

## Medidas (32)

### 1 Volumen
| Medida | DAX | Propósito |
|---|---|---|
| `Envios Totales` | `COUNTROWS ( LOL_TRANSPORT_TRACKING )` | Volumen bruto de envíos. |
| `Envios Operativos` | `CALCULATE ( COUNTROWS (…), [es_operativo] = 1 )` | Envíos con seguimiento válido (excluye Error/Lanzado). Base de las tasas. |
| `En Reparto` | `CALCULATE ( COUNTROWS (…), [estado_norm] = "En reparto" )` | Envíos en reparto (estado canónico). |
| `En Transito` | `CALCULATE ( COUNTROWS (…), [estado_norm] = "En tránsito" )` | Envíos en tránsito (estado canónico). |
| `En Curso` | `CALCULATE ( COUNTROWS (…), [es_en_curso] = 1 )` | Activos en la red (tránsito+reparto+agencia+recogido+etiqueta). |
| `En Agencia Destino` | `CALCULATE ( COUNTROWS (…), [estado_norm] = "En agencia destino" )` | Envíos en agencia destino. |
| `Errores Tracking` | `CALCULATE ( COUNTROWS (…), [es_error_tracking] = 1 )` | Envíos cuyo seguimiento falló (estado "Error"). |
| `% Cobertura Seguimiento` | `DIVIDE ( [Envios Operativos], [Envios Totales] )` | KPI de calidad del dato. |

### 2 Entrega
| Medida | DAX | Propósito |
|---|---|---|
| `Entregados` | `CALCULATE ( COUNTROWS (…), [es_entregado] = 1 )` | Envíos entregados. |
| `% Entregados` | `DIVIDE ( [Entregados], [Envios Operativos] )` | % entregados sobre envíos operativos. |

### 3 Tiempo
| Medida | DAX | Propósito |
|---|---|---|
| `SLA Valor` | `SELECTEDVALUE ( 'SLA Días'[SLA Días], 3 )` | Valor del slicer de SLA (días), por defecto 3. |
| `Envios Con Tiempo Lab` | `CALCULATE ( COUNTROWS (…), NOT ISBLANK ( [dias_entrega_laborables] ) )` | Envíos con tiempo de entrega calculado. |
| `Tiempo Entrega Mediana (Lab)` | `MEDIANX ( FILTER (…), [dias_entrega_laborables] )` | Mediana de días laborables de entrega. |
| `Tiempo Entrega Media (Lab)` | `AVERAGEX ( FILTER (…), [dias_entrega_laborables] )` | Media de días laborables. |
| `Tiempo Entrega P90 (Lab)` | `PERCENTILEX.INC ( FILTER (…), [dias_entrega_laborables], 0.9 )` | Percentil 90 de días laborables. |
| `Tiempo Entrega Mediana (Nat)` | `MEDIANX ( FILTER (…), [dias_entrega_natural] )` | Mediana de días naturales. |
| `A Tiempo` | `CALCULATE ( COUNTROWS (…), [dias_entrega_laborables] <= [sla_objetivo] )` | Envíos entregados dentro del SLA. |
| `% A Tiempo` | `DIVIDE ( [A Tiempo], [Envios Con Tiempo Lab] )` | % de entregas dentro de SLA. |

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

### 6 Fases
| Medida | DAX | Propósito |
|---|---|---|
| `Fase Lanz-Transito` | `MEDIANX ( FILTER ( entregados, [dur_lanz_transito] >= 0 ), [dur_lanz_transito] )` | Mediana de días de lanzamiento a tránsito. |
| `Fase Transito-Reparto` | `MEDIANX ( … [dur_transito_reparto] … )` | Mediana de tránsito a reparto. |
| `Fase Reparto-Entrega` | `MEDIANX ( … [dur_reparto_entrega] … )` | Mediana de reparto a entrega. |

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

## Relaciones

- **Activa:** `LOL_TRANSPORT_TRACKING[fecha_lanzamiento]` → `Calendario[Date]` (eje temporal principal).
- **Inactiva:** `LOL_TRANSPORT_TRACKING[fecha_entregado]` → `Calendario[Date]` (disponible para `USERELATIONSHIP`).
- 13 relaciones auto-generadas a `LocalDateTable_*` (jerarquías de fecha automáticas, una por columna de fecha) — candidatas a limpieza (ver Notas).
- `Festivos[Fecha]` → su `LocalDateTable`.

---

## Reglas de negocio clave

1. **Estado canónico sobre las fechas, no el texto.** `estado` (texto del transportista) viene desincronizado (p.ej. "Recogido" ya entregado, incidencias ocultas en "en tránsito"). `estado_norm`/`estado_grupo` reconstruyen el estado real por hito más avanzado.
2. **Tasas sobre `Envios Operativos`.** `% Entregados`, `Tasa Incidencias` y `Tasa Devoluciones` excluyen del denominador los envíos sin seguimiento (Error + Lanzado), para no diluir las tasas.
3. **Entregado vence a Devuelto.** Un envío con `fecha_entregado` y `fecha_devuelto` cuenta como **Entregado** (la devolución es logística inversa posterior); `es_entregado` y `es_devuelto` son excluyentes.
4. **Tiempo de entrega = tránsito, con suelo de 1 día.** Se mide desde la salida del almacén (`fecha_en_transito`), no desde el lanzamiento. Mínimo 1 día laborable (una entrega nunca es 0 días).
   - ⚠️ **UPS:** registra el "en tránsito" el mismo día del reparto (≈73% same-day), así que su tiempo de tránsito real es ~0 y el suelo lo lleva a 1. Si se quisiera el tiempo *total* (etiqueta→entrega) para UPS, sería una métrica aparte.

---

## Notas y mejoras pendientes

- **Auto fecha/hora:** hay 13 `LocalDateTable_*` auto-generadas (una por columna de fecha) + time-intelligence activado. Con un `Calendario` propio ya en el modelo, se podría desactivar la auto fecha/hora y eliminarlas para aligerar el modelo (cambio estructural, revisar referencias del informe).
- **`vw_envios.sql`** replica parte de la lógica pero no está cableado; mantener sincronizado o eliminar para evitar confusión.
- ~39 envíos sin `fecha_lanzamiento` no mapean al `Calendario` (no afecta a los tiempos, que arrancan en tránsito).

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
