# Documentación técnica — pbir-models

Documentación de los modelos semánticos (Power BI / PBIP-TMDL) de Lola Casademunt.

← [README del repositorio](../README.md)

*Última revisión: 28 de agosto de 2026.*

## Los seis modelos

| Modelo | Carpeta | Hechos principales | Medidas | Documentación |
|---|---|---|---:|---|
| **Finanzas** | [Finanzas/](../Finanzas/) | `LOL_PBIFINANCIALENTRIES` (apuntes de diario) | 118 + 19 aux. | [Finanzas-Finanzas.md](Finanzas-Finanzas.md) |
| **PurchaseOrders (Compras)** | [Compras/](../Compras/) | Pedido, entrada de mercancía, factura de proveedor | 0 | [Compras-PurchaseOrders.md](Compras-PurchaseOrders.md) |
| **B2BSalesOrder** | [SalesOrder/](../SalesOrder/) | Pedido, albarán, factura, nota de crédito, devolución, **cartera viva** | 174 | [SalesOrder-B2BSalesOrder.md](SalesOrder-B2BSalesOrder.md) |
| **SgiRetail** | [SgiRetail/](../SgiRetail/) | `LOL_PBISGIRETAIL` (líneas de ticket), tráfico, objetivos | 238 | [SgiRetail-SgiRetail.md](SgiRetail-SgiRetail.md) |
| **TransportsTracking** | [Transport/](../Transport/) | `LOL_TRANSPORT_TRACKING` (un envío por fila) | 34 | [Transport-TransportsTracking.md](Transport-TransportsTracking.md) |
| **Plan de Servicio B2B** | [ServicePlan/](../ServicePlan/) | `LOL_PLANMATERIAL` (cartera abierta) | 23 | [ServicePlan-Plan de Servicio B2B.md](ServicePlan-Plan%20de%20Servicio%20B2B.md) |

## Orígenes de datos

| Origen | Uso |
|---|---|
| `Sql.Database("192.168.0.232", "hana_etl_admin")` → `dbo.*` | Origen principal de **todos** los modelos (tablas `LOL_PBI*` / `LOL_*` pobladas por ETL). |
| `SapHana.Database("192.168.0.231:30015")` | **ServicePlan** y **SalesOrder** (`LOL_PLANMATERIAL`, vía `Value.NativeQuery` — la misma consulta en los dos modelos) y **Finanzas** (`JournalEntryItem` — legado en retirada — y los dos `CostCenter`). |

La dirección general es sacar las lecturas de HANA y servirlas desde el SQL de staging; Finanzas está a medio camino (ver su documentación).

## Patrones comunes

**Cadenas de documentos.** Tres modelos siguen un documento hasta sus derivados. SAP encadena por la pareja (`BaseEntry`, `BaseLine`), que son dos columnas; Power BI necesita una. La solución en todos los casos es una **clave compuesta en columna calculada**, concatenando con `&` (no `COMBINEVALUES`):

```dax
KLinea = <tabla>[DocEntry] & "-" & <tabla>[LineNum]
KBase  = IF ( NOT ISBLANK ( <tabla>[BaseEntry] ) && <tabla>[BaseEntry] <> "",
              <tabla>[BaseEntry] & "-" & <tabla>[BaseLine] )
```

El `IF` evita que las líneas sin documento base acaben todas agrupadas bajo la cadena `"-"`.

A partir de ahí, los dos modelos divergen y **no por capricho**:

| | Compras | SalesOrder |
|---|---|---|
| Cómo se recorre | **Relaciones físicas** | **Medidas con `TREATAS`** |
| Por qué | Entradas y facturas no comparten dimensiones con el pedido: no hay caminos múltiples | Las cinco tablas de documento comparten cliente, agente, artículo, temporada y calendario: las relaciones daban ambigüedad |

**Tablas-contenedor de medidas.** Todos los modelos alojan las medidas en tablas sin datos (partición calculada `{BLANK()}` o `Row("Column", BLANK())`), agrupadas con `displayFolder`: `_Medidas`, `MedidasVentas`, `MedidasTemporadas`, `Medidas Finanzas`…

**Tablas desconectadas como selector.** Parámetros y *field parameters* (`ParamTopN`, `OrdenSelector`, `DimensionB2B`, `Dimensión Finanzas`, `TablaFechasFiltro`) sin relación física, leídos con `SELECTEDVALUE` y aplicados con `TREATAS`. Implica que el filtrado pasa **por medida**: un visual que no use esas medidas no queda filtrado.

**Segmentadores de fecha que se congelan.** Un segmentador de fecha guarda su estado en **dos sitios**, y solo uno filtra: `visual.objects.data[].properties.endDate` es el estado visual del control, pero el recorte efectivo vive en `visual.objects.general[].properties.filter.filter.Where[]`, como una `Comparison` con `ComparisonKind: 3` contra un literal `datetime'...'`. Borrar solo el `endDate` no descongela nada, y las dos fechas ni siquiera coinciden (*"Before 26/08"* se guarda como `endDate` = 26/08 pero el filtro dice `< 27/08`). Quitando `properties.filter` y dejando el `mode`, Power BI recalcula el tope desde el dato al abrir; sobre un `Calendario` cuya partición acabe en `MAX(TODAY(), …)` eso aterriza solo en el día en curso. Comprobar también los marcadores: uno con `suppressData: false` vuelve a aplicar la fecha vieja al pulsarlo.

**Medidas netas: filtrar los dos lados.** Cuando una medida resta dos tablas (facturas − notas de crédito, pedido − devoluciones), el `CALCULATE` que la acota tiene que acotar **las dos**. Si solo se filtra una, se resta el total histórico de la otra. En SalesOrder ha mordido dos veces; la segunda dejó el facturado de agosto de 2026 en −2.602.130,54 € en vez de 2.801.612,26 €.

**Tablas calculadas escritas a mano en TMDL.** El nombre de columna debe coincidir con el que produce el DAX (`SELECTCOLUMNS(..., "Nombre", [col])`), o al publicar el servicio da `Missing_References`.

## Deuda técnica compartida

| Problema | Alcance | Nota |
|---|---|---|
| **Fecha/hora automática activada** | Los 6 modelos | Decenas de `LocalDateTable_*` ocultas, una por columna de fecha. Todos salvo Compras tienen ya un `Calendario` propio, así que son redundantes: desactivarla aligera modelo y refresco. |
| **Sin `Calendario` propio** | Compras | El caso peor: sus tres tablas de documento se filtran por fechas independientes, sin eje temporal común. |
| **Sin ninguna medida** | Compras | El informe va con agregaciones implícitas y solo mide unidades; los importes están sin usar. |
| **Importes que llegan como texto** | SalesOrder | Se convierten con `VALUE`/`INT`/`Number.From` al calcular, y los fallos de parseo quedan en 0 o blank en silencio. |
| **Sin RLS** | Los 6 modelos | Quien accede al informe ve todos los datos. |
| **`cultures/es-ES.tmdl`** | Los 6 modelos | Solo metadatos lingüísticos de Q&A auto-generados; no aporta documentación funcional. |
| **Migración HANA → SQL a medias** | Finanzas | Las medidas ya leen la tabla nueva, pero cinco piezas siguen apuntando a `JournalEntryItem`, así que el refresco aún necesita HANA. |

---

### Anexo · Consultar un modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.

---
*Documentación generada analizando los ficheros TMDL de cada modelo. Para regenerar o ampliar, revisar los `.tmdl` en `*.SemanticModel/definition/tables/`.*
