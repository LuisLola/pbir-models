# pbir-models — modelos semánticos de Power BI de Lola Casademunt

Los seis proyectos **PBIP/TMDL** de la casa: la definición de cada modelo semántico y de su informe, versionada como texto.

Todo está en `main`. Cada carpeta es un proyecto independiente que se abre con su `.pbip`.

| Proyecto | Qué mide | Medidas | Documentación |
|---|---|---:|---|
| [**Finanzas/**](Finanzas/) | Balance, cuenta de P&G y P&G de gestión, con comparativa interanual y desglose por canal/tienda | 150 | [📄 Finanzas](docs/Finanzas-Finanzas.md) |
| [**Compras/**](Compras/) | Pedidos de compra y su cadena hasta la entrada de mercancía, la solicitud de traslado, el traslado real, el packing y la factura de proveedor | 63 | [📄 PurchaseOrders](docs/Compras-PurchaseOrders.md) |
| [**SalesOrder/**](SalesOrder/) | Pedidos B2B: comparativa entre temporadas, embudo de cumplimiento (servido / facturado / devuelto) y cierre de mes sobre la cartera viva | 262 | [📄 B2BSalesOrder](docs/SalesOrder-B2BSalesOrder.md) |
| [**SgiRetail/**](SgiRetail/) | Retail: ventas de tienda física y online, tráfico, objetivos, conversión y catálogo de producto | 278 | [📄 SgiRetail](docs/SgiRetail-SgiRetail.md) |
| [**Transport/**](Transport/) | Seguimiento de envíos y almacén: estado, tiempos de entrega y de preparación, SLA, embudo de picking, inbound e incidencias | 147 | [📄 TransportsTracking](docs/Transport-TransportsTracking.md) |
| [**ServicePlan/**](ServicePlan/) | Plan de servicio B2B: cuánto de la cartera abierta se puede servir y cuándo | 51 | [📄 Plan de Servicio B2B](docs/ServicePlan-Plan%20de%20Servicio%20B2B.md) |

> 📈 **Estado de la migración Bi2Go → Power BI** (avance, orígenes de datos, paneles e hitos): [**docs/Migracion-PowerBI.md**](docs/Migracion-PowerBI.md).

Índice técnico transversal (orígenes, convenciones, patrones comunes): [**docs/README.md**](docs/README.md).

## Cómo trabajar

```bash
git clone https://github.com/LuisLola/pbir-models.git   # o git@github.com:… si tienes clave SSH en GitHub
cd pbir-models
```

Abre el `.pbip` del proyecto que toques (`Finanzas/Finanzas.pbip`, `Compras/PurchaseOrders.pbip`, …). Power BI Desktop trabaja con un proyecto cada vez; las demás carpetas no le molestan.

Al guardar desde Desktop se reserializan muchos ficheros: revisa el diff antes de commitear, porque a menudo la mitad son reordenaciones sin cambio real de contenido.

## Orígenes de datos

| Origen | Modelos |
|---|---|
| `Sql.Database("192.168.0.232", "hana_etl_admin")` → `dbo.*` | Todos salvo ServicePlan. Tablas `LOL_PBI*` / `LOL_*` pobladas por ETL. |
| `SapHana.Database("192.168.0.231:30015")` | **ServicePlan** y **SalesOrder** (`LOL_PLANMATERIAL`), **Finanzas** (`JournalEntryItem` y los dos `CostCenter`), **Compras** (cabecera de pedido, packing y traslado real) y **Transport** (origen del picking). |

La dirección es sacar las lecturas de HANA y servirlas desde el SQL de staging: hoy 33 de 42 tablas de origen (79 %). El detalle y los hitos, en [docs/Migracion-PowerBI.md](docs/Migracion-PowerBI.md).

## Convenciones

- Formato **TMDL** (`*.SemanticModel/definition/`), `compatibilityLevel` 1600/1606, cultura **es-ES**, modo **Import**.
- Las medidas viven en **tablas-contenedor** (`_Medidas`, `MedidasVentas`, `Medidas Finanzas`, …), organizadas con `displayFolder`. La lógica de negocio suele estar en **columnas calculadas**.
- **Cadenas de documentos** (pedido → albarán/entrada → factura): claves compuestas `DocEntry & "-" & LineNum` en columnas calculadas. Con `&`, no con `COMBINEVALUES`.
- Al crear **tablas calculadas** a mano en TMDL, el nombre de columna debe coincidir con el que produce el DAX (`SELECTCOLUMNS(..., "Nombre", [col])`), o al publicar da `Missing_References`.
- Todos los modelos tienen la **fecha/hora automática activada** y arrastran decenas de `LocalDateTable_*` ocultas. Todos tienen ya un `Calendario` propio, así que son redundantes: desactivarla es la mejora pendiente común a todos.
- **Ningún modelo tiene RLS** definido.

## Ficheros compartidos

| Fichero | Para qué |
|---|---|
| `Tema_Lola_45.json` | Tema de Power BI con la paleta de marca (rosa `#DFA0C9`, negro, blanco). |
| `Tema_Lola_Montserrat.json` | Tema anterior, solo tipografía. |
| `fix_tmdl_tail.py` | Repara el final de ficheros `.tmdl` cuando Desktop los deja truncados. |
| `FechaIniLY` | Fichero auxiliar (vacío). |
| `Migracion-Bi2Go-PowerBI.docx` / `.pdf` | Foto del estado de la migración de agosto de 2026 (histórico; el estado vivo está en `docs/Migracion-PowerBI.md`). |

## Historia: las ramas por modelo

Hasta agosto de 2026 el repo tenía **una rama por modelo** (`serviceplan`, `sgiretail`, `transport`, `salesorder`, `compras`, `finanzas`). No funcionó: cada rama acababa arrastrando copias desactualizadas de los otros modelos, y saber cuál era la buena requería consultar una tabla. Al consolidar, cada carpeta se tomó de la rama donde estaba al día:

| Carpeta | Venía de |
|---|---|
| `ServicePlan/` | `serviceplan` |
| `SgiRetail/` | `sgiretail` |
| `Transport/` | `transport` |
| `SalesOrder/`, `Compras/`, `Finanzas/` | `finanzas` |

Las ramas antiguas siguen existiendo como **archivo histórico** (tags `archivo/<rama>`). No trabajes en ellas ni las fusiones: su historia ya está representada aquí.
