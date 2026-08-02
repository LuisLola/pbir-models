# Documentación — pbir-models

Documentación técnica de los modelos semánticos (Power BI / PBIP-TMDL) de Lola Casademunt en este repositorio.

*Última revisión: 2 de agosto de 2026.*

## Estructura del repositorio

El repo está organizado **una rama por modelo** (cambiar de rama muestra/oculta las carpetas de modelo correspondientes — comportamiento esperado, no pérdida de datos). Las ramas más recientes arrastran además las carpetas de los modelos anteriores en el estado que tenían al bifurcarse, así que **la carpeta de un modelo no está al día en todas las ramas donde aparece**.

| Rama | Carpetas presentes | Modelo(s) que esta rama tiene **al día** |
|---|---|---|
| `main` | SalesOrder, SgiRetail | — (estado de 2026-06-09) |
| `serviceplan` | ServicePlan | ServicePlan |
| `sgiretail` | SgiRetail | SgiRetail |
| `transport` | Transport, SalesOrder | Transport |
| `salesorder` | SalesOrder, SgiRetail, Transport | — (SalesOrder quedó atrás, ver nota) |
| `compras` | Compras, SalesOrder, SgiRetail, Transport | Compras, SalesOrder |
| `finanzas` | Finanzas, Compras, SalesOrder, SgiRetail, Transport | Finanzas, SalesOrder |

> ⚠️ **La rama `salesorder` está por detrás.** El trabajo posterior sobre **B2BSalesOrder** (embudo por documento, devoluciones, página ejecutiva de cumplimiento) se commiteó sobre la línea `compras` → `finanzas`, no sobre `salesorder`. El estado canónico de SalesOrder está en `compras` y `finanzas` (mismo árbol); `salesorder` se quedó en 2026-07-16.

## Modelos documentados

**La carpeta `docs/` completa está replicada idéntica en las 7 ramas.** Da igual en cuál estés: siempre ves la documentación de los 6 modelos. Lo que sí cambia por rama son las **carpetas de modelo** — ahí es donde tienes que situarte para trabajar.

| Modelo | Carpeta | Medidas | Documentación | Rama donde el modelo está al día |
|---|---|---:|---|---|
| **Plan de Servicio B2B** | ServicePlan/ | 23 | [ServicePlan-Plan de Servicio B2B.md](ServicePlan-Plan%20de%20Servicio%20B2B.md) | `serviceplan` |
| **SgiRetail** | SgiRetail/ | 238 | [SgiRetail-SgiRetail.md](SgiRetail-SgiRetail.md) | `sgiretail` |
| **TransportsTracking** | Transport/ | 34 | [Transport-TransportsTracking.md](Transport-TransportsTracking.md) | `transport` |
| **B2BSalesOrder** | SalesOrder/ | 96 | [SalesOrder-B2BSalesOrder.md](SalesOrder-B2BSalesOrder.md) | `compras`, `finanzas` |
| **PurchaseOrders (Compras)** | Compras/ | 0 (agregaciones implícitas) | [Compras-PurchaseOrders.md](Compras-PurchaseOrders.md) | `compras`, `finanzas` |
| **Finanzas** | Finanzas/ | 118 + 19 auxiliares | [Finanzas-Finanzas.md](Finanzas-Finanzas.md) | `finanzas` |

> Cada `.md` describe el modelo tal y como está **en su rama al día**. Si lo lees desde otra rama, la carpeta del modelo que tienes en disco puede ser anterior a lo que cuenta el documento — y los enlaces relativos a ficheros `.tmdl` pueden no resolver. Para tocar el modelo, cambia a la rama de la última columna.

## Orígenes de datos

| Origen | Uso |
|---|---|
| `Sql.Database("192.168.0.232", "hana_etl_admin")` → `dbo.*` | Origen principal de **SgiRetail, B2BSalesOrder, TransportsTracking, Compras y Finanzas** (tablas `LOL_PBI*` / `LOL_*` pobladas por ETL). |
| `SapHana.Database("192.168.0.231:30015")` | **ServicePlan** (`LOL_PLANMATERIAL`, vía `Value.NativeQuery`) y **Finanzas** (`JournalEntryItem` — legado en retirada — y los dos `CostCenter`). |

La dirección general es sacar las lecturas de HANA y servirlas desde el SQL de staging; Finanzas está a medio camino (ver su documentación).

## Convenciones

- Modelos en formato **TMDL** (carpeta `*.SemanticModel/definition/`), `compatibilityLevel` 1600/1606, cultura **es-ES**, modo **Import**.
- La lógica de negocio suele vivir en **columnas calculadas** y en medidas alojadas en **tablas-contenedor** (`_Medidas`, `MedidasVentas`, `Medidas Finanzas`, …) con `displayFolder` para agruparlas.
- **Cadenas de documentos** (pedido → albarán/entrada → factura): se resuelven con **claves compuestas** `DocEntry & "-" & LineNum` en columnas calculadas. En Compras se usan **relaciones físicas**; en SalesOrder se usan **medidas con `TREATAS`** (las relaciones daban ambigüedad). Usar `&`, no `COMBINEVALUES`.
- Al crear **tablas calculadas** a mano en TMDL, el nombre de columna debe coincidir con el que produce el DAX (usar `SELECTCOLUMNS(..., "Nombre", [col])`), o al publicar el servicio da `Missing_References`.
- Todos los modelos tienen la **fecha/hora automática activada**, lo que genera decenas de tablas `LocalDateTable_*` ocultas. Con un `Calendario` propio en el modelo son redundantes; desactivarla es la mejora pendiente común a todos.
- **Ningún modelo tiene RLS** definido.

---

### Anexo · Consultar un modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.

---
*Documentación generada analizando los ficheros TMDL de cada modelo. Para regenerar/ampliar, revisar los `.tmdl` en `*.SemanticModel/definition/tables/`.*
