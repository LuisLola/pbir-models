# pbir-models — modelos semánticos de Power BI de Lola Casademunt

Repositorio de los proyectos **PBIP/TMDL**: la definición de los modelos semánticos y de los informes, versionada como texto.

> 🧭 **Estás en `main`, que hace de índice.** El repo usa **una rama por modelo**: al cambiar de rama, las carpetas de los demás modelos desaparecen del disco. **Es esperado, no es pérdida de datos.** Desde aquí llegas a todo sin adivinar dónde está.

## Accesos directos

| Modelo | Documentación | Rama donde está al día | Carpeta del proyecto |
|---|---|---|---|
| **Plan de Servicio B2B** | [📄 Plan de Servicio B2B](https://github.com/LuisLola/pbir-models/blob/serviceplan/docs/ServicePlan-Plan%20de%20Servicio%20B2B.md) | [`serviceplan`](https://github.com/LuisLola/pbir-models/tree/serviceplan) | [📁 ServicePlan/](https://github.com/LuisLola/pbir-models/tree/serviceplan/ServicePlan) |
| **SgiRetail** | [📄 SgiRetail](https://github.com/LuisLola/pbir-models/blob/sgiretail/docs/SgiRetail-SgiRetail.md) | [`sgiretail`](https://github.com/LuisLola/pbir-models/tree/sgiretail) | [📁 SgiRetail/](https://github.com/LuisLola/pbir-models/tree/sgiretail/SgiRetail) |
| **TransportsTracking** | [📄 TransportsTracking](https://github.com/LuisLola/pbir-models/blob/transport/docs/Transport-TransportsTracking.md) | [`transport`](https://github.com/LuisLola/pbir-models/tree/transport) | [📁 Transport/](https://github.com/LuisLola/pbir-models/tree/transport/Transport) |
| **B2BSalesOrder** | [📄 B2BSalesOrder](https://github.com/LuisLola/pbir-models/blob/finanzas/docs/SalesOrder-B2BSalesOrder.md) | `finanzas` ⚠️ | `SalesOrder/` |
| **PurchaseOrders (Compras)** | [📄 PurchaseOrders (Compras)](https://github.com/LuisLola/pbir-models/blob/finanzas/docs/Compras-PurchaseOrders.md) | `finanzas` ⚠️ | `Compras/` |
| **Finanzas** | [📄 Finanzas](https://github.com/LuisLola/pbir-models/blob/finanzas/docs/Finanzas-Finanzas.md) | `finanzas` ⚠️ | `Finanzas/` |

> ⚠️ Las ramas **`compras` y `finanzas` todavía no están en GitHub** (solo en local). Sus enlaces no resolverán desde aquí hasta que se haga `git push -u origin compras finanzas`. Mientras tanto se leen en local con `git switch <rama>`.

La carpeta [`docs/`](docs/) está replicada idéntica en las 7 ramas, así que los `.md` se leen **sin cambiar de rama**. Índice técnico: [docs/README.md](docs/README.md).

## Mapa de ramas

| Rama | Carpetas que verás en disco | Modelo(s) al día aquí |
|---|---|---|
| [`main`](https://github.com/LuisLola/pbir-models/tree/main) | SalesOrder, SgiRetail | — *(solo copias antiguas arrastradas)* |
| [`serviceplan`](https://github.com/LuisLola/pbir-models/tree/serviceplan) | ServicePlan | **ServicePlan** |
| [`sgiretail`](https://github.com/LuisLola/pbir-models/tree/sgiretail) | SgiRetail | **SgiRetail** |
| [`transport`](https://github.com/LuisLola/pbir-models/tree/transport) | Transport, SalesOrder | **Transport** |
| [`salesorder`](https://github.com/LuisLola/pbir-models/tree/salesorder) | SalesOrder, SgiRetail, Transport | — *(solo copias antiguas arrastradas)* |
| `compras` *(sin push)* | Compras, SalesOrder, SgiRetail, Transport | **SalesOrder** |
| `finanzas` *(sin push)* | Finanzas, Compras, SalesOrder, SgiRetail, Transport | **Finanzas**, **Compras**, **SalesOrder** |

Dos trampas que conviene tener presentes:

- **`main` es un respaldo de junio de 2026, no la punta.** Ningún modelo está al día aquí; sirve de índice y de copia histórica.
- **`salesorder` se quedó atrás.** El trabajo de B2BSalesOrder posterior al 16-jul-2026 (embudo por documento, devoluciones, página de cumplimiento) se commiteó sobre la línea `compras` → `finanzas`.

Para comprobar en qué rama está la última versión de un modelo sin fiarte de esta tabla:

```bash
for b in main salesorder transport sgiretail serviceplan compras finanzas; do
  git log -1 --format="$b %ad %s" --date=short $b -- <Carpeta>
done
```

## Convenciones

- Formato **TMDL** (`*.SemanticModel/definition/`), `compatibilityLevel` 1600/1606, cultura **es-ES**, modo **Import**.
- Origen principal: `Sql.Database("192.168.0.232", "hana_etl_admin")`, esquema `dbo`. Quedan lecturas de **SAP HANA** en ServicePlan y Finanzas.
- Las medidas viven en **tablas-contenedor** (`_Medidas`, `MedidasVentas`, `Medidas Finanzas`, …) organizadas con `displayFolder`.
- **Cerrar el `.pbip` en Power BI Desktop antes de cambiar de rama.**
- Ningún modelo tiene **RLS** definido.

El detalle de cada punto, en el [índice técnico](docs/README.md).
