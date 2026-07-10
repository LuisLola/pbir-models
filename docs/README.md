# Documentación — pbir-models

Documentación técnica de los modelos semánticos (Power BI / PBIP-TMDL) de Lola Casademunt en este repositorio.

## Estructura del repositorio

El repo está organizado **una rama por carpeta/modelo** (cambiar de rama muestra/oculta el modelo correspondiente — comportamiento esperado, no pérdida de datos). Cada rama contiene su modelo + los archivos compartidos (`FechaIniLY`, `Tema_Lola_Montserrat.json`, `fix_tmdl_tail.py`).

| Rama | Carpeta / modelo visible |
|---|---|
| `main` | (todos, según estado) |
| `transport` | Transport, SalesOrder |
| `salesorder` | SalesOrder |
| `sgiretail` | SgiRetail |
| `serviceplan` | ServicePlan |

## Modelos documentados

Cada documentación técnica vive **en la rama de su modelo** (junto a su definición, para que los enlaces resuelvan). Para leerla: `git checkout <rama>` y abrir el `.md`.

| Modelo | Carpeta | Medidas | Documentación | Rama |
|---|---|---:|---|---|
| **Plan de Servicio B2B** | [ServicePlan/](../ServicePlan/) | ~23 | [ServicePlan-Plan de Servicio B2B.md](ServicePlan-Plan%20de%20Servicio%20B2B.md) | `serviceplan` |
| **TransportsTracking** | Transport/ | 32 | `docs/Transport-TransportsTracking.md` | `transport` |
| **B2BSalesOrder** | SalesOrder/ | 76 | `docs/SalesOrder-B2BSalesOrder.md` | `transport` |
| **SgiRetail** | SgiRetail/ | 238 | `docs/SgiRetail-SgiRetail.md` | `sgiretail` |

## Pendiente de documentar

- **Compras**, **Finanzas** — carpetas presentes pero **vacías** (sin definición de modelo en ninguna rama). Cuando tengan modelo, se les crea su rama y su doc.

## Convenciones

- Modelos en formato **TMDL** (carpeta `*.SemanticModel/definition/`).
- Cultura **es-ES**, modo **Import**.
- La lógica de negocio suele vivir en **columnas calculadas** y en las medidas de tablas dedicadas.
- Al crear **tablas calculadas** a mano en TMDL, el nombre de columna debe coincidir con el que produce el DAX (usar `SELECTCOLUMNS(..., "Nombre", [col])`), o al publicar el servicio da `Missing_References`.

---
*Documentación generada analizando los ficheros TMDL de cada modelo. Para regenerar/ampliar, revisar los `.tmdl` en `*.SemanticModel/definition/tables/`.*
