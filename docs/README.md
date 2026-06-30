# Documentación — pbir-models

Documentación técnica de los modelos semánticos (Power BI / PBIP-TMDL) de Lola Casademunt en este repositorio.

## Estructura del repositorio

El repo está organizado por **modelo/área**, y dividido en **ramas git** (cambiar de rama muestra/oculta el modelo correspondiente — comportamiento esperado, no pérdida de datos):

| Rama | Modelos visibles |
|---|---|
| `main` | (todos, según estado) |
| `transport` | Transport, SalesOrder |
| `salesorder` | SalesOrder |
| `sgiretail` | SgiRetail |

## Modelos documentados

| Modelo | Carpeta | Medidas | Documentación |
|---|---|---:|---|
| **TransportsTracking** | [Transport/](../Transport/) | 32 | [Transport-TransportsTracking.md](Transport-TransportsTracking.md) |
| **B2BSalesOrder** | [SalesOrder/](../SalesOrder/) | 76 | [SalesOrder-B2BSalesOrder.md](SalesOrder-B2BSalesOrder.md) |
| **SgiRetail** | [SgiRetail/](../SgiRetail/) | 238 | `docs/SgiRetail-SgiRetail.md` — **en la rama `sgiretail`** (junto a su modelo) |

> **SgiRetail** se documenta en su propia rama (`sgiretail`), donde reside la definición del modelo y los enlaces resuelven. Para verla: `git checkout sgiretail` y abrir `docs/SgiRetail-SgiRetail.md`.

## Pendiente de documentar

- **Compras**, **Finanzas** — carpetas presentes pero sin definición de modelo en ninguna rama analizada.

## Convenciones

- Modelos en formato **TMDL** (carpeta `*.SemanticModel/definition/`).
- Las medidas viven en tablas dedicadas (`_Medidas`, `Medidas*`); la lógica de negocio suele estar en **columnas calculadas**.
- Cultura **es-ES**, modo **Import**.

---
*Documentación generada analizando los ficheros TMDL de cada modelo. Para regenerar/ampliar, revisar los `.tmdl` en `*.SemanticModel/definition/tables/`.*
