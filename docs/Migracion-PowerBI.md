# Migración Bi2Go → Power BI: estado

← [Índice de documentación](README.md) · [README del repositorio](../README.md)

*Última revisión: 24 de noviembre de 2026. Sustituye a la foto de agosto (`Migracion-Bi2Go-PowerBI.docx`/`.pdf` en la raíz, que se conserva como histórico).*

## En una línea

La lógica de negocio ya está construida en los seis modelos: Compras, que en agosto no tenía ni una medida, tiene hoy 63. El objetivo de **servir todo desde el SQL de staging ha retrocedido**: desde agosto se han añadido cuatro lecturas nuevas directas a HANA (tres en Compras y una en Transport), y ninguna de las antiguas se ha retirado.

## Avance

| Dimensión | Agosto 2026 | Noviembre 2026 | Qué cambió |
|---|---|---|---|
| Lógica de negocio | 75 % (9 de 12 paneles) | **83 % (10 de 12)** | Compras pasa de 0 a 63 medidas. Era el hito 3 de agosto, que lo ligaba al panel *Productividad proveedor*. |
| Acabado visual | 50 % (6 de 12) | **50 % (6 de 12)** | Mucho trabajo de acabado (menú adaptable, tema de marca, píldoras de filtro, cabeceras fijas) pero ningún panel ha cambiado de estado. Los tres rankings siguen bloqueados por las imágenes. |
| Datos en el hub SQL | 86 % (25 de 29 orígenes) | **79 % (33 de 42 tablas)** | Se cuenta distinto: ahora es por tabla de origen, contada en los `.tmdl`. Hay más tablas en el hub, pero también 4 lecturas nuevas a HANA. |

## Dónde están los datos

Tablas de origen por modelo, contadas en los `.tmdl` (`Sql.Database` frente a `SapHana.Database`; no se cuentan tablas calculadas ni `LocalDateTable`):

| Modelo | Hub SQL | HANA | Tablas en HANA | Situación |
|---|---:|---:|---|---|
| SgiRetail · Retail y online | 6 | 0 | — | Centralizado |
| SalesOrder · B2B multimarca | 10 | 1 | `LOL_PLANMATERIAL` | Mixto: comparte con ServicePlan la misma consulta de cartera |
| Transport · Logística y SLA | 7 | 1 | `LOL_PBIPICKINGORIGEN` (**nueva**, sept.) | Mixto: la tabla se marca como provisional |
| Compras · Aprovisionamiento | 8 | 3 | `LOL_PBIPURCHASEHEADER`, `LOL_PBIPACKAGES`, `LOL_PBISTOCKTRANSFERREAL` (**nuevas**, oct.–nov.) | Mixto: en agosto estaba centralizado |
| Finanzas · Balance y PyG | 2 | 3 | `JournalEntryItem`, `CostCenter`, `CostCenter_Dim2` | Mixto: sin avance; lo añadido desde agosto depende **más** de `JournalEntryItem` |
| ServicePlan · Cartera B2B | 0 | 1 | `LOL_PLANMATERIAL` | Pendiente de migrar |
| **Total** | **33** | **9** | 8 consultas distintas | **79 % en el hub** |

Todas las lecturas de HANA son `Value.NativeQuery(SapHana.Database("192.168.0.231:30015"), "SELECT …")`, es decir, SQL nativo contra `SBO_LOLA_SUCURSAL`. Para pasarlas al hub hay que llevar esa misma consulta al ETL como tabla `LOL_PBI*` y cambiar la partición a `Sql.Database("192.168.0.232", "hana_etl_admin")`, sin tocar columnas ni medidas.

## Estado por panel

| Panel | Área | Modelo | Lógica | Visual | Cambio desde agosto |
|---|---|---|---|---|---|
| Pedidos de venta B2B | Multimarca | SalesOrder | Lista | Lista | + páginas de Incidencias y Detalle de pedido, catálogo HTML a dos niveles |
| Informe de ventas B2C | Retail | SgiRetail | Lista | Lista | Catálogos a dos niveles, secciones, cabeceras fijas |
| Balance | Finanzas | Finanzas | Lista | Lista | Menú adaptable, píldoras de filtro |
| Balance operativo | Finanzas | Finanzas | Lista | Lista | Ídem |
| Pérdidas y ganancias | Finanzas | Finanzas | Lista | Lista | Ídem |
| Análisis de servicio | Multimarca | ServicePlan / SalesOrder | Lista | Lista | Ventanas de quincena rehechas desde `Fecha Fin`; arreglo de las fechas en blanco |
| Ranking por temporada | Producto | SgiRetail | Lista | Sin imágenes | Pasan al *Catálogo HTML* a dos niveles; desde septiembre el recuadro de foto sale vacío en los artículos con URL (antes, silueta) |
| Ranking con agrupaciones | Producto | SgiRetail | Lista | Sin imágenes | Ídem |
| Ranking temporada vs anterior | Producto | SgiRetail | Lista | Sin imágenes | Ídem |
| Productividad proveedor | Multimarca | Compras | **Lista** (antes *Sin medidas*) | Básico → **en curso** | 63 medidas; páginas Previsión de entradas, Análisis de entradas (pedido → albarán → ST → traslado), Ficha de pedido y Previsión de salidas |
| Producto B2C & B2B | Producto | — | Sin empezar | Sin empezar | — |
| Pantallas corporativas HQ | Producto | — | Sin empezar | Sin empezar | — |

Fuera de la lista de 12 paneles de Bi2Go, **Transport** ha crecido de 34 a 147 medidas: tiempos de almacén, embudo operativo, picking con seguimiento, inbound y pedido → albarán.

## Próximos hitos

| # | Hito | Estado en noviembre | Desbloquea |
|---|---|---|---|
| 1 | Imágenes de producto en los rankings | **Bloqueado.** El visual HTML no carga imágenes externas; la única foto real está en la tabla nativa de *Catálogo HTML*. Opciones: incrustar la imagen (base64) desde el ETL, o rankings en visual nativo | +3 paneles |
| 2 | Sacar Plan de Servicio de HANA (`LOL_PLANMATERIAL`) | **Sin avance.** La misma consulta alimenta también SalesOrder: migrarla una vez libera los dos modelos | ServicePlan 100 % hub |
| 3 | Medidas en el modelo de Compras | ✅ **Hecho** (63 medidas, sept.–nov.) | +1 panel |
| 4 | Retirar las 3 tablas legado de Finanzas | **Sin avance**, y la dependencia ha crecido: 6 medidas de píldora y 23 visuales leen `JournalEntryItem` | Finanzas 100 % hub |
| 5 | Decidir alcance de Pantallas HQ y comparativa B2C & B2B | Sin decisión registrada | Decisión |
| 6 | **Nuevo:** llevar al ETL las 4 lecturas HANA añadidas desde agosto (3 de Compras: cabecera de pedido, packing y traslado real; 1 de Transport: origen del picking) | Pendiente | Vuelve al 86 %+ |

## Lo que Bi2Go no llegó a desarrollar

| Pendiente en Bi2Go | Área | Situación en Power BI | Plazo |
|---|---|---|---|
| Compras | Control | **Construido**: cadena pedido → entrada → solicitud de traslado → traslado, con packing y estado del operador logístico | Hecho (falta sacar 3 tablas de HANA) |
| Integración logística | Operaciones | Operativo: seguimiento de transportes y SLA, ahora también con tiempos de almacén e inbound | Hecho |
| Zalando y marketplaces | Operaciones | SgiRetail tiene página de Marketplace | Corto |
| Google Analytics | Operaciones | Sin origen: el tráfico actual es solo de tienda física | Requiere integración |

## Deuda que afecta a la migración

- **Fecha/hora automática activada en los 6 modelos.** Todos tienen ya `Calendario` propio (Compras incluido), así que las `LocalDateTable_*` sobran.
- **Sin RLS en ningún modelo.**
- **Puntos de revisión que han salido al documentar** (detalle en cada documento):
  - Compras: las etiquetas de los HTML cruzan "En tránsito" y "Recibidas".
  - Transport: los envíos "en agencia destino" cuentan como entregados pero no tienen tiempo de entrega.
  - SalesOrder: hay una copia anidada y desfasada del informe en `definition/pages/pages/`.
  - ServicePlan: el `Calendario` solo cubre 2025.
