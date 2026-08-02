# PurchaseOrders (Compras) — Documentación del modelo

Panel de **compras / aprovisionamiento** de Lola Casademunt. Sigue las **líneas de pedido de compra** y su recorrido documental hasta la entrada de mercancía y la factura de proveedor, cruzado con el maestro de artículos y el maestro de tiendas/almacenes.

- **Proyecto:** [Compras/PurchaseOrders](../Compras/) · `PurchaseOrders.pbip`
- **Modelo:** `PurchaseOrders.SemanticModel` · **Informe:** `PurchaseOrders.Report`
- **compatibilityLevel:** 1606 · **Cultura:** es-ES · **Modo:** Import
- **RLS:** ninguno

> Modelo **joven**: la estructura (tablas, claves, relaciones) está montada, pero **todavía no tiene ni una sola medida DAX** — el informe trabaja con agregaciones implícitas. Ver §6.

---

## 1. Origen de datos

Todas las tablas se importan del mismo SQL de staging, sin transformación en M (la partición es un `Sql.Database` + navegación directa):

```m
let
    Origen = Sql.Database("192.168.0.232", "hana_etl_admin"),
    dbo_<TABLA> = Origen{[Schema="dbo",Item="<TABLA>"]}[Data]
in
    dbo_<TABLA>
```

| Tabla | Origen (`dbo.*`) | Papel | Cols |
|---|---|---|---:|
| `LOL_PBIPURCHASEORDERS` | `LOL_PBIPURCHASEORDERS` | **Hechos** — líneas de pedido de compra | 57 (55 origen + 2 calc.) |
| `LOL_PBIPURCHASEGOODRECEIPT` | `LOL_PBIPURCHASEGOODRECEIPT` | **Hechos** — líneas de entrada de mercancía | 58 (56 origen + 2 calc.) |
| `LOL_PBIPURCHASEINVOICE` | `LOL_PBIPURCHASEINVOICE` | **Hechos** — líneas de factura de proveedor | 56 (55 origen + 1 calc.) |
| `LOL_PBIMODELITEM` | `LOL_PBIMODELITEM` | Dimensión — maestro de artículos / modelo-color | 57 |
| `LOL_PBITIENDAS` | `LOL_PBITIENDAS` | Dimensión — maestro de tiendas / almacenes | 14 |

Además hay **1 plantilla + 27 tablas `LocalDateTable_*`** ocultas, generadas por la fecha/hora automática (`__PBI_TimeIntelligenceEnabled = 1`), una por cada columna de fecha de las tres tablas de hechos y de `LOL_PBITIENDAS`.

> **No hay tabla `Calendario` propia.** El modelo depende íntegramente de las jerarquías automáticas por columna de fecha. Es la carencia estructural más importante (ver §6).

---

## 2. Las tres tablas de documento

Las tres comparten prácticamente el **mismo esquema de columnas** (vienen de la misma vista de documentos de marketing de SAP B1): identificación del documento, del proveedor, del artículo, fechas y los importes de línea.

### Columnas comunes relevantes

| Grupo | Columnas |
|---|---|
| **Identificación del documento** | `DocumentInternalKey` (DocEntry), `DocumentLineInternalKey` (LineNum), `DocumentNumber`, `DocumentRowNumber`, `DocumentTypeCode`, `LineUID` |
| **Enlace al documento base** | `BaseDocumentInternalKey`, `BaseDocumentLineInternalKey`, `BaseDocumentTypeCode` |
| **Proveedor / gestión** | `CardCode`, `Owner`, `SalesEmployeeOrBuyerNumber`, `NumAtCard`, `ProjectCode` |
| **Artículo / almacén** | `ItemCode`, `WarehouseCode`, `UoMCode`, `ItemsPerUnit` |
| **Cantidades** | `Quantity`, `QuantityInInventoryUoM`, `OpenQuantity`, `OpenQuantityInInventoryUoM` |
| **Importes** | `UnitPrice`, `UnitPriceLC`, `GrossUnitPriceLC`, `LineTotalAmountLC`, `LineTotalAmountSC`, `TaxAmountLC`, `LineDiscountPercentage`, `LineExchangeRate`, `PriceCurrency` |
| **Estado** | `LineStatus` (`"O"` abierto / `"C"` cerrado), `TaxOnly`, `ValidacionOC` |
| **Fechas** | `DocumentDate`, `PostingDate`, `DueDate`, `LineDeliveryDate`, `FechaDelivery`, `FechaDeliveryActualizada`, `FechaEmbarque`, `FechaRecepcion`, `FechaComunicacion` |
| **Campos de usuario (UDF)** | `MktDoc_Row_GSP_SEASON`, `MktDoc_Titl_GSP_SEASON` (temporada), `MktDoc_Titl_GSP_PEDREG` (pedido regular), `MktDoc_Titl_GSP_REP` (reposición), `MktDoc_Titl_LOL_TIPLOG` (tipo logístico), `U_LOL_PORT`, `U_LOL_INSPECTION`, `U_LOL_TIPOIMPUTACION`, `U_GSP_LOLSENTPAPERLESS`, `EtiquetasEnviadas`, `ShippingType`, `ProductionBoMType` |

Diferencias entre las tres:

- `LOL_PBIPURCHASEGOODRECEIPT` añade **`CANCELED`** (marca de entrada anulada) — la única columna de negocio que no está en las otras dos.
- `LOL_PBIPURCHASEORDERS` es la única con la columna calculada **`Estado`**.

---

## 3. La cadena de documentos: Pedido → Entrada → Factura

Es la aportación principal del modelo. SAP encadena documentos por (`BaseEntry`, `BaseLine`), pero eso son **dos** columnas: no sirve para una relación de Power BI, que necesita una sola clave. La solución son **columnas calculadas de clave compuesta** con concatenación `&`:

```dax
-- En LOL_PBIPURCHASEORDERS (línea propia)
KLinea = LOL_PBIPURCHASEORDERS[DocumentInternalKey] & "-" & LOL_PBIPURCHASEORDERS[DocumentLineInternalKey]

-- En LOL_PBIPURCHASEGOODRECEIPT (línea propia + línea del documento base)
KLinea = LOL_PBIPURCHASEGOODRECEIPT[DocumentInternalKey] & "-" & LOL_PBIPURCHASEGOODRECEIPT[DocumentLineInternalKey]
KBase  = IF (
             NOT ISBLANK ( LOL_PBIPURCHASEGOODRECEIPT[BaseDocumentInternalKey] )
                 && LOL_PBIPURCHASEGOODRECEIPT[BaseDocumentInternalKey] <> "",
             LOL_PBIPURCHASEGOODRECEIPT[BaseDocumentInternalKey] & "-" & LOL_PBIPURCHASEGOODRECEIPT[BaseDocumentLineInternalKey]
         )

-- En LOL_PBIPURCHASEINVOICE (solo línea del documento base)
KBase  = IF (
             NOT ISBLANK ( LOL_PBIPURCHASEINVOICE[BaseDocumentInternalKey] )
                 && LOL_PBIPURCHASEINVOICE[BaseDocumentInternalKey] <> "",
             LOL_PBIPURCHASEINVOICE[BaseDocumentInternalKey] & "-" & LOL_PBIPURCHASEINVOICE[BaseDocumentLineInternalKey]
         )
```

Todas son `isHidden` y con `lineageTag` fijo (`11111111-000n-…`) puesto a mano en el TMDL.

El `IF (NOT ISBLANK … && <> "")` es deliberado: las líneas **sin documento base** (una factura directa sin entrada previa, una entrada sin pedido) devuelven `BLANK()` en lugar de la cadena basura `"-"`, y así no se agrupan todas juntas en un falso enlace.

### Relaciones de la cadena

| Desde (muchos) | Hacia (uno) | Significado |
|---|---|---|
| `LOL_PBIPURCHASEGOODRECEIPT[KBase]` | `LOL_PBIPURCHASEORDERS[KLinea]` | Cada línea de entrada apunta a la línea de pedido que la originó |
| `LOL_PBIPURCHASEINVOICE[KBase]` | `LOL_PBIPURCHASEGOODRECEIPT[KLinea]` | Cada línea de factura apunta a la línea de entrada que la originó |

Son relaciones **físicas activas, M:1, dirección simple**. Encadenadas dan el recorrido completo Pedido → Entrada → Factura: al filtrar un pedido, se propaga a sus entradas y de ahí a sus facturas.

> **Diferencia con SalesOrder.** En el modelo de ventas (B2BSalesOrder) la misma cadena se resolvió con **medidas `TREATAS`** porque las relaciones físicas generaban ambigüedad al compartir dimensiones. Aquí las relaciones físicas funcionan porque las tablas de entrada y factura **no** comparten relación con las dimensiones (`LOL_PBIMODELITEM`, `LOL_PBITIENDAS`): solo el pedido las tiene, así que no hay caminos múltiples.

### Relación inactiva heredada

| Desde | Hacia | Estado |
|---|---|---|
| `LOL_PBIPURCHASEINVOICE[LineUID]` | `LOL_PBIPURCHASEORDERS[LineUID]` | ⛔ **Inactiva** (`AutoDetected_1e15babb…`) |

Es un intento anterior de enlazar factura directamente con pedido por `LineUID`, saltándose la entrada. Quedó desactivado al montar la cadena por `KBase`/`KLinea`. Candidata a borrar.

---

## 4. Dimensiones

### `LOL_PBIMODELITEM` — artículos / modelo-color

Maestro de producto compartido con los modelos B2BSalesOrder y SgiRetail. Clave `ItemCode`. Contiene la jerarquía comercial (`Categoria`, `Codigo_familia_agregada`/`Descripcion_familia_agregada`, `Nombre_familia`, `Codigo_Subfamilia`/`Subfamilia`, `GrupodeArt` — usado como *marca* —, `Material`, `TIPO`), descripciones y sus variantes `_EN`, atributos de talla/color/tallaje, temporada (`Codigo_de_temporada`, `Nombre_temporada`), proveedor (`Codigo_de_Proveedor`, `Nombre_de_Proveedor`), datos de aduana (`Paisdeorigen`, `PartidaArancelaria`, `Composicion_modelo`), los PVP por país (`PVP_ESPANA_Y_PORTUGAL`, `PVP_EUROPA`, `PVP_GRECIA`, `PVP_POLONIA`, `PVP_SUECIA`, `PVP_SUIZA`), `URL_imagen_producto` y flags (`Activo_Retail`, `Activo_Inacatalog`, `Congelado`, `Sostenible`, `Control_de_calidad`).

- **Relación:** `LOL_PBIPURCHASEORDERS[ItemCode]` → `LOL_PBIMODELITEM[ItemCode]`, **bidireccional**.
- **Sin** la columna calculada `Modelo-Color` que sí tiene la copia de este maestro en B2BSalesOrder.

### `LOL_PBITIENDAS` — tiendas / almacenes

Clave `TiendaID`. Columnas: `NombreTienda`, `TipoTienda`, `Territorio`, `AreaManager`, `ResponsableTienda`, `IDCuentaPersona`, fechas (`FechaApertura`, `FechaCierre`, `FechaComunicacion`) y datos técnicos del TPV (`VersionBD`, `VersionTpv`, `UltimaSincronizacionServer`, `Int`).

- **Relación:** `LOL_PBIPURCHASEORDERS[WarehouseCode]` → `LOL_PBITIENDAS[TiendaID]`, **bidireccional**.

> Ambas relaciones de dimensión son **bidireccionales**. Con un solo hecho conectado a ellas no genera ambigüedad hoy, pero si en el futuro se relacionan entradas o facturas con las mismas dimensiones aparecerán caminos múltiples: en ese momento habrá que pasarlas a dirección simple.

---

## 5. Columna calculada de negocio

Solo hay una además de las claves de cadena:

```dax
Estado = IF ( LOL_PBIPURCHASEORDERS[LineStatus] = "O", "Abierto", "Cerrado" )
```

Traduce el `LineStatus` de SAP a etiqueta legible. Es el eje de casi todas las matrices del informe (columnas Abierto / Cerrado).

---

## 6. Medidas

**El modelo no tiene ninguna medida DAX.** Todos los números del informe salen de **agregaciones implícitas** sobre columnas (`Sum(Quantity)`, `Sum(OpenQuantity)`).

Consecuencias prácticas:

- Los importes (`LineTotalAmountLC`, `UnitPriceLC`…) **no se están usando**: el panel solo mide **unidades**.
- No hay forma de expresar los KPIs naturales del área (% recibido, % facturado, pendiente de recibir, lead time pedido→entrada) sin escribir medidas.
- Las agregaciones implícitas no pueden reutilizarse ni formatearse de forma centralizada.

**Recomendación:** crear una tabla-contenedor (p. ej. `MedidasCompras`, partición calculada `{BLANK()}`) y llevar allí, como mínimo:

| Medida propuesta | Definición |
|---|---|
| `Unidades Pedidas` | `SUM ( LOL_PBIPURCHASEORDERS[Quantity] )` |
| `Unidades Pendientes` | `SUM ( LOL_PBIPURCHASEORDERS[OpenQuantity] )` |
| `Importe Pedido` | `SUM ( LOL_PBIPURCHASEORDERS[LineTotalAmountLC] )` |
| `Unidades Recibidas` | `SUM ( LOL_PBIPURCHASEGOODRECEIPT[Quantity] )` (excluyendo `CANCELED`) |
| `Importe Recibido` | `SUM ( LOL_PBIPURCHASEGOODRECEIPT[LineTotalAmountLC] )` |
| `Importe Facturado` | `SUM ( LOL_PBIPURCHASEINVOICE[LineTotalAmountLC] )` |
| `% Recibido` / `% Facturado` | `DIVIDE ( [Importe Recibido], [Importe Pedido] )` … |
| `Lead Time Pedido→Entrada` | mediana de días entre la fecha del pedido y la de su entrada encadenada |

Con las relaciones de cadena ya montadas, estas medidas funcionan directamente sin `TREATAS`.

---

## 7. Informe

`PurchaseOrders.Report`, 3 páginas de 1280×720:

| Página | Contenido |
|---|---|
| **Página 1** (17 visuales) | Panel principal. Tres tarjetas de `Quantity`, cuatro matrices de unidades por `Estado` (por artículo × fecha de recepción, por tienda, por categoría, por documento×tienda) y una barra de segmentadores: temporada de línea (`MktDoc_Row_GSP_SEASON`), artículo, categoría, familia, tienda, fecha de recepción y buscador de nº de documento. |
| **Página 2** (2 visuales) | Tabla de detalle línea a línea: documento, artículo, descripción larga, tienda, cantidad y cantidad abierta. |
| **Página 3** (2 visuales) | **Matriz de la cadena** — el único visual que recorre las tres tablas de documento a la vez (pedido, entrada y factura) más artículo y tienda, con segmentador de temporada. Es la validación visual de que la cadena `KBase`/`KLinea` encadena bien. |

---

## 8. Notas y mejoras pendientes

1. **Faltan medidas (crítico).** Ver §6. Es lo que separa este modelo de ser un panel de compras completo.
2. **No hay `Calendario`.** Sin dimensión de fecha propia no se puede comparar periodos, hacer YTD ni acumular por temporada de forma consistente entre las tres tablas de documento. Además, las tres tablas se filtran hoy por fechas *independientes* (cada una con su jerarquía automática), así que no comparten eje temporal. Crear un `Calendario` y relacionarlo con la fecha de referencia de cada documento es el siguiente paso estructural.
3. **27 `LocalDateTable_*` + fecha/hora automática.** Coste de tamaño y refresco por nada: cada columna de fecha genera su tabla. Al crear el `Calendario` conviene desactivar la fecha/hora automática de una vez.
4. **Relación inactiva `LineUID`** entre factura y pedido: residuo del enfoque anterior, borrar.
5. **Relaciones de dimensión bidireccionales:** ver aviso en §4.
6. **Los tres esquemas de documento están duplicados columna a columna.** Si el ETL las mantiene alineadas es cómodo, pero cualquier cambio hay que replicarlo tres veces (y las claves calculadas también). Vigilar que `CANCELED` no acabe siendo necesario también en las otras dos.
7. **Entradas anuladas.** `LOL_PBIPURCHASEGOODRECEIPT[CANCELED]` existe pero **nada la filtra** todavía. Cuando se escriban las medidas de recepción hay que excluirla, o las unidades recibidas saldrán infladas.
8. **`cultures/es-ES.tmdl`** solo contiene metadatos lingüísticos de Q&A auto-generados; no aporta documentación funcional.

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
