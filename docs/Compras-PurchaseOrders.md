# PurchaseOrders (Compras) — Documentación del modelo

Última revisión: 24 de noviembre de 2026

Panel de **compras / aprovisionamiento** de Lola Casademunt. Sigue los **pedidos de compra** y su recorrido documental y físico: pedido → entrada de mercancía (albarán) → solicitud de traslado (ST) → traslado real al almacén, más el **packing** (bultos) que el proveedor carga en SAP y el **estado logístico** que declara el operador (RMT). Se cruza con el maestro de artículos, el de tiendas/almacenes y, para la página de salidas, con los pedidos de venta B2B.

- **Proyecto:** [Compras/PurchaseOrders](../Compras/) · `PurchaseOrders.pbip`
- **Modelo:** `PurchaseOrders.SemanticModel` · **Informe:** `PurchaseOrders.Report`
- **compatibilityLevel:** 1606 · **Cultura:** es-ES · **Modo:** Import
- **RLS:** ninguno
- **Medidas:** 63, todas en la tabla-contenedor `_Medidas` (ver §7)

> Historia corta: el modelo nació sin una sola medida y con solo la cadena Pedido → Entrada → Factura. Desde entonces (commits `d37ed37`, `1da2362`, `d1a2f93`, `b363202`) ha ganado `_Medidas`, `Calendario`, la cabecera de pedido, el packing, el traslado real y la página **Análisis de entradas**.

---

## 1. Orígenes de datos

El modelo lee de **dos** orígenes. Lo normal es el SQL de staging; las tres tablas logísticas más nuevas van **directas a HANA** con una consulta nativa, porque las vistas que necesitan (packing, ST, traslado) no están en el ETL.

| Origen | Conector | Tablas |
|---|---|---|
| SQL de staging `192.168.0.232` · BD `hana_etl_admin` | `Sql.Database` + navegación a `dbo.<TABLA>` | `LOL_PBIPURCHASEORDERS`, `LOL_PBIPURCHASEGOODRECEIPT`, `LOL_PBIPURCHASEINVOICE`, `LOL_PBISTOCKTRANSFERSTATUS`, `LOL_PBIMODELITEM`, `LOL_PBITIENDAS`, `LOL_PBICLIENTES`, `LOL_PBIB2BSALESORDER` |
| SAP HANA `192.168.0.231:30015` · esquema `SBO_LOLA_SUCURSAL` | `Value.NativeQuery ( SapHana.Database ( …, [Implementation="2.0"] ), "SELECT …" )` | `LOL_PBIPURCHASEHEADER` (`OPOR`), `LOL_PBIPACKAGES` (`@GSP_MWPACKAGES` + `@GSP_MWPACK_LIN` + `@GSP_MWPACK_DOCS` + `PDN1`/`OPOR`), `LOL_PBISTOCKTRANSFERREAL` (`OWTQ`/`WTQ1` + `WTR1`/`OWTR`) |
| Sin origen externo | DAX / M interno | `Calendario` (calculada), `_Medidas` (`Row("Column", BLANK())`), `DimensionesCompras` y `DimensionesCompras2` (parámetros de campo), `UltimaActualizacion` (M: `DateTimeZone.UtcNow()` pasado a hora de España con el cambio de horario) |

Patrón de las particiones SQL:

```m
let
    Origen = Sql.Database("192.168.0.232", "hana_etl_admin"),
    dbo_<TABLA> = Origen{[Schema="dbo",Item="<TABLA>"]}[Data],
    // Solo en ORDERS, STOCKTRANSFERSTATUS y B2BSALESORDER:
    ColumnasNumericas = { … },
    Presentes = List.Intersect({ColumnasNumericas, Table.ColumnNames(dbo_<TABLA>)}),
    TiposNumericos = Table.TransformColumnTypes(dbo_<TABLA>, List.Transform(Presentes, each {_, type number}), "en-US")
in
    TiposNumericos
```

El paso `ColumnasNumericas` fuerza a número, con cultura `en-US`, las columnas de importe/cantidad que el ETL entrega como texto; el `List.Intersect` evita que la actualización falle si el ETL quita alguna. `LOL_PBIB2BSALESORDER` lleva además `CommandTimeout` de 1 h.

---

## 2. Tablas

| Tabla | Origen | Papel | Cols (calc.) |
|---|---|---|---:|
| `LOL_PBIPURCHASEORDERS` | SQL | **Hechos** — líneas de pedido de compra | 65 (2) |
| `LOL_PBIPURCHASEGOODRECEIPT` | SQL | **Hechos** — líneas de entrada de mercancía (albarán) | 58 (2) |
| `LOL_PBIPURCHASEINVOICE` | SQL | **Hechos** — líneas de factura de proveedor | 56 (1) |
| `LOL_PBIPURCHASEHEADER` | HANA | **Cabecera de pedido** (una fila por pedido, `OPOR`) con el estado del pedido en columnas calculadas | 30 (18) |
| `LOL_PBIPACKAGES` | HANA | **Packing** — una fila por línea de bulto, flujo de compras, sin bultos anulados | 34 (5) |
| `LOL_PBISTOCKTRANSFERREAL` | HANA | **ST y traslado real** — una fila por línea de ST que viene de una entrada, con lo trasladado de verdad (`WTR1`) | 16 (1) |
| `LOL_PBISTOCKTRANSFERSTATUS` | SQL | Estado logístico de las líneas de ST según el operador (RMT) | 52 (6) |
| `LOL_PBIB2BSALESORDER` | SQL | Hechos de **venta B2B** — solo para la página *Previsión Salidas* | 49 |
| `LOL_PBIMODELITEM` | SQL | Dimensión — artículos / modelo-color | 64 (1) |
| `LOL_PBITIENDAS` | SQL | Dimensión — tiendas / almacenes | 17 |
| `LOL_PBICLIENTES` | SQL | Dimensión — interlocutores (proveedores y clientes) | 29 |
| `Calendario` | DAX | Dimensión de fecha, de 2022-01-01 a `MAX ( TODAY (), MAX ( ORDERS[DueDate] ) )` | 34 |
| `DimensionesCompras`, `DimensionesCompras2` | DAX | Parámetros de campo (13 campos: referencia, categoría, familia, almacenaje, plancha, proveedor, tienda, operador, temporada, estado, año, mes, semana) | 3 |
| `UltimaActualizacion` | M | Marca de hora del último refresco (oculta) | 1 |
| `_Medidas` | DAX | Contenedor de las 63 medidas | 1 |

Además siguen **1 plantilla + 46 tablas `LocalDateTable_*`** ocultas de la fecha/hora automática (`__PBI_TimeIntelligenceEnabled = 1`), una por columna de fecha de las tablas de hechos, de `LOL_PBISTOCKTRANSFERSTATUS` y de `LOL_PBITIENDAS`. Ver §9.

---

## 3. Las tres tablas de documento

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

- **`CANCELED`** (documento anulado) está en `LOL_PBIPURCHASEORDERS` y en `LOL_PBIPURCHASEGOODRECEIPT`, **no** en la factura. Las medidas lo filtran siempre (`CANCELED = "N"`).
- `LOL_PBIPURCHASEORDERS` tiene además las columnas de calidad y logística de la línea: `FechaDisponibilidad`, `FechaInspeccion`, `FechaInspeccionReal`, `InspeccionCalidad`, `QualityAgency`/`QualityAgencyName`, `InspectionType`, `Tipo_Transporte`, `PrecioCompra`, `IdSauco`, `InvntItem`, `DV_Id`/`DV_Nombre`; y la columna calculada **`Estado`** (`LineStatus` → `"Abierto"`/`"Cerrado"`).

---

## 4. Relaciones

### 4.1 La cadena de documentos: Pedido → Entrada → Factura

SAP encadena documentos por (`BaseEntry`, `BaseLine`), pero eso son **dos** columnas: una relación de Power BI necesita una sola clave. La solución son **columnas calculadas de clave compuesta** con `&`:

```dax
-- LOL_PBIPURCHASEORDERS (línea propia)
KLinea = [DocumentInternalKey] & "-" & [DocumentLineInternalKey]

-- LOL_PBIPURCHASEGOODRECEIPT (línea propia + línea base, SOLO si la base es un pedido: tipo 22)
KLinea = [DocumentInternalKey] & "-" & [DocumentLineInternalKey]
KBase  = IF ( [BaseDocumentTypeCode] = "22" && NOT ISBLANK ( [BaseDocumentInternalKey] ) && [BaseDocumentInternalKey] <> "",
              [BaseDocumentInternalKey] & "-" & [BaseDocumentLineInternalKey] )

-- LOL_PBIPURCHASEINVOICE (línea base, SOLO si la base es una entrada: tipo 20)
KBase  = IF ( [BaseDocumentTypeCode] = "20" && NOT ISBLANK ( [BaseDocumentInternalKey] ) && [BaseDocumentInternalKey] <> "",
              [BaseDocumentInternalKey] & "-" & [BaseDocumentLineInternalKey] )
```

Todas son `isHidden`. El `IF` es deliberado: las líneas **sin documento base** (o con una base de otro tipo de documento) devuelven `BLANK()` en lugar de la cadena basura `"-"` o de una clave que coincidiría por casualidad con un DocEntry de otra tabla.

### 4.2 Todas las relaciones de negocio

| Desde (muchos) | Hacia (uno) | Dirección | Significado |
|---|---|---|---|
| `LOL_PBIPURCHASEGOODRECEIPT[KBase]` | `LOL_PBIPURCHASEORDERS[KLinea]` | simple | Entrada → línea de pedido que la originó |
| `LOL_PBIPURCHASEINVOICE[KBase]` | `LOL_PBIPURCHASEGOODRECEIPT[KLinea]` | simple | Factura → línea de entrada que la originó |
| `LOL_PBIPURCHASEORDERS[DocumentInternalKey]` | `LOL_PBIPURCHASEHEADER[KPedido]` | **ambas** | Línea de pedido → su cabecera |
| `LOL_PBIPACKAGES[KPedido]` | `LOL_PBIPURCHASEHEADER[KPedido]` | simple | Bulto → pedido |
| `LOL_PBISTOCKTRANSFERREAL[KEntrada]` | `LOL_PBIPURCHASEGOODRECEIPT[KLinea]` | simple | Línea de ST / traslado → línea de entrada |
| `LOL_PBISTOCKTRANSFERSTATUS[KBaseEntrada]` | `LOL_PBIPURCHASEGOODRECEIPT[KLinea]` | **ambas** | Estado RMT → línea de entrada |
| `LOL_PBISTOCKTRANSFERSTATUS[KPedido]` | `LOL_PBIPURCHASEORDERS[KLinea]` | ⛔ inactiva | Atajo RMT → pedido (cerraría un bucle con la anterior) |
| `LOL_PBISTOCKTRANSFERSTATUS[SKU]` / `[WhsCode]` | `LOL_PBIMODELITEM[ItemCode]` / `LOL_PBITIENDAS[TiendaID]` | ⛔ inactivas | Residuo, sin uso |
| `LOL_PBIPURCHASEORDERS[ItemCode]` | `LOL_PBIMODELITEM[ItemCode]` | simple | Artículo |
| `LOL_PBIPURCHASEORDERS[WarehouseCode]` | `LOL_PBITIENDAS[TiendaID]` | simple | Almacén destino |
| `LOL_PBIPURCHASEORDERS[CardCode]` | `LOL_PBICLIENTES[CodeCliente]` | simple | Proveedor |
| `LOL_PBIPURCHASEORDERS[FechaRecepcion]` | `Calendario[Date]` | simple | Eje temporal de compras |
| `LOL_PBIPURCHASEINVOICE[FechaRecepcion]` | `Calendario[Date]` | ⛔ inactiva | Sin uso |
| `LOL_PBIB2BSALESORDER[DocDueDate]` | `Calendario[Date]` | simple | Eje temporal de ventas (Previsión Salidas) |
| `LOL_PBIB2BSALESORDER[ItemCode]` / `[WhsCode]` / `[CardCode]` | `LOL_PBIMODELITEM` / `LOL_PBITIENDAS` / `LOL_PBICLIENTES` | simple | Dimensiones de la venta |

Encadenadas, las relaciones físicas dan el recorrido completo: al filtrar un pedido se propaga a sus entradas, de ahí a sus facturas, a sus ST/traslados (`STOCKTRANSFERREAL`) y a su estado RMT. El packing **no** cuelga de la entrada sino de la cabecera, porque `@GSP_MWPACKAGES` identifica el pedido (vía `PDN1.BaseEntry`), no la línea de entrada.

> Las dimensiones solo cuelgan de `LOL_PBIPURCHASEORDERS` (y de la venta B2B), así que entrada, factura, ST y packing se filtran por artículo/almacén **a través del pedido**. Por eso siguen sin ambigüedad las relaciones físicas, frente al modelo B2BSalesOrder, que tuvo que resolver su cadena con `TREATAS`.

> La relación inactiva `AutoDetected` factura↔pedido por `LineUID` que documentaba la versión anterior **ya no existe**.

### 4.3 El cruce cartesiano en la matriz de entradas

`GOODRECEIPT → ORDERS` es de dirección simple: el filtro baja del pedido a sus entradas, pero **no sube**. Cuando la matriz de *Análisis de entradas* pone el albarán (`LOL_PBIPURCHASEGOODRECEIPT[DocumentNumber]`) como nivel de fila, el albarán no filtra el pedido y cada pedido se cruza con **todos** los albaranes. Las medidas que suman el pedido (`Pedidos`, `Líneas de compra`, `Unidades`, `Cajas`) abren la relación solo en ese nivel:

```dax
Unidades =
IF ( ISINSCOPE ( LOL_PBIPURCHASEGOODRECEIPT[DocumentNumber] ),
    CALCULATE ( SUM ( LOL_PBIPURCHASEORDERS[Quantity] ), LOL_PBIPURCHASEORDERS[CANCELED] = "N",
                CROSSFILTER ( LOL_PBIPURCHASEGOODRECEIPT[KBase], LOL_PBIPURCHASEORDERS[KLinea], BOTH ) ),
    CALCULATE ( SUM ( LOL_PBIPURCHASEORDERS[Quantity] ), LOL_PBIPURCHASEORDERS[CANCELED] = "N" )
)
```

Fuera de esa fila la medida se comporta igual que antes. `Cajas`, en lugar de `CROSSFILTER`, pasa los DocEntry de la entrada al packing con `TREATAS` sobre `LOL_PBIPACKAGES[AlbaranEntradaEntry]`.

---

## 5. Tablas logísticas

### `LOL_PBIPURCHASEHEADER` — cabecera de pedido

Lee `OPOR` **entera** (también los anulados) para que la relación con las líneas no tenga miembro en blanco; los anulados se apartan con `Anulado = "Y"` en el estado y en las medidas. Columnas de origen: `DocEntryPedido`, `Pedido`, `FechaPedido`, `Temporada`, `CodProveedor`, `Proveedor`, `EstadoPedido`, `Anulado`, `IdSauco` (id de la API de SAUCO), `CajasDeclaradas`, `PalesDeclarados`, `EstadoRMT`.

Columnas calculadas (agregan por contexto de fila a través de las relaciones):

| Columna | Qué es |
|---|---|
| `KPedido`, `PedidoTexto` | Clave de texto (`FORMAT ( DocEntry, "0" )`) y nº de pedido como texto para el buscador |
| `UdsPedidasTot`, `UdsRecibidasTot`, `UdsEnSTTot`, `UdsEnAlmacenTot` | Unidades pedidas, recibidas (entradas no anuladas), en ST y trasladadas (`WTR1`) del pedido |
| `CajasTot`, `CajasEnSTTot` | Bultos del pedido y los ya copiados a su ST |
| `AlmacenPrincipal` | Almacén con más unidades del pedido (170 de 428 pedidos reparten líneas entre almacenes) |
| `OrdenEstado` / `Estado` | Escalera: 0 `anulado`, 1 `pendiente`, 2 `sin traslado`, 3 `en tránsito`, 4 `llegada parcial`, 5 `en el almacén`. Es **columna** para poder agrupar y segmentar |
| `ConEntrada`, `SinCajas`, `PendienteDeCopiar`, `ConSauco` | Banderas Sí/No |
| `RMTOrden` / `RMTEstadoPedido` / `RMTFechaPedido` | Estado RMT **más atrasado** de las líneas del pedido (MIN del código), su etiqueta y la última fecha |

### `LOL_PBIPACKAGES` — packing

Consulta nativa sobre `@GSP_MWPACKAGES` + `@GSP_MWPACK_LIN`, con los documentos por los que ha pasado cada bulto pivotados desde `@GSP_MWPACK_DOCS` (entrada, ST, traslado) y el pedido deducido de `PDN1`. Solo bultos no anulados y de flujo *Compras*. `PesoCajaUnaVez` trae el peso solo en la primera línea de cada caja (`PesoCaja` se repite por artículo). Calculadas: `KPedido`, `CopiadaAST` (tiene ST), `ApuntaA` (documento más avanzado: traslado › ST › entrada), `EntradaTexto` y `STTexto` para los buscadores.

### `LOL_PBISTOCKTRANSFERREAL` — solicitud de traslado y traslado real

Una fila por línea de ST (`OWTQ`/`WTQ1`) cuya base es una entrada (`BaseType = 20`), con lo trasladado de verdad sumado desde `WTR1` (`BaseType = 1250000001`). Columnas: `DocEntryAE`/`LineaAE` (línea de entrada), `Solicitud_traslado`, `CantidadSolicitada`, `EstadoST`, `STAnulada`, `Traslado`, `FechaTraslado`, `AlmacenOrigen`/`AlmacenDestino`, `DocumentosTraslado`, `CantidadTrasladada`. Calculada: `KEntrada`.

> **Es la única fuente fiable de llegada física.** Los campos `…RMT` de `LOL_PBISTOCKTRANSFERSTATUS` son lo que declara el operador logístico y no cuadran con SAP (29.278 frente a 30.017 uds en la comprobación).

### `LOL_PBISTOCKTRANSFERSTATUS` — estado RMT

Vista del ETL con el estado logístico de cada línea de ST según el operador. Calculadas: `KBaseEntrada`, `KPedido` (`RELATED` de la entrada), `CantidadPedida` (`LOOKUPVALUE` al pedido), `RMTCodigo` (estado de línea; si viene vacío —39 % de las filas— cae al de cabecera), `RMTEstado` (etiqueta: 20 documento recibido, 25 en plancha, 28 control de calidad, 30 mercancía llegada, 50 recepción recibida, 100 recepción finalizada, 200 varios) y `RMTFecha` (texto ISO → fecha).

---

## 6. Dimensiones

### `LOL_PBIMODELITEM` — artículos / modelo-color

Maestro de producto compartido con B2BSalesOrder y SgiRetail. Clave `ItemCode`. Jerarquía comercial (`Categoria`, familia agregada, `Nombre_familia`, subfamilia, `GrupodeArt` como *marca*, `Material`, `TIPO`), descripciones y variantes `_EN`, talla/color, temporada, proveedor (`Codigo_de_Proveedor`, `Nombre_de_Proveedor`), aduana, PVP por país, `URL_imagen_producto`, flags, y los atributos logísticos `Tipo_almacenaje` y `Plancha_en_RMT`. Calculada: **`Referencia-Modelo`** (`Referencia_modelo & " - " & …`), base de la medida `Referencias`.

### `LOL_PBITIENDAS` — tiendas / almacenes

Clave `TiendaID`. `NombreTienda`, `TipoTienda`, `Territorio`, `OperadorLogistico`, fechas de apertura/cierre/comunicación y datos técnicos del TPV.

### `LOL_PBICLIENTES` — interlocutores

Maestro de interlocutores del ETL (clientes y proveedores comparten tabla en SAP). Clave `CodeCliente`. Se relaciona con el pedido de compra (`CardCode` = proveedor) y con la venta B2B (`CardCode` = cliente). Hoy ningún visual lo usa.

### `Calendario`

Tabla calculada (`CALENDAR` desde 2022-01-01) con claves (`FechaKey`, `AñoMesKey`, `AñoSemanaKey`), año, mes (`MesTexto`), semana (`NombreSemana`) y jerarquía Año → Mes → Semana. Marcada como tabla de fechas (`dataCategory: Time`). Solo filtra `LOL_PBIPURCHASEORDERS` por **`FechaRecepcion`** y `LOL_PBIB2BSALESORDER` por **`DocDueDate`**; entradas, ST, packing y cabecera no tienen relación directa (les llega el filtro a través del pedido).

### `DimensionesCompras` / `DimensionesCompras2`

Dos parámetros de campo idénticos (13 opciones) pensados para que el usuario elija el eje de filas y de columnas. Ver §9: hoy los segmentadores existen pero ningún visual consume el parámetro.

---

## 7. Medidas

**63 medidas** en `_Medidas` (partición `Row("Column", BLANK())`), organizadas en 8 carpetas. Reglas que comparten:

- **Anulados fuera siempre:** `LOL_PBIPURCHASEORDERS[CANCELED] = "N"`, `LOL_PBIPURCHASEGOODRECEIPT[CANCELED] = "N"`, `LOL_PBIPURCHASEHEADER[Anulado] = "N"`. Medido contra `SBO_LOLA_SUCURSAL` (temporada S27+F27, DocDate 2026-01-01..10-18): sin filtro 511 pedidos y 990.752 uds; con él, los 428 y 893.988 de control. En entradas, 51.608 → 43.748.
- **Rama `ISINSCOPE` + `CROSSFILTER BOTH`** en `Pedidos`, `Líneas de compra`, `Unidades` y `Cajas` (ver §4.3).
- **Packing por `TREATAS`** desde `LOL_PBIPURCHASEORDERS[DocumentInternalKey]` a `LOL_PBIPACKAGES[KPedido]`.
- **Escalera de estado** (`Sin entrada` → `Sin traslado` → `En tránsito` → `Llegada parcial` → `En el almacén`): tener ST **no** es estar recibido; solo el traslado real (`WTR1`) cuenta como llegada.

### 0 Pildoras (16)

| Medida | Qué mide |
|---|---|
| `Chip <campo>` ×8 | Texto de la píldora de filtro activo: el valor si hay uno, `n sel.` si hay varios, en blanco si no se filtra. Campos: `Categoria`, `MktDoc_Row_GSP_SEASON`, `NombreTienda`, `Nombre_de_Proveedor`, `Nombre_familia`, `OperadorLogistico`, `Plancha_en_RMT`, `Tipo_almacenaje` |
| `Chip <campo> Color` ×8 | Color de la píldora: rosa `#DFA0C9` si el campo está filtrado, `#6F263D` si no |

### 0 Contexto (3)

| Medida | Qué mide |
|---|---|
| `Última actualización` | "Actualizado dd/MM/yyyy a las HH:mm" desde `UltimaActualizacion` |
| `Rango de fechas` | Mín–máx de `Calendario[Date]` en contexto |
| `Rango y actualización` | Las dos juntas, para la tarjeta de cabecera |

### 1 Volumen (6)

| Medida | Qué mide |
|---|---|
| `Pedidos de compra` | Pedidos distintos (`DISTINCTCOUNT` de DocEntry en las **líneas**), sin anulados |
| `Líneas de compra` | Líneas de pedido sin anulados (rama `ISINSCOPE`) |
| `Pedidos abiertos` | `Pedidos de compra` con alguna línea `Estado = "Abierto"` |
| `Proveedores` | `CardCode` distintos con pedido, sin anulados |
| `Referencias` | `Referencia-Modelo` distintas |
| `Unidades` | Unidades **pedidas** al proveedor, sin anulados (rama `ISINSCOPE`). Antes sumaba por error `LOL_PBIB2BSALESORDER[Qty]` (venta) |

### 2 Mercancia (9)

| Medida | Qué mide |
|---|---|
| `Unidades recibidas` | Unidades de entradas no anuladas |
| `Unidades en solicitud` | Lo que la ST pidió mover (`CantidadSolicitada`): stock en tránsito |
| `Unidades en el almacen` | Lo trasladado de verdad: `SUM ( LOL_PBISTOCKTRANSFERREAL[CantidadTrasladada] )` (= `WTR1.Quantity`). **No** usa los campos RMT |
| `Unidades en transito` | `MAX ( solicitud − almacén, 0 )` |
| `Orden estado mercancia` | Peldaño 1–5 de la escalera, para ordenar |
| `Estado de la mercancia` | Etiqueta del peldaño |
| `Estado logistico` | `MIN` del estado de línea RMT (el más atrasado) |
| `% recibido` | `Unidades recibidas / Unidades` |
| `% en el almacen` | `Unidades en el almacen / Unidades` |

### 3 Embalaje (8)

| Medida | Qué mide |
|---|---|
| `Cajas` | Bultos (`PackCode` distintos) del pedido por `TREATAS`; con el albarán en fila, los del albarán |
| `Cajas copiadas a ST` | Bultos que ya figuran en su ST |
| `Cajas pendientes de copiar` | `Cajas − Cajas copiadas a ST` |
| `Unidades en caja` | Unidades embaladas (suma del contenido de las cajas) |
| `Peso embalado` | Suma de `PesoCajaUnaVez` |
| `Faltan cajas` | Bandera Sí/No: hay entrada y ni un bulto |
| `Pendiente de copiar` | Bandera Sí/No: hay bultos fuera de su ST |
| `Pedidos sin cajas` | Pedidos con entrada y sin bultos |

### 4 Resumen (15)

| Medida | Qué mide |
|---|---|
| `Pedidos` | Pedidos no anulados contados en la **cabecera** (rama `ISINSCOPE`) |
| `Pedidos con entrada` | `Pedidos` con `ConEntrada = "Sí"` |
| `Pedidos en el almacen` / `Pedidos con llegada parcial` / `Pedidos en transito` | `Pedidos` con `OrdenEstado` = 5 / 4 / 3 |
| `Pedidos con SAUCO` / `Pedidos sin SAUCO` | Con / sin id de la API de SAUCO (el almacén 4-C002L va por otro circuito: sin packing ni id) |
| `% con SAUCO` | Parte de los pedidos con id |
| `% del total pedido` | Peso de la fila sobre `ALLSELECTED ( LOL_PBIPURCHASEHEADER )` |
| `Entradas` | Albaranes de entrada distintos, sin anulados |
| `Entradas con cajas` / `Entradas sin cajas` | Entradas con / sin bultos del packing |
| `% entradas con cajas` | Parte de las entradas con packing |
| `HTML Resumen Compras` | Las siete tarjetas de cabecera de *Análisis de entradas* en un solo HTML |
| `HTML Detalle Pedido` | Ficha completa del pedido (cabecera, cifras, logística, fechas, documentos, RMT y contenido de las cajas) para la página de tooltip |

### 5 RMT (4)

| Medida | Qué mide |
|---|---|
| `Estado RMT` | Etiqueta del estado RMT más atrasado en contexto |
| `Fecha estado RMT` | Última fecha de estado |
| `Estados RMT distintos` | Cuántos estados conviven (>1 = el pedido va a varias velocidades) |
| `Líneas en RMT` | Líneas de ST con estado RMT |

### 6 Documentos (2)

| Medida | Qué mide |
|---|---|
| `Albarán de entrada` | Nº de albarán si hay uno, "n albaranes" si hay varios; en blanco en la fila del propio albarán. Sale de `LOL_PBIPURCHASEGOODRECEIPT` sin anulados, **no** de `STOCKTRANSFERSTATUS[AlbaranEntrada]` (dejaría fuera los 2.281 albaranes sin traslado) |
| `Solicitud de traslado` | Nº de ST si hay una, "n ST" si hay varias (cada albarán tiene como mucho una, medido 2026-11-18) |

---

## 8. Informe

`PurchaseOrders.Report`, 4 páginas. Las tres visibles comparten cabecera, menú de navegación (botones), tarjeta `Rango y actualización` y la barra de segmentadores: temporada, categoría, familia, proveedor, tienda, operador logístico, tipo de almacenaje, plancha en RMT y los dos parámetros de campo.

| Página | Tamaño | Contenido |
|---|---|---|
| **Previsión Entradas** (30 visuales) | 1280×720 | Dos matrices de unidades **pedidas** por `Calendario` (Año → Mes → Semana) × `Referencia-Modelo`, una por `Plancha_en_RMT` y otra por `Tipo_almacenaje`. Agregación implícita `Sum(LOL_PBIPURCHASEORDERS.Quantity)` |
| **Previsión Salidas** (37 visuales) | 1280×720 | Seis matrices del mismo diseño con `Sum(LOL_PBIB2BSALESORDER.Qty)` (unidades de **venta** B2B por `DocDueDate`) |
| **Análisis de entradas** (36 visuales) | 1280×1600 | `HTML Resumen Compras` (7 tarjetas), buscadores de texto (nº de pedido, nº de entrada, SSCC, nº de ST) y la **matriz en diseño tabular** Estado → Pedido → Albarán → Estado RMT → Proveedor → Almacén principal, con columnas `% del total pedido`, `Pedidos`, `Líneas de compra`, **ALBARANES** (`Albarán de entrada`), **ST** (`Solicitud de traslado`), `Unidades`, `Unidades recibidas`, `% recibido`, `Unidades en el almacen`, `% en el almacen`, `Cajas`, `Pedidos sin cajas`, `Estado RMT`, `Fecha estado RMT`. Es la página activa al abrir |
| **Ficha de pedido** (1 visual) | 620×620 | Página de **tooltip** (`type: Tooltip`, oculta) con `HTML Detalle Pedido`; la invoca la matriz de *Análisis de entradas* |

La factura (`LOL_PBIPURCHASEINVOICE`) y `LOL_PBICLIENTES` ya no aparecen en ningún visual: la antigua *Página 3* con la matriz de la cadena completa desapareció.

---

## 9. Notas y deuda técnica

1. **Fecha/hora automática sigue activada.** Ya existe `Calendario`, pero quedan 46 `LocalDateTable_*` (han crecido desde las 27 de agosto con `STOCKTRANSFERSTATUS` y la venta B2B). Desactivarla aligera modelo y refresco; antes hay que quitar los bloques `variation` de las columnas de fecha.
2. **`Calendario` solo filtra pedido (`FechaRecepcion`) y venta (`DocDueDate`).** Entradas, traslados y packing no tienen eje temporal propio; "por fecha" siempre significa "por fecha de recepción prevista del pedido". Queda además la relación inactiva `INVOICE[FechaRecepcion] → Calendario` sin uso.
3. **Las *Previsión* siguen con agregaciones implícitas.** `Sum(Quantity)` en *Previsión Entradas* **no excluye pedidos anulados** (la medida `Unidades` sí): las dos páginas pueden no cuadrar. Sustituir por `[Unidades]`.
4. **Parámetros de campo huérfanos.** `DimensionesCompras` y `DimensionesCompras2` tienen segmentador en las tres páginas, pero ninguna matriz usa el parámetro en sus filas/columnas: el segmentador no hace nada.
5. **Segmentadores de compra en *Previsión Salidas*.** La temporada (`LOL_PBIPURCHASEORDERS[MktDoc_Row_GSP_SEASON]`) no llega a `LOL_PBIB2BSALESORDER`: pedido → artículo es de dirección simple, así que en esa página ese filtro no afecta a la venta. Comprobar que es lo que se quiere.
6. **Etiquetas cruzadas en los HTML.** En `HTML Resumen Compras` y `HTML Detalle Pedido`, el valor de `[Unidades recibidas]` se rotula "Unidades en tránsito"/"En tránsito" y el de `[Unidades en el almacen]` se rotula "Unidades recibidas"/"Recibidas". Si es vocabulario de negocio deliberado, conviene renombrar las medidas para que coincidan; si no, es un error de rotulado.
7. **Dos definiciones paralelas del estado.** La columna `LOL_PBIPURCHASEHEADER[Estado]` (`pendiente`, `sin traslado`…, en minúscula, con `anulado`) y la medida `Estado de la mercancia` (`Sin entrada`, `Sin traslado`…) aplican la misma escalera con textos distintos. Lo mismo `Pedidos de compra` (líneas) frente a `Pedidos` (cabecera). Cualquier cambio de regla hay que hacerlo en los dos sitios.
8. **Comentario desfasado en el packing.** `CajasTot` dice "el packing no tiene relación: se cruza por la clave", pero ya existe `LOL_PBIPACKAGES[KPedido] → LOL_PBIPURCHASEHEADER[KPedido]`. Las medidas de embalaje siguen usando `TREATAS`; funciona, pero la relación y el `TREATAS` hacen lo mismo por dos caminos.
9. **Relaciones bidireccionales** `ORDERS ↔ HEADER` y `STOCKTRANSFERSTATUS ↔ GOODRECEIPT`. Las calculadas de la cabecera (`Uds…Tot`, `RMTOrden`) dependen de ellas. Si en el futuro se relaciona el packing, la ST o la entrada con las dimensiones, aparecerán caminos múltiples.
10. **Relaciones inactivas residuales:** `STOCKTRANSFERSTATUS[SKU]`, `[WhsCode]` y `[KPedido]`, e `INVOICE[FechaRecepcion]`. Candidatas a borrar.
11. **Tres consultas nativas a HANA embebidas en el modelo.** El SQL de `LOL_PBIPACKAGES` es largo (subconsultas pivotadas de `@GSP_MWPACK_DOCS`, `ROW_NUMBER`) y vive en una sola línea del TMDL: difícil de revisar y versionar. Además, el modelo depende ahora de dos puertas de enlace/orígenes (SQL y HANA). Valorar llevarlas al ETL como vistas `LOL_PBI*`.
12. **Calculadas de la cabecera con contexto de fila.** 18 columnas calculadas en `LOL_PBIPURCHASEHEADER` (varias con `CALCULATE` sobre tablas grandes y una `TOPN` por fila en `AlmacenPrincipal`) se recalculan en cada refresco. Vigilar el tiempo de proceso.
13. **Importes sin usar.** Todas las medidas son de unidades, bultos y recuentos; `LineTotalAmountLC` y el resto de importes siguen fuera del informe.
14. **`Chip … Color`** usa `#6F263D` para el estado sin filtrar, color que no está en la paleta oficial de tres colores.
15. **`docs/README.md` sigue diciendo "0 medidas" y "sin Calendario" para Compras**: actualizarlo.
16. **`cultures/es-ES.tmdl`** solo contiene metadatos lingüísticos de Q&A auto-generados.

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
