# Modelo semántico: B2BSalesOrder

> 📁 **Esta documentación está replicada en todas las ramas.** Describe el modelo tal y como está en la rama **`compras` / `finanzas`**. En otras ramas la carpeta `SalesOrder/` puede existir pero en un estado anterior: para trabajar sobre el modelo, sitúate en la rama indicada. Índice: [README.md](README.md).

> Documentación técnica generada a partir del modelo TMDL ubicado en
> [`SalesOrder/B2BSalesOrder.SemanticModel/definition/`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/).
> **Solo lectura**: este documento describe el modelo, no lo modifica.
>
> ⚠️ **Rama.** Esta documentación describe el estado de las ramas **`compras` / `finanzas`** (mismo árbol). La rama `salesorder` se quedó en 2026-07-16 y **no** contiene el embudo por documento ni las devoluciones descritos aquí.

---

## 1. Resumen del modelo

**B2BSalesOrder** es un modelo semántico de Power BI (compatibilityLevel **1600**, `powerBI_V3`, cultura **es-ES**) orientado al análisis de **pedidos de venta B2B** (canal mayorista) de Lola Casademunt. Tiene dos ejes analíticos:

1. **Comparativa entre temporadas** — la temporada seleccionada frente a la temporada anterior equivalente, sobre venta bruta y neta, más el tratamiento separado de **incidencias** (devoluciones / RMA) y un **catálogo visual de productos** en HTML.
2. **Embudo de cumplimiento por documento** — recorrido Pedido → Albarán → Factura → Nota de crédito / Devolución, para medir qué parte de lo pedido se ha servido, facturado y devuelto.

| Concepto | Valor |
|---|---|
| Nombre del modelo | `Model` (proyecto B2BSalesOrder) |
| compatibilityLevel | 1600 |
| Cultura / idioma de origen | es-ES |
| Time Intelligence automático | Activado (`__PBI_TimeIntelligenceEnabled = 1`) |
| Tablas "reales" (visibles/funcionales) | 21 |
| Tablas auto-generadas de fechas | 1 plantilla + 20 `LocalDateTable_*` ocultas |
| Nº de medidas | **96** |
| Orígenes de datos | SQL Server `192.168.0.232`, base `hana_etl_admin`, esquema `dbo` (10 tablas importadas vía M) + tablas calculadas DAX |
| RLS / Roles | **Ninguno** |

### Arquitectura

- **Hechos — pedido:** [`LOL_PBIB2BSALESORDER`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIB2BSALESORDER.tmdl) (líneas de pedido B2B). Es la tabla central.
- **Hechos — cadena de documentos:** [`LOL_PBISALESDELIVERY`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBISALESDELIVERY.tmdl) (albaranes), [`LOL_PBIINVOICESSALES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIINVOICESSALES.tmdl) (facturas), [`LOL_PBICREDITNOTESSALES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBICREDITNOTESSALES.tmdl) (notas de crédito), [`LOL_PBIRETURNSALES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIRETURNSALES.tmdl) (devoluciones), [`LOL_PBIRETURNREQUESTS`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIRETURNREQUESTS.tmdl) (solicitudes de devolución).
- **Hechos derivada:** [`VentasIncidencias`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/VentasIncidencias.tmdl) (subconjunto calculado del pedido, solo incidencias).
- **Dimensiones de origen SQL:** [`LOL_PBICLIENTES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBICLIENTES.tmdl), [`LOL_PBIAGENTES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIAGENTES.tmdl), [`LOL_PBIMODELITEM`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIMODELITEM.tmdl), [`LOL_PBISEASON`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBISEASON.tmdl).
- **Dimensión de fechas calculada:** [`Calendario`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/Calendario.tmdl).
- **Apoyo / parámetros:** [`TemporadasUsadas`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/TemporadasUsadas.tmdl), [`DimensionB2B`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/DimensionB2B.tmdl), [`OrdenSelector`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/OrdenSelector.tmdl), [`ParamTopN`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/ParamTopN.tmdl).
- **Contenedores de medidas (sin datos):** `MedidasVentas`, `MedidasTemporadas`, `MedidasIncidencias`, `MedidasFormato`, `MedidasCatalogo`.

> **Cambio respecto a versiones anteriores del modelo:** la tabla `B2C1` (*field parameter* de segmentación) **ya no existe**. Y `TemporadasUsadas`, que era una tabla desconectada, ahora **sí tiene relación física** con `LOL_PBISEASON[Code]`.

---

## 2. Tablas

### 2.1. Tabla de hechos — `LOL_PBIB2BSALESORDER`

**Propósito:** Líneas de pedidos de venta B2B (grano = línea de documento).
**Origen:** Consulta M sobre SQL Server.

```m
let
    Origen = Sql.Database("192.168.0.232", "hana_etl_admin"),
    dbo_LOL_PBIB2BSALESORDER = Origen{[Schema="dbo",Item="LOL_PBIB2BSALESORDER"]}[Data],
    #"LineTotal_Num" = Table.AddColumn(dbo_LOL_PBIB2BSALESORDER, "LineTotal_Num", each try Number.From([LineTotal_LC], "en-US") otherwise 0, type number),
    #"ImporteBruto" = Table.AddColumn(#"LineTotal_Num", "ImporteBruto", each try Number.From([ImporteBruto_LC], "en-US") otherwise 0, type number)
in
    #"ImporteBruto"
```

> **Importante:** casi todas las columnas llegan como **texto (string)** desde el origen. El paso M convierte dos importes a número creando `LineTotal_Num` (venta neta) e `ImporteBruto` (venta bruta) con `Number.From(..., "en-US")` y `try ... otherwise 0`.

**Columnas numéricas (las que suman):**

| Columna | Tipo | Significado |
|---|---|---|
| `ImporteBruto` | double (€) | Venta **bruta** por línea (derivada en M de `ImporteBruto_LC`). Base de las medidas de temporada. |
| `LineTotal_Num` | double (€) | Venta **neta** por línea (derivada en M de `LineTotal_LC`). |
| `ImporteBruto_LC` | double | Importe bruto en moneda local. Es el que usa el **embudo de cumplimiento**. |

**Columnas clave para relaciones y filtros:**

| Columna | Uso |
|---|---|
| `CardCode` | Cliente → `LOL_PBICLIENTES[CodeCliente]`. |
| `SlpCode` | Vendedor → `LOL_PBIAGENTES[SlpCode]`. |
| `ItemCode` | Artículo → `LOL_PBIMODELITEM[ItemCode]`. |
| `Season` | Temporada → `LOL_PBISEASON[Code]`. |
| `DocDate` | Fecha de documento → relación activa con `Calendario[Date]`. |
| `DocEntry`, `LineNum` | Identificación de la línea; base de la clave de cadena. |
| `LineUID` | Identificador de línea usado para enlazar notas de crédito y devoluciones. |
| `Canceled` | Marca de anulación (`"Y"`). |
| `IsIncidence` | Marca de incidencia (`"Y"`). |
| `Reposicion` | Marca de reposición (`"Y"`); distingue pedido inicial de reposición. |
| `RMAMotiveCod`, `IncidenceType` | Motivo / tipo de incidencia. |
| `Qty` | Cantidad (texto; se convierte con `VALUE`/`INT`). |

**Columna calculada:**
```dax
KPedidoLinea = IF ( NOT ISBLANK ( LOL_PBIB2BSALESORDER[LineNum] ),
                    LOL_PBIB2BSALESORDER[DocEntry] & "-" & LOL_PBIB2BSALESORDER[LineNum] )
```

---

### 2.2. Las tablas de la cadena de documentos

Cinco tablas nuevas, todas importadas directamente de `dbo.*` sin transformación:

| Tabla | Grano | Importe que se suma | Columna calculada |
|---|---|---|---|
| `LOL_PBISALESDELIVERY` | Línea de albarán | `ImporteNetoLineaML` | `KAlbaranLinea`, `KAlbaranBase` |
| `LOL_PBIINVOICESSALES` | Línea de factura | `LineTotal_LC` | `KFacturaBase` |
| `LOL_PBICREDITNOTESSALES` | Línea de nota de crédito | `LineTotal_LC` | — (enlaza por `LineUID`) |
| `LOL_PBIRETURNSALES` | Línea de devolución | `LineTotal_LC` | — (enlaza por `LineUID`) |
| `LOL_PBIRETURNREQUESTS` | Línea de solicitud de devolución | `LineTotal_LC` | `SeasonResuelta` |

**Claves de cadena** (concatenación con `&`, ver §3):

```dax
-- LOL_PBISALESDELIVERY
KAlbaranBase  = IF ( LOL_PBISALESDELIVERY[BaseType] = "17",           -- 17 = Pedido de venta
                     LOL_PBISALESDELIVERY[BaseEntry] & "-" & LOL_PBISALESDELIVERY[BaseLine] )
KAlbaranLinea = IF ( NOT ISBLANK ( LOL_PBISALESDELIVERY[LineaInternaDocumento] ),
                     LOL_PBISALESDELIVERY[DocEntry] & "-" & LOL_PBISALESDELIVERY[LineaInternaDocumento] )

-- LOL_PBIINVOICESSALES
KFacturaBase  = IF ( LOL_PBIINVOICESSALES[BaseType] = "15",           -- 15 = Albarán
                     LOL_PBIINVOICESSALES[BaseEntry] & "-" & LOL_PBIINVOICESSALES[BaseLine] )
```

El filtro por `BaseType` es esencial: una factura puede colgar de un albarán (`15`) o directamente de un pedido (`17`); solo se encadenan las que vienen de albarán, para no contar dos veces.

**`SeasonResuelta`** en `LOL_PBIRETURNREQUESTS` rellena la temporada cuando la solicitud no la trae, tomándola del artículo:
```dax
SeasonResuelta =
IF ( NOT ISBLANK ( LOL_PBIRETURNREQUESTS[Season] ) && LOL_PBIRETURNREQUESTS[Season] <> "",
     LOL_PBIRETURNREQUESTS[Season],
     IF ( RELATED ( LOL_PBIMODELITEM[Codigo_de_temporada] ) <> ""
          && RELATED ( LOL_PBIMODELITEM[Codigo_de_temporada] ) <> "000",
          RELATED ( LOL_PBIMODELITEM[Codigo_de_temporada] ) ) )
```

---

### 2.3. Tabla de hechos derivada — `VentasIncidencias`

**Propósito:** Aísla las líneas marcadas como incidencia (`IsIncidence = "Y"`) para analizarlas por separado de la venta "limpia".
**Origen:** **Tabla calculada DAX**:

```dax
SELECTCOLUMNS (
    FILTER ( 'LOL_PBIB2BSALESORDER', 'LOL_PBIB2BSALESORDER'[IsIncidence] = "Y" ),
    "TipoRegistro", "Incidencia",
    "LineID", 'LOL_PBIB2BSALESORDER'[DocEntry] & "-" & 'LOL_PBIB2BSALESORDER'[ItemCode],
    "DocEntry", 'LOL_PBIB2BSALESORDER'[DocEntry],
    "LineNum", 'LOL_PBIB2BSALESORDER'[LineNum],
    "IncidenciaCode", 'LOL_PBIB2BSALESORDER'[RMAMotiveCod],
    "IncidenciaDesc", 'LOL_PBIB2BSALESORDER'[IncidenceType],
    "FechaDocumento", 'LOL_PBIB2BSALESORDER'[DocDate],
    "CodigoCliente", 'LOL_PBIB2BSALESORDER'[CardCode],
    "CodigoVendedor", 'LOL_PBIB2BSALESORDER'[SlpCode],
    "CodigoArticulo", 'LOL_PBIB2BSALESORDER'[ItemCode],
    "TemporadaLinea", 'LOL_PBIB2BSALESORDER'[Season],
    "PedidoReposicionLinea", 'LOL_PBIB2BSALESORDER'[Reposicion],
    "Cantidad", IFERROR ( INT ( 'LOL_PBIB2BSALESORDER'[Qty] ), 0 ),
    "VentaBrutaML", 'LOL_PBIB2BSALESORDER'[ImporteBruto],
    "ImporteNetoML", 'LOL_PBIB2BSALESORDER'[LineTotal_Num]
)
```

---

### 2.4. Dimensiones

**`LOL_PBICLIENTES`** — Maestro de clientes B2B. Clave `CodeCliente`. Destacadas: `RazonSocial`, `GroupName`/`GroupCode`, `Pais`, `Provincia`, `Poblacion`, `CP`, `NIF`, `Moneda`, `TarifaCode`/`TarifaName`, `Balance`.

**`LOL_PBIAGENTES`** — Maestro de vendedores. Clave `SlpCode`. Destacadas: `Agente_Cajero`, `Email`, `Commission`, `Bloqueado`, `Memo`.

**`LOL_PBIMODELITEM`** — Maestro de productos (modelo-color, familias, PVP por país, imágenes). Clave `ItemCode`. Columna calculada:
```dax
Modelo-Color = LOL_PBIMODELITEM[Referencia_modelo] & "." & LOL_PBIMODELITEM[Codigo_de_color]
```
Es la **clave de agrupación de producto** del catálogo HTML. Otras destacadas: `Categoria`, `Descripcion_familia_agregada`, `Nombre_familia`, `Subfamilia`, `GrupodeArt` (marca), `Material`, `Codigo_de_temporada`, los `PVP_*` (string) y `URL_imagen_producto`.

**`LOL_PBISEASON`** — Maestro de temporadas y su lógica de comparación. Clave `Code`.

| Columna | Significado |
|---|---|
| `Code`, `Name` | Código y nombre de temporada. |
| `CompareSeason` | Temporada con la que se compara (la "anterior equivalente"). |
| `TemporadaAnterior` | Temporada inmediatamente anterior ("temporada pasada"). |
| `Inicio`, `Fin`, `FechaInicioB2B` | Fechas de la temporada. La comparación por días equivalentes usa **`Fin`** como ancla, no `Inicio` (ver §6). |
| `AlmacenB2B`, `PptoB2B` | Almacén y presupuesto B2B. |

**`Calendario`** — Tabla calculada `CALENDAR` desde `2022-01-01` hasta el `31/12` de (año actual + 2), enriquecida con `FechaKey`, `AñoMesKey`, `Año`, `Mes`, `MesTexto`, `Trimestre`, `Semana`, `InicioMes`/`FinMes`, `InicioTrimestre`/`FinTrimestre`, `InicioAño`/`FinAño` y flags `EsHoy`, `EsPasado`, `EsFuturo`, `EsMesActual`, `EsAñoActual`. Marcada como dimensión de fecha.

### 2.5. Tablas de apoyo y parámetros

| Tabla | Tipo / Origen | Propósito |
|---|---|---|
| `TemporadasUsadas` | Calculada: `DISTINCT(SELECTCOLUMNS('LOL_PBISEASON',"Code",…))` | Selector de temporada. `TemporadaSeleccionada` lee `SELECTEDVALUE` de su `Code`. **Ahora relacionada** con `LOL_PBISEASON[Code]`. |
| `ParamTopN` | `GENERATESERIES(5, 50, 5)` | Parámetro "Top N" del catálogo. |
| `OrdenSelector` | `DATATABLE` | Criterio de orden del catálogo: `Ventas (€)` (1), `Unidades` (2), `PVP` (3). |
| `DimensionB2B` | `DATATABLE` | Dimensión de agrupación del catálogo: Categoría(0), Familia agregada(1), Familia(2), Subfamilia(3), Marca(4), Cliente(5), Grupo cliente(6), País(7), Agente(8), Temporada(9). |

---

## 3. La cadena de documentos (Pedido → Albarán → Factura)

Es la parte más delicada del modelo, y la que más veces se rehízo.

### El problema

SAP encadena documentos por la pareja (`BaseEntry`, `BaseLine`). Eso son **dos columnas**, y una relación de Power BI necesita una sola clave. Además, las cinco tablas de documento comparten las **mismas dimensiones** (cliente, agente, artículo, temporada, calendario): si se relacionan todas físicamente entre sí *y* con las dimensiones, aparecen **caminos de filtro múltiples** y el motor da ambigüedad.

### La solución adoptada

1. **Claves compuestas en columnas calculadas**, concatenando con `&` (§2.2). Se probó `COMBINEVALUES` y se descartó: `&` es suficiente y no arrastra la semántica de "relación válida" que `COMBINEVALUES` promete al motor.
2. **Nada de relaciones físicas entre documentos.** Las que se habían creado (albarán→pedido, factura→albarán) se **eliminaron**; el recorrido se hace **en la medida, con `TREATAS`**.

```dax
Servido (cadena) =
VAR OrdKeys =
    FILTER ( VALUES ( LOL_PBIB2BSALESORDER[KPedidoLinea] ),
             NOT ISBLANK ( LOL_PBIB2BSALESORDER[KPedidoLinea] ) )
RETURN
CALCULATE (
    [Importe Servido],
    TREATAS ( OrdKeys, LOL_PBISALESDELIVERY[KAlbaranBase] ),
    REMOVEFILTERS ( LOL_PBISALESDELIVERY )
)

Facturado (cadena) =
VAR OrdKeys  = FILTER ( VALUES ( LOL_PBIB2BSALESORDER[KPedidoLinea] ), NOT ISBLANK ( … ) )
VAR DelvKeys =                                       -- salto intermedio: pedido → albarán
    CALCULATETABLE (
        FILTER ( VALUES ( LOL_PBISALESDELIVERY[KAlbaranLinea] ), NOT ISBLANK ( … ) ),
        TREATAS ( OrdKeys, LOL_PBISALESDELIVERY[KAlbaranBase] ),
        REMOVEFILTERS ( LOL_PBISALESDELIVERY )
    )
RETURN
CALCULATE (
    [Importe Facturado],
    TREATAS ( DelvKeys, LOL_PBIINVOICESSALES[KFacturaBase] ),   -- albarán → factura
    REMOVEFILTERS ( LOL_PBIINVOICESSALES )
)
```

`Notas Credito (cadena)` y `Devoluciones (cadena)` son más simples: enlazan directamente por `LineUID`, sin saltos intermedios.

El `REMOVEFILTERS` de la tabla destino es imprescindible: sin él, el filtro que llega por las dimensiones compartidas se combinaría con el `TREATAS` y devolvería solo la intersección.

> **Contraste con Compras.** En el modelo PurchaseOrders la misma cadena se resolvió con **relaciones físicas** y funciona, porque allí las tablas de entrada y factura **no** comparten dimensiones con el pedido: no hay caminos múltiples que resolver.

### Relaciones inactivas heredadas

Quedan tres relaciones desactivadas, residuos de los intentos anteriores. Candidatas a borrar:

| Desde | Hacia | Nota |
|---|---|---|
| `LOL_PBICREDITNOTESSALES[LineUID]` | `LOL_PBIB2BSALESORDER[LineUID]` | 1:1, bidireccional, inactiva |
| `LOL_PBIRETURNREQUESTS[LineUID]` | `LOL_PBIB2BSALESORDER[LineUID]` | inactiva |
| `LOL_PBIRETURNREQUESTS[LineUID]` | `LOL_PBIRETURNSALES[LineUID]` | inactiva |

---

## 4. Relaciones

Salvo indicación, cardinalidad **muchos-a-uno**, dirección de filtro **única**, **activas**.

### 4.1. Hechos → dimensiones

Las seis tablas de hechos cuelgan de las mismas cuatro dimensiones más el calendario (esquema en estrella múltiple):

| Tabla de hechos | → Cliente | → Agente | → Artículo | → Temporada | → Calendario |
|---|---|---|---|---|---|
| `LOL_PBIB2BSALESORDER` | `CardCode` | `SlpCode` | `ItemCode` | `Season` | `DocDate` |
| `VentasIncidencias` | `CodigoCliente` | `CodigoVendedor` | `CodigoArticulo` | `TemporadaLinea` | `FechaDocumento` |
| `LOL_PBISALESDELIVERY` | `CodigoCliente` | `CodigoVendedor` | `CodigoArticulo` | `TemporadaLinea` | `FechaDocumento` |
| `LOL_PBIINVOICESSALES` | `CardCode` | `SlpCode` | `ItemCode` | `Season` | `DocDate` |
| `LOL_PBICREDITNOTESSALES` | `CardCode` | `SlpCode` | `ItemCode` | `Season` | `DocDate` |
| `LOL_PBIRETURNSALES` | `CardCode` | `SlpCode` | `ItemCode` | `Season` | `DocDate` |
| `LOL_PBIRETURNREQUESTS` | `CardCode` | `SlpCode` | `ItemCode` | **`SeasonResuelta`** | *(solo LocalDateTable)* |

Y `TemporadasUsadas[Code]` → `LOL_PBISEASON[Code]`.

### 4.2. Relaciones con tablas de fecha auto-generadas

Decenas de relaciones M:1 hacia `LocalDateTable_*[Date]` con `joinOnDateBehavior: datePartOnly` — una por cada columna de fecha de cada tabla (incluidas las columnas de período de `Calendario`). Son las jerarquías de fecha automáticas; ver §6.

> `ParamTopN`, `OrdenSelector` y `DimensionB2B` **no tienen relaciones**: se leen con `SELECTEDVALUE` (patrón de parámetro).

---

## 5. Medidas (96)

Repartidas en 5 tablas-contenedor.

### 5.1. `MedidasTemporadas` (23) — control de la temporada seleccionada

El "motor" del modelo: resuelve qué temporada está seleccionada, cuál es su anterior/pasada y las fechas de corte que reutiliza todo lo demás.

#### Carpeta `Seleccion`

**`FechaLimiteSeleccionada`** — `D MMM YYYY`
```dax
VAR FechaFiltro = MAX ( 'Calendario'[Date] )
VAR FechaCarga = TODAY ()
RETURN MIN ( FechaFiltro, FechaCarga )
```
Fecha de corte global: el menor entre la fecha máxima visible en el filtro y hoy. Evita contar ventas "futuras" y se usa como `FechaCorte` en casi todas las medidas de venta.

**`TemporadaSeleccionada`** = `SELECTEDVALUE('TemporadasUsadas'[Code])`.
**`TemporadaAnterior`** — `LOL_PBISEASON[CompareSeason]` de la seleccionada.
**`TemporadaAnteriorTotal`** — etiqueta de texto `"<código> Total"`.
**`TemporadaPasada`** — `LOL_PBISEASON[TemporadaAnterior]` de la seleccionada.

#### Carpeta `Cabeceras` (9 textos dinámicos)
`TituloComparativa` (`"T2526 a 30 jun 2026"`), `CabeceraTemporadaActual`, `CabeceraTemporadaAnterior`, `CabeceraTemporadaAnteriorIniciales`, `CabeceraEvolPct`, `CabeceraEvol`, `CabeceraEvolIniciales`, `CabeceraRealizado`, `CabeceraTemporadaPasada`.

#### Carpeta `Validacion` (9)
Auditan que la ventana de "días equivalentes" cuadra: `ValidacionFechaInicioTemporadaSel` (= `MAX(LOL_PBISEASON[Fin])`, ojo), `ValidacionFechaFinTemporadaSel`, `ValidacionFechaInicioTemporadaAnt`, `ValidacionFechaFinTemporadaAnt`, `ValidacionDiasTemporadaSel`, `ValidacionFechaCorteAnt12M` (`EDATE(FechaCorte,-12)`), `ValidacionFechaFinAntDiasEq`, `ValidacionRangoTemporadaAnt` (volcado de texto), `ValidacionVersionRangos` (= `"Rangos por LOL_PBISEASON[Fin] v3"`).

### 5.2. `MedidasVentas` (55)

Patrón común: `SUM` de `ImporteBruto` (o `LineTotal_Num`) con `REMOVEFILTERS('Calendario')` y `REMOVEFILTERS([DocDate])`, tope `DocDate <= FechaCorte`, excluyendo anulados e incidencias.

#### Carpeta `Base` (6)
`VentaBruta`, `VentaBrutaSalesOrder` (alias), `VentaNeta`, `VentaBrutaTotal`, `VentaNetaTotal`, `VentasTemporadaSeleccionada_Bruta`.

```dax
VentaBruta =
VAR FechaCorte = [FechaLimiteSeleccionada]
RETURN CALCULATE (
    SUM ( 'LOL_PBIB2BSALESORDER'[ImporteBruto] ),
    REMOVEFILTERS ( 'Calendario' ),
    REMOVEFILTERS ( 'LOL_PBIB2BSALESORDER'[DocDate] ),
    'LOL_PBIB2BSALESORDER'[Canceled] <> "Y",
    'LOL_PBIB2BSALESORDER'[IsIncidence] <> "Y",
    'LOL_PBIB2BSALESORDER'[DocDate] <= FechaCorte
)
```

#### Carpeta `Temporada` (13)
`VentasTemporadaSeleccionada` (+ alias `…SalesOrder`), `VentasTemporadaAnterior` (ventana de **días equivalentes** desde `MAX(Fin)` de la anterior), `VentasTemporadaAnterior_SoloInicio` (sin reposiciones), `VentasTemporadaAnterior_Total`, `VentasTemporadaPasada_SoloInicio`, `RealizadoTemporadaAnterior_%`.

#### Carpeta `Evolucion` (4)
`EvolTemporadaSeleccionada`, `EvolTemporadaSeleccionada_%`, `EvolTemporadaSeleccionada_Completa` (vs venta inicial de la anterior), `EvolTemporadaSeleccionada_%Completa`.

#### Carpeta `YTD` (6)
`VentasYTD` (`DATESYTD` sobre `LineTotal_Num`), `VentasYTD_SalesOrder` (alias), `VentasYTD_LY` (`SAMEPERIODLASTYEAR`), `EvolVentasYTD`, `EvolVentasYTD_%`.

#### Carpeta `Total` (8)
Variantes que **no recortan por días equivalentes** y **no excluyen anulados/incidencias**: `VentaBrutaTotal`, `VentasTemporadaSeleccionadaTotal`, `VentasTemporadaAnteriorTotal`, `VentasTemporadaAnterior_SoloInicioTotal`, `VentasTemporadaAnterior_TotalTotal`, `EvolTemporadaSeleccionadaTotal`, `EvolTemporadaSeleccionadaTotal_%`, `VentaNetaTotal`. Ver aviso en §6.

#### Carpeta `Validacion` (6)
`ValidacionVentasAnt_Corte12M`, `ValidacionVentasAnt_DiasEq`, `ValidacionVentasAnt_DiasEq_Limpia`, `ValidacionVentasAnt_DiasEq_AllFact`, `ValidacionDifAnt_DiasEq_vs_Corte12M`, `ValidacionLineasAnt_DiasEq`.

#### Carpeta `Servicio Temporada` (21) — **el embudo de cumplimiento**

| Medida | Definición | Qué mide |
|---|---|---|
| `Pedido Original` | `SUM(LOL_PBIB2BSALESORDER[ImporteBruto_LC])` | Todo lo pedido, incluidas anulaciones. **Denominador del embudo.** |
| `Pedido Final` | `CALCULATE(…, IsIncidence = "N")` | Pedido sin incidencias. |
| `Pedidos Cancelados` | `CALCULATE(…, Canceled = "Y")` | Importe anulado. |
| `% Cancelado` | `DIVIDE ( -[Pedidos Cancelados], [Pedido Original] )` | Tasa de anulación (el signo negativo asume importes de anulación negativos). |
| `Importe Servido` | `SUM(LOL_PBISALESDELIVERY[ImporteNetoLineaML])` | Albaranado (suma directa, **sin** cadena). |
| `Servido (cadena)` | `TREATAS` pedido→albarán | Albaranado **de este pedido**. |
| `% Servido` | `DIVIDE([Importe Servido],[Pedido Original])` | — |
| `Pendiente Servir` | `[Pedido Original] - [Importe Servido]` | — |
| `Importe Facturado` | `SUM(LOL_PBIINVOICESSALES[LineTotal_LC])` | Facturado (suma directa). |
| `Facturado (cadena)` | `TREATAS` pedido→albarán→factura | Facturado **de este pedido**. |
| `% Facturado` | `DIVIDE([Importe Facturado],[Pedido Original])` | — |
| `Pendiente Facturar` | `[Pedido Original] - [Importe Facturado]` | — |
| `Importe Notas Credito` | `SUM(LOL_PBICREDITNOTESSALES[LineTotal_LC])` | — |
| `Notas Credito (cadena)` | `TREATAS` por `LineUID` | — |
| `% Notas Credito` | `DIVIDE([Importe Notas Credito],[Importe Facturado])` | — |
| `Importe Devoluciones` | `SUM(LOL_PBIRETURNSALES[LineTotal_LC])` | — |
| `Devoluciones (cadena)` | `TREATAS` por `LineUID` | — |
| `% Devoluciones` | `DIVIDE([Devoluciones (cadena)],[Facturado (cadena)])` | Tasa de devolución sobre lo realmente facturado del pedido. |
| `Notas + Devol. Abiertas (cadena)` | `[Notas Credito (cadena)] +` devoluciones con `LineStatus = "O"` | Impacto negativo total, contando solo las devoluciones aún abiertas. |
| `Importe Solicitudes Devolucion` | `SUM(LOL_PBIRETURNREQUESTS[LineTotal_LC])` | — |
| `Solicitudes Devol. Abiertas` | `CALCULATE(…, LineStatus = "O")` | Devoluciones solicitadas pendientes de tramitar. |

> **`Importe X` vs `X (cadena)`.** Las primeras suman la tabla de destino tal cual (respondiendo al filtro que llegue por las dimensiones compartidas). Las `(cadena)` navegan el enlace documento a documento. Para "de este pedido, ¿cuánto se ha servido?" hay que usar la variante `(cadena)`; las `%` del embudo (`% Servido`, `% Facturado`) usan hoy la variante **directa** (ver §6).

### 5.3. `MedidasIncidencias` (14)
Trabajan sobre `VentasIncidencias` replicando la estructura temporal de ventas.

- **`Base`**: `VentaBrutaIncidencias`, `ImporteNetoIncidencias`, `CantidadIncidencias`.
- **`Control`**: `LineasSalesOrderExcluidasPorIncidencia` (nº de líneas de pedido marcadas como incidencia, para cuadrar).
- **`Temporada`**: `VentasTemporadaSeleccionadaIncidencias`, `VentasTemporadaAnteriorIncidencias`, `…SoloInicio…`, `…Total…`, `VentasTemporadaPasadaSoloInicioIncidencias`.
- **`YTD`**: `VentasYTD_Incidencias`.
- **`Validacion`**: 4 medidas espejo de las de ventas.

### 5.4. `MedidasFormato` (2)
`ColorEvol` y `ColorEvolPct` — hex de formato condicional: verde `#00703C` si ≥ 0, rojo `#C00000` si negativo, gris `#595959` si blank.

### 5.5. `MedidasCatalogo` (2)
`Catálogo Top N` = `SELECTEDVALUE(ParamTopN[Top N], 10)`.

**`HTML Catálogo B2B`** — medida de texto que genera HTML completo (con `<style>` embebido) para un visual *HTML Content*:

> *Catálogo visual de productos B2B (foto, ranking con medallas, badge de marca). Se agrupa por la dimensión elegida en `DimensionB2B`, se ordena por `OrdenSelector` y muestra Top N (`ParamTopN`) por grupo. Ventas = `ImporteBruto` (excluye anulados/incidencias, hasta fecha de corte, respeta la temporada seleccionada).*

Con un `SWITCH` sobre `DimensionB2B[Orden]` (0–9) construye pares (grupo, modelo-color) con ventas y unidades mediante `SUMMARIZE`/`ADDCOLUMNS`, toma los 12 grupos top y dentro de cada grupo los `Top N` modelos, y dibuja una tarjeta con ranking (medalla), barra de progreso y silueta SVG de respaldo si no hay foto. Paleta de marca (rosa `#DFA0C9`, negro, gris).

---

## 6. Notas y observaciones

1. **Medidas "alias" redundantes.** `VentaBrutaSalesOrder = [VentaBruta]`, `VentasTemporadaSeleccionadaSalesOrder = [VentasTemporadaSeleccionada]`, `VentasYTD_SalesOrder = [VentasYTD]`. Duplican superficie de mantenimiento.

2. **Inconsistencia "Total" vs base en el filtrado de anulados/incidencias.** Las medidas de `Base`/`Temporada` excluyen `Canceled="Y"` e `IsIncidence="Y"`; las de `Total` **no**. Como las de **Evolución** se basan en `VentasTemporadaSeleccionadaTotal` (sin filtrar) restando `VentasTemporadaAnterior` (sí filtrada), se compara una venta "sucia" contra otra "limpia". Revisar si es intencional.

3. **El embudo mezcla medidas directas y de cadena.** `% Servido` y `% Facturado` dividen `Importe Servido` / `Importe Facturado` (sumas directas) entre `Pedido Original`. En un contexto filtrado por pedido concreto eso puede no coincidir con lo que devuelven `Servido (cadena)` / `Facturado (cadena)`. Si el objetivo es "% de este pedido", los porcentajes deberían usar las variantes `(cadena)` — como sí hace `% Devoluciones`.

4. **Tres bases de importe conviviendo.** `ImporteBruto` (medidas de temporada), `LineTotal_Num` (neta/YTD) e `ImporteBruto_LC` (embudo). No son intercambiables; al comparar cifras entre páginas hay que saber cuál está debajo.

5. **Datos numéricos almacenados como texto.** En las tablas de documento casi todos los importes/cantidades vienen como `string`. `Qty`, los `PVP_*` y los descuentos se convierten con `VALUE`/`INT`/`Number.From` al calcular, lo que puede ocultar errores de parseo (quedan en 0/blank en silencio).

6. **Lógica de comparación anclada en `LOL_PBISEASON[Fin]`.** El "inicio" de la ventana de días equivalentes usa `MAX(Fin)`, no `Inicio` (sello `ValidacionVersionRangos = "Rangos por LOL_PBISEASON[Fin] v3"`). Es deliberado —hay todo un bloque de validación para auditarlo— pero contraintuitivo. Y hay **dos** definiciones de "anterior": `CompareSeason` (comparación) y `TemporadaAnterior` (pasada); no confundirlas.

7. **Tres relaciones inactivas** entre documentos (§3). Residuos del enfoque por relaciones físicas; conviene borrarlas para que no confundan.

8. **`LOL_PBIRETURNREQUESTS` no está en el `Calendario`.** Sus fechas (`DocDate`, `DocDueDate`) solo cuelgan de `LocalDateTable_*`. Cualquier medida de solicitudes de devolución **ignora el filtro de fecha del informe**. Si se quiere analizarlas por periodo, falta la relación con `Calendario[Date]`.

9. **Time Intelligence automático infla el modelo.** Con una dimensión `Calendario` propia y marcada, las `LocalDateTable_*` son redundantes: desactivar la fecha/hora automática reduce tamaño y elimina jerarquías duplicadas.

10. **Dependencia fuerte de `FechaLimiteSeleccionada`.** Casi todas las medidas de venta dependen de ella (incluye `TODAY()`), así que los resultados **cambian con la fecha de ejecución** y no son reproducibles históricamente sin fijar el calendario.

11. **Página "Duplicado de Página 1" es la activa.** El informe abre por una página cuyo nombre delata que nació como copia. Conviene renombrarla (es la que lleva el embudo completo) y decidir qué pasa con la original.

12. **`cultures/es-ES.tmdl`** solo contiene metadatos lingüísticos de Q&A auto-generados.

---

## 7. Informe

`B2BSalesOrder.Report`, 7 páginas de 1280×720:

| Página | Contenido |
|---|---|
| **Página 1** (20 visuales) | Comparativa de temporadas: tarjetas de venta actual / anterior / evolución, matriz por dimensión y segmentadores. |
| **Análisis Incidencias** (4) | Incidencias frente a venta: `VentaBrutaIncidencias`, `VentasYTD_Incidencias`. |
| **Página 2** (16) | **Catálogo B2B** — visual *HTML Content* con `HTML Catálogo B2B` + segmentadores de dimensión, orden y Top N. |
| **Página 3** (5) | Tablas de detalle (sin medidas: columnas directas). |
| **Página 4** (2) | Auditoría de rangos: `ValidacionVersionRangos`, `VentasTemporadaSeleccionada` vs `VentasTemporadaAnterior`. |
| **Duplicado de Página 1** (20) ← *página activa* | Igual que Página 1 pero con el **embudo**: `Pedido Original`, `Pedido Final`, `% Cancelado`, `Servido (cadena)`, `Pendiente Servir`, `Facturado (cadena)`, `Pendiente Facturar`, `Notas + Devol. Abiertas (cadena)`, `Solicitudes Devol. Abiertas`. |
| **Cumplimiento de Servicio** (7) | Página ejecutiva del embudo: `Pedido Final`, `Servido (cadena)`, `% Servido`, `Pendiente Servir`, `Facturado (cadena)`, `% Facturado`, `Pendiente Facturar`. |

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
