# Modelo semántico: B2BSalesOrder

> Documentación técnica generada a partir del modelo TMDL ubicado en
> [`SalesOrder/B2BSalesOrder.SemanticModel/definition/`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/).
> **Solo lectura**: este documento describe el modelo, no lo modifica.
>
> *Última revisión: 24 de noviembre de 2026.*

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
| Tablas "reales" (visibles/funcionales) | 31 |
| Tablas auto-generadas de fechas | 1 plantilla + 24 `LocalDateTable_*` ocultas |
| Nº de medidas | **262** |
| Orígenes de datos | SQL Server `192.168.0.232`, base `hana_etl_admin`, esquema `dbo` (10 tablas vía M) **+ HANA `192.168.0.231:30015`** (`LOL_PLANMATERIAL`, vía `Value.NativeQuery`) + tablas calculadas DAX |
| RLS / Roles | **Ninguno** |

### Arquitectura

- **Hechos — pedido:** [`LOL_PBIB2BSALESORDER`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIB2BSALESORDER.tmdl) (líneas de pedido B2B). Es la tabla central.
- **Hechos — cadena de documentos:** [`LOL_PBISALESDELIVERY`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBISALESDELIVERY.tmdl) (albaranes), [`LOL_PBIINVOICESSALES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIINVOICESSALES.tmdl) (facturas), [`LOL_PBICREDITNOTESSALES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBICREDITNOTESSALES.tmdl) (notas de crédito), [`LOL_PBIRETURNSALES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIRETURNSALES.tmdl) (devoluciones), [`LOL_PBIRETURNREQUESTS`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIRETURNREQUESTS.tmdl) (solicitudes de devolución).
- **Hechos derivada:** [`VentasIncidencias`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/VentasIncidencias.tmdl) (subconjunto calculado del pedido, solo incidencias).
- **Dimensiones de origen SQL:** [`LOL_PBICLIENTES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBICLIENTES.tmdl), [`LOL_PBIAGENTES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIAGENTES.tmdl), [`LOL_PBIMODELITEM`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIMODELITEM.tmdl), [`LOL_PBISEASON`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBISEASON.tmdl).
- **Dimensiones calculadas nuevas:** [`LOL_INCIDENCIAS`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_INCIDENCIAS.tmdl) (catálogo de motivos de incidencia, `DATATABLE`) y [`PedidosB2B`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/PedidosB2B.tmdl) (números de pedido distintos). Ver §2.4.
- **Dimensión de fechas calculada:** [`Calendario`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/Calendario.tmdl).
- **Cartera viva:** [`LOL_PLANMATERIAL`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PLANMATERIAL.tmdl), importada de HANA. Es la misma tabla que sostiene el modelo *ServicePlan*, traída aquí para poder cerrar el mes y medir el servicio sin salir de este informe.
- **Apoyo / parámetros:** `TemporadasUsadas`, `DimensionB2B`, `DimensionB2B1/2/3` (*field parameters*), `OrdenSelector`, `ParamTopN`, `MesFacturacion`, `MostrarTotalLook`, `UltimaActualizacion`.
- **Contenedores de medidas (sin datos):** `MedidasVentas`, `MedidasTemporadas`, `MedidasIncidencias`, `MedidasFormato`, `MedidasCatalogo`, **`MedidasCartera`**.

> **Qué ha cambiado desde la revisión del 28 de agosto.** El modelo pasa de 174 a **262 medidas** (+88). Lo nuevo:
>
> - **`MedidasIncidencias` pasa de 14 a 58** — carpetas `7 Análisis` (20), `8 Temporada` (10) y `9 Motivos` (14), que sostienen la página nueva **Incidencias** (§5.3).
> - **`MedidasVentas` pasa de 86 a 130** — 30 píldoras de filtro (`0 Pildoras`), 2 de contexto de cabecera (`0 Contexto`) y 12 de la página de obtención de detalles (*drillthrough*) **Detalle de pedido** (`9 Detalle pedido`) (§5.2).
> - **Dos tablas nuevas:** `LOL_INCIDENCIAS` (catálogo de 17 motivos, enlazado por `IncidenceCode`) y `PedidosB2B` (dimensión de pedido que filtra a la vez pedido e incidencias).
> - **`VentasIncidencias`** gana `NumeroPedido`, `IncidenciaMotivo` e `IncidenciaTipoCat`; el código de incidencia sale ahora de `IncidenceCode`, no de `RMAMotiveCod`.
> - **`DimensionB2B1/2/3`** añaden `Año`(11), `Mes`(12) y `Semana`(13); `Pedido`(10) apunta ya a `PedidosB2B[NumeroPedido]`.
> - **Catálogo HTML por dos dimensiones**: sección (`DimensionB2B1`) y subnivel (`DimensionB2B2`), con cabeceras fijas (§5.5).
> - **Informe:** 10 páginas (antes 8), menú adaptable con dos juegos de visuales por página, tema `CY26SU05`.
>
> Revisión anterior (96 → 174): `MedidasCartera` (cierre de mes y Total Look sobre `LOL_PLANMATERIAL`), 31 medidas de `MedidasVentas`, los *field parameters* y las columnas `Total Look`, `Código Total Look` y `EsMuestra`.

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
| `IncidenceCode` | Código de motivo de incidencia (texto) → `LOL_INCIDENCIAS[cod_texto]`. Nuevo. |
| `DocNum` | Número de pedido visible → `PedidosB2B[NumeroPedido]`. |
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
    "NumeroPedido", 'LOL_PBIB2BSALESORDER'[DocNum],
    "LineNum", 'LOL_PBIB2BSALESORDER'[LineNum],
    "IncidenciaCode", 'LOL_PBIB2BSALESORDER'[IncidenceCode],
    "IncidenciaMotivo", COALESCE (
        LOOKUPVALUE ( LOL_INCIDENCIAS[motivo], LOL_INCIDENCIAS[cod_texto], 'LOL_PBIB2BSALESORDER'[IncidenceCode] ),
        COALESCE (
            LOOKUPVALUE ( LOL_INCIDENCIAS[tipo_incidencia], LOL_INCIDENCIAS[cod_texto], 'LOL_PBIB2BSALESORDER'[IncidenceCode] ),
            'LOL_PBIB2BSALESORDER'[IncidenceType],
            "Sin motivo" ) ),
    "IncidenciaTipoCat", LOOKUPVALUE ( LOL_INCIDENCIAS[tipo_incidencia], LOL_INCIDENCIAS[cod_texto], 'LOL_PBIB2BSALESORDER'[IncidenceCode] ),
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

`IncidenciaMotivo` es el texto del motivo según `LOL_INCIDENCIAS`; si el código no está catalogado cae al tipo, luego a `IncidenceType` y por último a `"Sin motivo"`, en vez de salir en blanco. `NumeroPedido` es el enlace con `PedidosB2B`.

> Es una **tabla calculada**: la relación con `PedidosB2B` le llega por su propia columna `NumeroPedido`, no a través de `LOL_PBIB2BSALESORDER`.

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

**`Calendario`** — Tabla calculada `CALENDAR` desde `2022-01-01` hasta el `31/12` de (año actual + 2), enriquecida con `FechaKey`, `AñoMesKey`, `Año`, `Mes`, `MesTexto`, `Trimestre`, `Semana`, `InicioMes`/`FinMes`, `InicioTrimestre`/`FinTrimestre`, `InicioAño`/`FinAño` y flags `EsHoy`, `EsPasado`, `EsFuturo`, `EsMesActual`, `EsAñoActual`. Marcada como dimensión de fecha. `NombreSemana` (`"Semana " & FORMAT(WEEKNUM([Date],2),"00")`) es la que usan los *field parameters* para la opción `Semana`.

**`LOL_INCIDENCIAS`** — *Nueva.* Catálogo de motivos de incidencia: tabla calculada `DATATABLE` con 17 códigos (1–5, 7–11, 21–27), cada uno con `motivo`, `descripcion_motivo`, `tipo_incidencia` (`CANCELACION` / `MODIFICACION`), `tipo_objeto_sap` (17 = pedido) y `visible_bi2go` (Y/N). Las `|` de la descripción se convierten en saltos de línea.

| Columna | Nota |
|---|---|
| `cod_incidencia` | Entero. |
| `cod_texto` | `FORMAT(cod_incidencia, "0")`. Existe porque `IncidenceCode` llega como **texto** en el pedido y una relación exige el mismo tipo en los dos lados. Se convierte aquí (17 filas) y no en el hecho (~1,9 M filas). |

**`PedidosB2B`** — *Nueva.* `DISTINCT(SELECTCOLUMNS(LOL_PBIB2BSALESORDER, "NumeroPedido", [DocNum]))`. Es una **dimensión de pedido**: cuelga de `LOL_PBIB2BSALESORDER[DocNum]` y de `VentasIncidencias[NumeroPedido]`, de modo que un filtro de pedido alcanza a las dos tablas. Filtrar el hecho directamente no llegaba a las incidencias.

### 2.5. Tablas de apoyo y parámetros

| Tabla | Tipo / Origen | Propósito |
|---|---|---|
| `TemporadasUsadas` | Calculada: `DISTINCT(SELECTCOLUMNS('LOL_PBISEASON',"Code",…))` | Selector de temporada. `TemporadaSeleccionada` lee `SELECTEDVALUE` de su `Code`. **Ahora relacionada** con `LOL_PBISEASON[Code]`. |
| `ParamTopN` | `GENERATESERIES(5, 50, 5)` | Parámetro "Top N" del catálogo. |
| `OrdenSelector` | `DATATABLE` | Criterio de orden del catálogo: `Ventas (€)` (1), `Unidades` (2), `PVP` (3). |
| `DimensionB2B` | `DATATABLE` | Dimensión de agrupación del catálogo: Categoría(0), Familia agregada(1), Familia(2), Subfamilia(3), Marca(4), Cliente(5), Grupo cliente(6), País(7), Agente(8), Temporada(9). **Ya no tiene segmentador en ninguna página**; se conserva solo como respaldo de `HTML Catálogo B2B`. |
| `DimensionB2B1` / `2` / `3` | *Field parameters* (`NAMEOF`) | Los tres selectores de desglose del informe. Mismos índices 0–9 que `DimensionB2B`, **más `Pedido`(10) → `PedidosB2B[NumeroPedido]`, `Año`(11) → `Calendario[Año]`, `Mes`(12) → `Calendario[MesTexto]` y `Semana`(13) → `Calendario[NombreSemana]`**. La columna de orden se llama `DimensionB2B1[DimensionB2B1 Orden]`, no `[Orden]`. En el catálogo HTML, `DimensionB2B1` fija la sección y `DimensionB2B2` el subnivel (§5.5). |
| `MesFacturacion` | Calculada sobre `Calendario` | Meses con `Inicio`/`Fin` (`EOMONTH`). **Ya no la lee ninguna medida**: el mes de cierre se deduce de la fecha de disponibilidad (§5.6). Solo la usa un visual de la página oculta *Previsión Mes* (copia antigua). `Inicio`/`Fin` tienen ahora su propia `LocalDateTable_*`. |
| `MostrarTotalLook` | `DATATABLE` | Interruptor Todo / Solo Total Look / Sin Total Look. Ver §5.6. |
| `UltimaActualizacion` | Calculada | Sello de hora del último refresco. |

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

**Ya no existen.** Las tres relaciones inactivas por `LineUID` (notas de crédito, solicitudes y devoluciones contra el pedido) se borraron: `LineUID` = `DocEntry & LineNum` sin separador colisiona y sus duplicados rompían la actualización. Hoy `relationships.tmdl` no tiene ninguna relación con `isActive: false`.

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

Y además:

| Desde | Hacia | Nota |
|---|---|---|
| `TemporadasUsadas[Code]` | `LOL_PBISEASON[Code]` | — |
| `LOL_PBIB2BSALESORDER[KPedidoLinea]` | `LOL_PLANMATERIAL[KLinea]` | **Bidireccional**. Enlaza la cartera viva con la línea de pedido. |
| `LOL_PBIB2BSALESORDER[IncidenceCode]` | `LOL_INCIDENCIAS[cod_texto]` | *Nueva.* Texto contra texto (§2.4). |
| `LOL_PBIB2BSALESORDER[DocNum]` | `PedidosB2B[NumeroPedido]` | *Nueva.* |
| `VentasIncidencias[NumeroPedido]` | `PedidosB2B[NumeroPedido]` | *Nueva.* Hace que el filtro de pedido llegue a las incidencias. |

### 4.2. Relaciones con tablas de fecha auto-generadas

Decenas de relaciones M:1 hacia `LocalDateTable_*[Date]` con `joinOnDateBehavior: datePartOnly` — una por cada columna de fecha de cada tabla (incluidas las columnas de período de `Calendario`). Son las jerarquías de fecha automáticas; ver §6.

> `ParamTopN`, `OrdenSelector` y `DimensionB2B` **no tienen relaciones**: se leen con `SELECTEDVALUE` (patrón de parámetro).

---

## 5. Medidas (262)

Repartidas en 6 tablas-contenedor.

| Tabla | Medidas |
|---|---:|
| `MedidasVentas` | 130 |
| `MedidasIncidencias` | 58 |
| `MedidasCartera` | 47 |
| `MedidasTemporadas` | 23 |
| `MedidasCatalogo` | 2 |
| `MedidasFormato` | 2 |

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

### 5.2. `MedidasVentas` (130)

Patrón común: `SUM` de `ImporteBruto` (o `LineTotal_Num`) con `REMOVEFILTERS('Calendario')` y `REMOVEFILTERS([DocDate])`, tope `DocDate <= FechaCorte`, excluyendo anulados e incidencias.

#### Carpeta `Base` (3)
`VentaBruta`, `VentaBrutaSalesOrder` (alias), `VentaNeta`. `VentaBrutaTotal` y `VentaNetaTotal` viven en la carpeta `Total`, y `VentasTemporadaSeleccionada_Bruta` en `Temporada`.

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

#### Carpeta `Temporada` (8)
`VentasTemporadaSeleccionada` (+ alias `…SalesOrder`), `VentasTemporadaAnterior` (ventana de **días equivalentes** desde `MAX(Fin)` de la anterior), `VentasTemporadaAnterior_SoloInicio` (sin reposiciones), `VentasTemporadaAnterior_Total`, `VentasTemporadaPasada_SoloInicio`, `RealizadoTemporadaAnterior_%`.

#### Carpeta `Evolucion` (4)
`EvolTemporadaSeleccionada`, `EvolTemporadaSeleccionada_%`, `EvolTemporadaSeleccionada_Completa` (vs venta inicial de la anterior), `EvolTemporadaSeleccionada_%Completa`.

#### Carpeta `YTD` (5)
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

#### Carpeta `Temporada KPI` (26)

Los KPI de cabecera del informe. Cada uno con su gemelo `… anterior` y su `Evol … %`:

| Bloque | Medidas |
|---|---|
| Volumen | `Pedidos temporada`, `Clientes temporada`, `Unidades temporada`, `Líneas temporada`, `Modelos temporada` |
| Medias | `Pedido medio`, `Venta por cliente`, `Unidades por pedido`, `Precio medio unidad`, `Líneas por pedido`, `Venta por modelo`, `Unidades por modelo` |
| Evolución | `Evol pedidos %`, `Evol clientes %`, `Evol unidades %`, `Evol pedido medio %`, `Evol modelos %`, `Evol venta por cliente %`, `Evol precio medio %` |

`Modelos temporada` cuenta **`Referencia_modelo`**, no `ItemCode`: un modelo, no una talla. Necesita `CROSSFILTER(…, BOTH)` para que el recuento de la dimensión responda al filtro del hecho.

#### Carpeta `Muestras` (3)

`Venta sin muestras`, `Venta de muestras`, `% Muestras sobre total`. Se apoyan en la columna calculada `EsMuestra` (Y/N), que marca la fila cuando la razón social del cliente contiene `MUESTR` o `SMS`, **o** cuando el almacén es `4-C002L`.

> El almacén se identificó empíricamente: el 96,5% de su volumen viene de clientes con `MUESTR` en el nombre. Y `SMS` deja un falso positivo conocido, *Sms SRL*, que es una empresa B2B real (unos 4.052 €).

#### Carpeta `Actualización` (2)

`Última actualización` y `Marca de actualización`, para el sello de hora de la cabecera.

#### Carpeta `0 Pildoras` (30) — *nueva*

Píldoras de filtro activo: 15 parejas `Chip <campo>` / `Chip <campo> Color`. La primera devuelve el valor elegido (o `"n sel."`) seguido de `✕`, y blank si el campo no está filtrado; la segunda, el color de fondo (rosa `#DFA0C9` si filtra). Campos: `Agente_Cajero`, `Autorizado`, `Bloqueado`, `Categoria`, `Code`, `Descripcion_familia_agregada`, `EsMuestra`, `Fecha Fin pedido venta`, `Fecha disponibilidad (línea del articulo)`, `GrupodeArt`, `Nombre_familia`, `P&H`, `Subfamilia`, `TipoTienda`, `TotalLook`.

```dax
Chip Categoria =
VAR n = COUNTROWS ( VALUES ( LOL_PBIMODELITEM[Categoria] ) )
VAR txt = IF ( n = 1, SELECTEDVALUE ( LOL_PBIMODELITEM[Categoria] ), n & " sel." )
RETURN IF ( ISFILTERED ( LOL_PBIMODELITEM[Categoria] ), txt & "  ✕" )
```

#### Carpeta `0 Contexto` (2) — *nueva*

`Rango de fechas` (`MIN`–`MAX` de `Calendario[Date]` en `dd/MM/yyyy`, o `"Sin fechas"`) y `Rango y actualización` (= rango · `[Última actualización]` · `[Aviso mes de cierre]`), que alimenta la tarjeta de cabecera de cada página.

#### Carpeta `9 Detalle pedido` (12) — *nueva*

Para la página de *drillthrough* **Detalle de pedido** (§7): `Pedido Líneas`, `Pedido Referencias`, `Pedido Unidades`, `Pedido Servido`, `Pedido Pendiente` (`SUMX` de `OpenQty`), `Pedido % servido` (= servido / unidades), `Pedido Importe`, `Pedido Precio medio`, `Pedido Descripción`, `Pedido Estado` (`DocStatus` traducido: `O` → Abierto, `C` → Cerrado, varios → "Varios"), `Pedido Cabecera` (texto "Pedido N · cliente · fecha", o "Selecciona un pedido") y **`HTML Cabecera Pedido`**, la ficha HTML de cabecera con chip de estado (verde si cerrado, ámbar si abierto). Todas leen `LOL_PBIB2BSALESORDER[DocNum]` con `SELECTEDVALUE`.

### 5.3. `MedidasIncidencias` (58)
Trabajan sobre `VentasIncidencias` replicando la estructura temporal de ventas.

- **`Base`**: `VentaBrutaIncidencias`, `ImporteNetoIncidencias`, `CantidadIncidencias`.
- **`Control`**: `LineasSalesOrderExcluidasPorIncidencia` (nº de líneas de pedido marcadas como incidencia, para cuadrar).
- **`Temporada`**: `VentasTemporadaSeleccionadaIncidencias`, `VentasTemporadaAnteriorIncidencias`, `…SoloInicio…`, `…Total…`, `VentasTemporadaPasadaSoloInicioIncidencias`.
- **`YTD`**: `VentasYTD_Incidencias`.
- **`Validacion`**: 4 medidas espejo de las de ventas.

Nuevas, para la página **Incidencias**:

| Carpeta | Nº | Medidas |
|---|---:|---|
| `7 Análisis` | 20 | Recuentos sobre `VentasIncidencias`: `Líneas de incidencia`, `Tipos de incidencia`, `Motivos con incidencia`, `Clientes / Referencias / Agentes con incidencia`, `Importe medio por incidencia`, `Unidades por incidencia`, `% incidencia sobre venta` (= `VentaBrutaIncidencias` / `VentaBruta`), `Incidencias sin catalogar`. Y la parte posventa: `Solicitudes de devolución`, `Líneas solicitadas`, `Unidades solicitadas` (`LOL_PBIRETURNREQUESTS`), `Devoluciones`, `Unidades devueltas`, `Importe devuelto`, `% devuelto sobre venta` (`LOL_PBIRETURNSALES`), `Abonos`, `Importe abonado` (`SUM(LOL_PBICREDITNOTESSALES[NetAmt_EUR])`), `% solicitud que acaba en devolución` (unidades devueltas / solicitadas). |
| `8 Temporada` | 10 | `Incidencias temporada` / `… anterior` (líneas), `Importe incidencias temporada` / `… anterior`, `Unidades incidencias temporada` / `… anterior`, `Evol incidencias €`, `Evol incidencias %` (blank si la anterior es 0, para no enseñar un infinito), `% incidencia temporada` / `… anterior`. Mismo patrón que ventas: `REMOVEFILTERS('Calendario')`, `TemporadaLinea = [TemporadaSeleccionada]`, `FechaDocumento <= [FechaLimiteSeleccionada]`. |
| `9 Motivos` | 14 | `Pedido bruto temporada`, `% cancelado temporada`, 11 medidas `Mot. <motivo>` y `HTML Tarjetas Incidencias`. |

**`% cancelado temporada`** se calcula sobre el pedido **bruto**, no sobre lo vivo:

```dax
Pedido bruto temporada = [VentasTemporadaSeleccionada] + [Importe incidencias temporada]
% cancelado temporada  = DIVIDE ( [Importe incidencias temporada], [Pedido bruto temporada] )
```

`VentasTemporadaSeleccionada` excluye las líneas de incidencia, así que sola no sirve de denominador: contra lo vivo, un pedido anulado entero daba 21.685%.

**`Mot. <motivo>`** = `CALCULATE([Importe incidencias temporada], VentasIncidencias[IncidenciaMotivo] = "<MOTIVO>")`. Hay una por motivo (Anulación cliente, Cierre tienda, Crédito insuficiente, No llega a mínimos de producción, Fuera de plazo de entrega, Rotura stock, Calidad, Motivo comercial, No conforme con política de imagen, Mínimos de compras no alcanzados, Sin motivo) porque la matriz necesita los motivos como **columnas** junto a las columnas fijas, cosa que no permite un campo en *Columnas*.

**`HTML Tarjetas Incidencias`** — franja de tarjetas en HTML (importe, líneas, unidades, % cancelado, evolución, importe medio). La evolución usa la lógica inversa a ventas: que las incidencias bajen es bueno y sale en verde.

### 5.4. `MedidasFormato` (2)
`ColorEvol` y `ColorEvolPct` — hex de formato condicional: verde `#00703C` si ≥ 0, rojo `#C00000` si negativo, gris `#595959` si blank.

### 5.5. `MedidasCatalogo` (2)
`Catálogo Top N` = `SELECTEDVALUE(ParamTopN[Top N], 10)`.

**`HTML Catálogo B2B`** — medida de texto que genera HTML completo (con `<style>` embebido) para un visual *HTML Content*:

> *Catálogo visual de productos B2B (foto, ranking con medallas, badge de marca). Se agrupa por la dimensión elegida, se ordena por `OrdenSelector` y muestra Top N (`ParamTopN`) por grupo. Ventas = `ImporteBruto` (excluye anulados/incidencias, hasta fecha de corte, respeta la temporada seleccionada).*

El índice de agrupación sale de **`DimensionB2B1`**, que es el que tiene segmentador en las páginas:

```dax
VAR _ix = COALESCE ( SELECTEDVALUE ( DimensionB2B1[DimensionB2B1 Orden] ),
                     SELECTEDVALUE ( DimensionB2B[Orden] ), 0 )
```

Antes leía solo `DimensionB2B`, que no tiene segmentador en ninguna de las tres páginas donde vive el catálogo: el HTML salía siempre agrupado por Categoría se moviera lo que se moviera. `DimensionB2B` queda como respaldo.

#### Dos niveles: sección y subnivel

Desde a8bd3f0 el catálogo admite **dos dimensiones**: `DimensionB2B1` elige la **sección** y `DimensionB2B2` el **subnivel**.

```dax
VAR _ix2     = SELECTEDVALUE ( DimensionB2B2[DimensionB2B2 Orden] )
VAR _unNivel = ISBLANK ( _ix2 ) || _ix2 = _ix      -- colapsa a un solo nivel
```

Si la segunda está vacía o repite la primera, el catálogo vuelve a un nivel (`@g2 = ""`). El `UNION` tiene ahora **121 ramas**: 11 de un nivel (`_ix = k && _unNivel`) y 110 de pares (`_ix = i && _ix2 = j`, i ≠ j, 0–10).

> ⚠️ El `UNION` tiene **una rama por índice (y por pareja)**. Si se añade una entrada al *field parameter* sin añadir sus ramas, esa opción deja el catálogo vacío con *"Sin ventas para los filtros actuales"*. Pasó con `Pedido` (índice 10) y **vuelve a pasar con `Año`(11), `Mes`(12) y `Semana`(13)**: están en los tres *field parameters* pero el catálogo no tiene ramas para ellos.

Para no recorrer dos veces la tabla cara, el cálculo va en dos pasadas: `GROUPBY` por (sección, subnivel) para totales y recuento de referencias; se quedan las **12 secciones** con más venta y, dentro de cada una, los **8 subniveles** mayores; solo para esas celdas se agrupa el detalle por modelo-color y se cruza (una sola vez) con la ficha del artículo (`NATURALLEFTOUTERJOIN`). En cada celda se pintan los `Top N` modelos con ranking (medalla), barra de progreso y silueta SVG de respaldo si no hay foto. Paleta de marca (rosa `#DFA0C9`, negro, gris).

**Presentación** (alineada con el catálogo de SgiRetail, 16cc8d2): fondo `#F2F3F5`, tipografía Manrope, secciones y subniveles plegables (`<details>`), cabecera de nivel 1 con el **recuento de referencias** (`n ref.`) e importe en píldora blanca, y **cabeceras fijas** al hacer scroll (`position: sticky`, la sección en `top: 0`, el subnivel en `top: 36px`, cada una con su `z-index`).

---

### 5.6. `MedidasCartera` (47) — cierre de mes y Total Look

Tabla-contenedor nueva, construida sobre `LOL_PLANMATERIAL` (la cartera viva importada de HANA). Cubre lo que antes no tenía sitio: cuánto se va a cerrar este mes, qué se puede servir y qué parte es Total Look.

| Carpeta | Nº | Contenido |
|---|---:|---|
| `Cierre del mes` | 18 | El mes de cierre, sus componentes y los avisos. |
| `Checks de filtro` | 14 | Un check por segmentador activo, para el panel de filtros. |
| `Cartera facturable` | 7 | `Cartera pendiente`, `en picking`, `asignada stock`, `en transito`, `Importe facturable`, `no facturable`, `% Facturable`. |
| `Total Look` | 6 | Recuentos del criterio (ver más abajo). |
| `Diagnostico` | 2 | `Filtros aplicados` (texto multilínea) y `Filtros activos` (recuento). |

#### El mes de cierre es automático

No hay segmentador de mes. El mes se deduce del día elegido en **Hasta Fecha Disponibilidad Stock**:

```dax
Fecha disponibilidad seleccionada =
VAR Tope =
    IF ( ISFILTERED ( LOL_PLANMATERIAL[Fecha disponibilidad (línea del articulo)] ),
         CALCULATE ( MAX ( LOL_PLANMATERIAL[Fecha disponibilidad (línea del articulo)] ),
                     ALLSELECTED ( LOL_PLANMATERIAL[Fecha disponibilidad (línea del articulo)] ) ) )
RETURN COALESCE ( Tope, TODAY () )
```

`Inicio mes de cierre` / `Fin mes de cierre` son el `EOMONTH` de esa fecha. La red de `TODAY()` importa: sin ella el tope sería el máximo del dato y el panel abriría en septiembre en vez del mes en curso.

> ⚠️ **La columna es muy dispersa**: solo 28 fechas distintas en unas 372.000 líneas, y tres son filas sueltas de una sola línea (11/03/2024, 28/07/2025, 04/03/2026). DAX no puede leer el tope literal de un segmentador, solo el último valor que *existe* por debajo. Del 26/06/2026 en adelante funciona exacto; apuntando entre el 1 y el 25 de junio salta a marzo por esa fila basura. Se arregla limpiando el origen.

`Cierre previsto del mes` = `Facturado del mes` + `Servido sin facturar` + `Cartera que vence en el mes`.

`Adelanto entrega` mide la cartera facturable cuya **fecha fin de servicio va más allá del mes de cierre**. Da 0 cuando el corte de servicio para justo en el fin de mes, que es lo normal; por eso existen `Aviso mes de cierre` y `Aviso adelanto entrega`, que devuelven `BLANK()` si la fecha de servicio es la de hoy y, si no, explican **por qué** sale cero.

> **Corregido:** `Facturado del mes` restaba las notas de crédito de **toda la historia**. `[Importe Facturado]` es `SUM(facturas) − SUM(notas)` y el `CALCULATE` solo acotaba `LOL_PBIINVOICESSALES[DocDate]`. Agosto 2026 daba **−2.602.130,54 €** en vez de 2.801.612,26 €. Ahora se acotan las dos tablas. Es el mismo patrón que ya mordió en `Facturado (cadena)`: **en una medida neta hay que filtrar los dos lados**.

#### Total Look

`U_GSP_LOLTOTALLOOK` agrupa **referencias de modelo**, no tallas: 299 looks sobre 732 referencias, frente a 8.949 sueltas. Cada look reúne de 1 a 5 referencias (media 2,45).

Hay dos fuentes y **no son intercambiables**:

| Fuente | Qué es | Cobertura |
|---|---|---|
| `LOL_PBIMODELITEM[U_GSP_LOLTOTALLOOK]` | el **código** del look | todas las tablas de hechos, vía `ItemCode` |
| `LOL_PLANMATERIAL[TotalLook]` | un **Y/N** ya calculado | solo la cartera viva: ~20% de las líneas de pedido |

Coinciden al 100% donde ambas existen, pero filtrar el histórico por el Y/N del plan tiraría el 79% de los datos. Por eso las columnas calculadas van sobre el artículo:

```dax
Total Look = IF ( LEN ( TRIM ( LOL_PBIMODELITEM[U_GSP_LOLTOTALLOOK] ) ) > 0, "Total Look", "Prenda suelta" )
```

El `LEN(TRIM(...))` no es adorno: el campo viene vacío de dos formas, BLANK y cadena vacía.

El interruptor `MostrarTotalLook[Mostrar]` está cableado **solo dentro de `Cartera asignada stock` y `Cartera en transito`** — no toca lo facturado ni lo pendiente de facturar, que son hechos consumados. Efecto lateral a tener presente: `Importe facturable` = picking + asignado + tránsito queda filtrado en 2 de 3 sumandos, y `% Facturable` es numerador filtrado sobre denominador sin filtrar.

Dos trampas que costaron una vuelta cada una:

- Una **medida-puerta 1/0** en el panel de filtros del visual solo esconde **filas**; no recalcula totales. En una tabla donde cada cliente tiene artículos de los dos tipos no esconde ninguna. Para que los importes se muevan, el parámetro tiene que entrar **dentro** de la medida.
- En la rama **"Todo" no se puede aplicar un filtro permisivo** sobre la columna del artículo: cualquier filtro obliga a pasar por `LOL_PBIMODELITEM` y tira las **7.054 líneas del plan sin línea de pedido (222.615,61 €)**. Esa rama va sin `CALCULATE`. Por eso "Solo" + "Sin" nunca suman "Todo".

> **Ojo con el origen.** La consulta HANA de `LOL_PLANMATERIAL` sí calcula una regla de tallas: los CTE `DESTALLADOS` y `TOTAL_LOOK` derivan `StockTodasTallas` por (`DocEntry`, modelo-color) y por (`DocEntry`, grupo de look), y alimentan la columna `Lanzamiento Picking`. Esa es la servibilidad real, y se decide **en el origen, no en DAX**. La columna `TotalLook` que llega al modelo es solo el Y/N.

---

## 6. Notas y observaciones

1. **Medidas "alias" redundantes.** `VentaBrutaSalesOrder = [VentaBruta]`, `VentasTemporadaSeleccionadaSalesOrder = [VentasTemporadaSeleccionada]`, `VentasYTD_SalesOrder = [VentasYTD]`. Duplican superficie de mantenimiento.

2. **Inconsistencia "Total" vs base en el filtrado de anulados/incidencias.** Las medidas de `Base`/`Temporada` excluyen `Canceled="Y"` e `IsIncidence="Y"`; las de `Total` **no**. Como las de **Evolución** se basan en `VentasTemporadaSeleccionadaTotal` (sin filtrar) restando `VentasTemporadaAnterior` (sí filtrada), se compara una venta "sucia" contra otra "limpia". Revisar si es intencional.

3. **El embudo mezcla medidas directas y de cadena.** `% Servido` y `% Facturado` dividen `Importe Servido` / `Importe Facturado` (sumas directas) entre `Pedido Original`. En un contexto filtrado por pedido concreto eso puede no coincidir con lo que devuelven `Servido (cadena)` / `Facturado (cadena)`. Si el objetivo es "% de este pedido", los porcentajes deberían usar las variantes `(cadena)` — como sí hace `% Devoluciones`.

4. **Tres bases de importe conviviendo.** `ImporteBruto` (medidas de temporada), `LineTotal_Num` (neta/YTD) e `ImporteBruto_LC` (embudo). No son intercambiables; al comparar cifras entre páginas hay que saber cuál está debajo.

5. **Datos numéricos almacenados como texto.** En las tablas de documento casi todos los importes/cantidades vienen como `string`. `Qty`, los `PVP_*` y los descuentos se convierten con `VALUE`/`INT`/`Number.From` al calcular, lo que puede ocultar errores de parseo (quedan en 0/blank en silencio).

6. **Lógica de comparación anclada en `LOL_PBISEASON[Fin]`.** El "inicio" de la ventana de días equivalentes usa `MAX(Fin)`, no `Inicio` (sello `ValidacionVersionRangos = "Rangos por LOL_PBISEASON[Fin] v3"`). Es deliberado —hay todo un bloque de validación para auditarlo— pero contraintuitivo. Y hay **dos** definiciones de "anterior": `CompareSeason` (comparación) y `TemporadaAnterior` (pasada); no confundirlas.

7. ~~Tres relaciones inactivas entre documentos.~~ **Resuelto**: ya no hay relaciones inactivas (§3).

8. **`LOL_PBIRETURNREQUESTS` no está en el `Calendario`.** Sus fechas (`DocDate`, `DocDueDate`) solo cuelgan de `LocalDateTable_*`. Cualquier medida de solicitudes de devolución **ignora el filtro de fecha del informe**. Si se quiere analizarlas por periodo, falta la relación con `Calendario[Date]`.

9. **Time Intelligence automático infla el modelo.** Con una dimensión `Calendario` propia y marcada, las `LocalDateTable_*` son redundantes: desactivar la fecha/hora automática reduce tamaño y elimina jerarquías duplicadas.

10. **Dependencia fuerte de `FechaLimiteSeleccionada`.** Casi todas las medidas de venta dependen de ella (incluye `TODAY()`), así que los resultados **cambian con la fecha de ejecución** y no son reproducibles históricamente sin fijar el calendario.

11. **Páginas duplicadas ocultas.** El informe abre ya por **Análisis Temporada** (`activePageName`). Las copias antiguas (*Anslisis Temporada*, *Rankings*, *Analisis de Servicio* y la primera *Previsión Mes*) siguen en el informe con `HiddenInViewMode`; son candidatas a borrar.

12. **`cultures/es-ES.tmdl`** solo contiene metadatos lingüísticos de Q&A auto-generados.

13. **`Incidencias sin catalogar` siempre da 0.** Cuenta las filas cuyo `IncidenciaMotivo` contiene `"Sin catalogar"`, pero la columna nunca produce ese texto: si el código no está en `LOL_INCIDENCIAS` cae al tipo, a `IncidenceType` o a `"Sin motivo"` (§2.3). Su comentario dice que hoy faltan los códigos 20, 16, 28, 15, 18 y 17; la medida no lo detectará hasta que la columna y la medida usen el mismo literal. Mientras, `Mot. Sin motivo` es la mejor pista.

14. **El catálogo no cubre `Año`, `Mes` ni `Semana`** (índices 11–13 de `DimensionB2B1/2`): elegirlos deja el HTML vacío (§5.5).

15. **El *drillthrough* de Detalle de pedido está ligado a `LOL_PBIB2BSALESORDER[DocNum]`**, mientras que la opción `Pedido` de los *field parameters* apunta ya a `PedidosB2B[NumeroPedido]`. Desde un visual que use la columna de `PedidosB2B` el botón de obtener detalles puede no ofrecerse; conviene comprobarlo o enlazar la página a `PedidosB2B[NumeroPedido]`.

16. **Carpeta duplicada `pages/pages/` en el informe.** Desde b363202 hay en `B2BSalesOrder.Report/definition/pages/pages/` una copia anidada de las 10 páginas (374 ficheros) que **ya no coincide** con las reales. No forma parte del informe vivo; conviene borrarla para que nadie edite la copia equivocada.

---

## 7. Informe

`B2BSalesOrder.Report`, 10 páginas de 1280x720 (tema base `CY26SU05` + tema propio `LolaCasademunt.json`):

| # | Página | Visible | Visuales | Contenido |
|---:|---|:---:|---:|---|
| 1 | Anslisis Temporada | oculta | 35 | Copia antigua de la comparativa. |
| 2 | Rankings | oculta | 23 | Catálogo B2B antiguo. |
| 3 | Analisis de Servicio | oculta | 13 | Embudo ejecutivo: `Pedido Final`, `Servido (cadena)`, `% Servido`, `Facturado (cadena)`, `Pendiente Facturar`. |
| 4 | Previsión Mes | oculta | 34 | Copia antigua, conserva el segmentador de Mes (`MesFacturacion`). Candidata a borrar. |
| 5 | **Análisis Temporada** *(activa)* | sí | 54 | Comparativa de temporadas con los tres *field parameters*. Es la página por la que abre el informe. |
| 6 | Análisis Servicio por Temporada | sí | 52 | Servicio por temporada + catálogo. |
| 7 | Previsión Mes | sí | 62 | Cierre de mes: cartera por cliente, tarjetas de cierre y los dos avisos. |
| 8 | RankingTemporadas | sí | 47 | Catálogo HTML por dos niveles (`DimensionB2B1` sección, `DimensionB2B2` subnivel), con `Top N`, `Orden` y `MostrarTotalLook`. |
| 9 | **Incidencias** *(nueva)* | sí | 40 | Franja `HTML Tarjetas Incidencias` + matriz de cancelado por motivo (`Pedido bruto temporada`, `Importe incidencias temporada`, `% cancelado temporada` y las `Mot. …` como columnas), desglosable por agente, cliente o marca. |
| 10 | **Detalle de pedido** *(nueva)* | *drillthrough* | 3 | Página de obtención de detalles sobre `LOL_PBIB2BSALESORDER[DocNum]`: `HTML Cabecera Pedido`, matriz por modelo-color / artículo (unidades, servido, pendiente…) y botón Volver. |

Las páginas visibles llevan en la cabecera la tarjeta `Rango y actualización` y la fila de píldoras de filtro (`0 Pildoras`).

> **Menú adaptable (1da2362).** Cada página visible tiene **dos juegos de visuales** —menú abierto y menú cerrado— que se alternan por marcador; por eso el número de visuales casi se duplica respecto a la revisión anterior. Al tocar un visual hay que tocar su gemelo.

> ⚠️ Siguen existiendo **dos páginas llamadas "Previsión Mes"** (4 y 7). La 7 es la viva; la 4 es el duplicado del que salió y está oculta.

### Segmentadores de fecha: no los congeles

Un segmentador de fecha guarda su estado en **dos sitios**, y solo uno filtra:

| Dónde | Qué es |
|---|---|
| `visual.objects.data[].properties.endDate` | el **estado visual** del control |
| `visual.objects.general[].properties.filter.filter.Where[]` | el **filtro efectivo**: `Comparison` con `ComparisonKind: 3` contra un literal `datetime'...'` |

Borrar solo el `endDate` no descongela nada. Y las dos fechas ni coinciden: *"Before 26/08"* se guarda como `endDate` = 26/08 pero el filtro dice `< 27/08`. `filterConfig` sale `null`: ahí no hay nada.

Quitando `properties.filter` de `objects.general` (y el `endDate`, dejando el `mode`), Power BI recalcula el tope desde el dato al abrir. Sobre `Calendario[Date]` eso aterriza en el día en curso, porque la partición acaba en `MAX(TODAY(), max DocDate)` y avanza sola. Sobre una columna de hechos el tope será el máximo del dato, o sea "sin recorte".

Estado actual: descongelados en **todas las páginas visibles**, incluida la *Previsión Mes* viva, que ya no guarda una fecha literal: el mes de cierre se toma del día en curso y cierra en su último día (`EOMONTH`, §5.6). Los marcadores tienen `suppressData: true`, así que no vuelven a aplicar la fecha vieja.

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
