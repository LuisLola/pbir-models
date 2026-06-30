# Modelo semántico: B2BSalesOrder

> Documentación técnica generada a partir del modelo TMDL ubicado en
> [`SalesOrder/B2BSalesOrder.SemanticModel/definition/`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/).
> **Solo lectura**: este documento describe el modelo, no lo modifica.

---

## 1. Resumen del modelo

**B2BSalesOrder** es un modelo semántico de Power BI (compatibilityLevel **1600**, `powerBI_V3`, cultura **es-ES**) orientado al análisis de **pedidos de venta B2B** (canal mayorista) de Lola Casademunt. El eje analítico central es la **comparativa entre temporadas** (la temporada seleccionada frente a la temporada anterior equivalente) sobre la venta bruta y neta, más un tratamiento separado de **incidencias** (devoluciones / RMA) y un **catálogo visual de productos** renderizado en HTML.

| Concepto | Valor |
|---|---|
| Nombre del modelo | `Model` (proyecto B2BSalesOrder) |
| compatibilityLevel | 1600 |
| Cultura / idioma de origen | es-ES |
| Time Intelligence automático | Activado (`__PBI_TimeIntelligenceEnabled = 1`) |
| Nº de tablas "reales" (visibles/funcionales) | 18 |
| Tablas auto-generadas de fechas | 1 plantilla + 14 `LocalDateTable_*` (ocultas) |
| Nº de medidas documentadas | **76** |
| Orígenes de datos | SQL Server `192.168.0.232`, base `hana_etl_admin`, esquema `dbo` (5 tablas importadas vía M) + tablas calculadas DAX |
| RLS / Roles | **Ninguno** (no hay seguridad a nivel de fila definida) |

### Arquitectura (esquema en estrella)

- **Tabla de hechos principal:** [`LOL_PBIB2BSALESORDER`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIB2BSALESORDER.tmdl) (líneas de pedido B2B).
- **Tabla de hechos derivada:** [`VentasIncidencias`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/VentasIncidencias.tmdl) (subconjunto calculado de la anterior, solo incidencias).
- **Dimensiones de origen SQL:** [`LOL_PBICLIENTES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBICLIENTES.tmdl) (clientes), [`LOL_PBIAGENTES`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIAGENTES.tmdl) (agentes/vendedores), [`LOL_PBIMODELITEM`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBIMODELITEM.tmdl) (artículos/modelos), [`LOL_PBISEASON`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/LOL_PBISEASON.tmdl) (temporadas).
- **Dimensión de fechas calculada:** [`Calendario`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/Calendario.tmdl).
- **Tablas de apoyo / parámetros:** [`TemporadasUsadas`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/TemporadasUsadas.tmdl), [`DimensionB2B`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/DimensionB2B.tmdl), [`OrdenSelector`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/OrdenSelector.tmdl), [`ParamTopN`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/ParamTopN.tmdl), [`B2C1`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/B2C1.tmdl).
- **Tablas-contenedor de medidas (sin datos):** [`MedidasVentas`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasVentas.tmdl), [`MedidasTemporadas`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasTemporadas.tmdl), [`MedidasIncidencias`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasIncidencias.tmdl), [`MedidasFormato`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasFormato.tmdl), [`MedidasCatalogo`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasCatalogo.tmdl).

---

## 2. Tablas

### 2.1. Tabla de hechos — `LOL_PBIB2BSALESORDER`

**Propósito:** Líneas de pedidos de venta B2B (grano = línea de documento). Es la tabla central del modelo.
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

> **Importante:** casi todas las columnas llegan como **texto (string)** desde el origen. El paso M convierte dos importes a número creando las columnas numéricas `LineTotal_Num` (venta neta) e `ImporteBruto` (venta bruta) con `Number.From(..., "en-US")` y `try ... otherwise 0`.

**Columnas numéricas (las que suman):**

| Columna | Tipo | Significado |
|---|---|---|
| `ImporteBruto` | double (€) | Venta **bruta** por línea (derivada en M de `ImporteBruto_LC`). Base de la mayoría de medidas. |
| `LineTotal_Num` | double (€) | Venta **neta** por línea (derivada en M de `LineTotal_LC`). |
| `ImporteBruto_LC` | double ($) | Importe bruto en moneda local (con `changedProperty = DataType`). |

**Columnas clave para relaciones y filtros:**

| Columna | Uso |
|---|---|
| `CardCode` | Cliente → relación con `LOL_PBICLIENTES[CodeCliente]`. |
| `SlpCode` | Vendedor → relación con `LOL_PBIAGENTES[SlpCode]`. |
| `ItemCode` | Artículo → relación con `LOL_PBIMODELITEM[ItemCode]`. |
| `Season` | Temporada → relación con `LOL_PBISEASON[Code]`. |
| `DocDate` | Fecha de documento → relación activa con `Calendario[Date]`. |
| `DocDueDate`, `FechaComunicacion` | Fechas adicionales con jerarquías de fecha automáticas. |
| `Canceled` | Marca de anulación (`"Y"` = anulado; las medidas excluyen `<> "Y"`). |
| `IsIncidence` | Marca de incidencia (`"Y"`); las medidas de venta excluyen incidencias. |
| `Reposicion` | Marca de reposición (`"Y"`); usada para distinguir pedido inicial vs reposición. |
| `RMAMotiveCod`, `IncidenceType` | Motivo / tipo de incidencia (devoluciones). |
| `Qty` | Cantidad (texto; se convierte con `VALUE`/`INT` en las medidas). |

Otras columnas descriptivas/contables (todas string): `CountryCode`, `Currency`, `DiscAmt_Commercial_EUR`, `DiscAmt_Financial_EUR`, `DiscPrcnt_Header`, `DiscPrcnt_Line`, `DocEntry`, `DocNum`, `DocStatus`, `ExpnsAlloc_EUR`, `GrossAmt_EUR`, `Inadis`, `Inaped`, `LineNum`, `LineStatus`, `LineTotal_LC`, `LineUID`, `NetAmt_EUR`, `OpenQty`, `PickIdNo`, `Price_EUR`, `Price_LC`, `PriceBefDi_EUR`, `PriceBefDi_LC`, `Rate`, `Source`, `TotalExpns_EUR`, `TotalWithExpns_EUR`, `Transporte`, `UserSign`, `WhsCode`.

---

### 2.2. Tabla de hechos derivada — `VentasIncidencias`

**Propósito:** Aísla las líneas marcadas como incidencia (`IsIncidence = "Y"`) para analizarlas por separado de la venta "limpia".
**Origen:** **Tabla calculada DAX** (no SQL). Se construye filtrando y proyectando columnas de la tabla de hechos:

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

**Columnas numéricas:** `Cantidad`, `VentaBrutaML` (bruta), `ImporteNetoML` (neta).
**Columnas de enlace:** `FechaDocumento` → `Calendario[Date]`, `CodigoCliente` → clientes, `CodigoVendedor` → agentes, `CodigoArticulo` → artículos, `TemporadaLinea` → temporadas.

---

### 2.3. Dimensión `LOL_PBICLIENTES` (clientes)

**Propósito:** Maestro de clientes B2B. **Origen:** SQL (`dbo.LOL_PBICLIENTES`).
**Clave:** `CodeCliente`. **Columnas destacadas:** `RazonSocial`, `GroupName`/`GroupCode` (grupo de cliente), `Pais`, `Provincia`, `Poblacion`, `CP`, `NIF`, `Moneda`, `TarifaCode`/`TarifaName`, `Balance` (double), datos de contacto y direcciones. Sin columnas calculadas.

### 2.4. Dimensión `LOL_PBIAGENTES` (agentes/vendedores)

**Propósito:** Maestro de vendedores. **Origen:** SQL (`dbo.LOL_PBIAGENTES`).
**Clave:** `SlpCode`. **Columnas destacadas:** `Agente_Cajero` (nombre usado en agrupaciones/catálogo), `Email`, `Commission` (double), `Bloqueado`, `Memo`. Sin columnas calculadas.

### 2.5. Dimensión `LOL_PBIMODELITEM` (artículos / modelos)

**Propósito:** Maestro de productos (modelo-color, familias, precios PVP por país, imágenes). Es la dimensión más rica del modelo. **Origen:** SQL (`dbo.LOL_PBIMODELITEM`).
**Clave:** `ItemCode`.

**Columna calculada (DAX):**

```dax
Modelo-Color = LOL_PBIMODELITEM[Referencia_modelo] & "." & LOL_PBIMODELITEM[Codigo_de_color]
```
> `Modelo-Color` concatena referencia de modelo y código de color; es la **clave de agrupación de producto** que usa el catálogo HTML para agregar ventas, unidades y atributos por modelo-color.

**Columnas destacadas:** jerarquía de familias (`Categoria`, `Descripcion_familia_agregada`, `Nombre_familia`, `Subfamilia`, `GrupodeArt` —usado como "marca"—, `Material`), descripciones (`Descripcion_modelo`, `Descripcion_Larga`, color, talla, y sus variantes `_EN`), precios `PVP_ESPANA_Y_PORTUGAL`, `PVP_EUROPA`, `PVP_GRECIA`, `PVP_POLONIA`, `PVP_SUECIA`, `PVP_SUIZA` (todos string), `URL_imagen_producto` (foto del catálogo), proveedores, sostenibilidad y flags de activación.

### 2.6. Dimensión `LOL_PBISEASON` (temporadas)

**Propósito:** Maestro de temporadas comerciales y su lógica de comparación. **Origen:** SQL (`dbo.LOL_PBISEASON`).
**Clave:** `Code`. **Columnas destacadas:**

| Columna | Significado |
|---|---|
| `Code`, `Name` | Código y nombre de temporada. |
| `CompareSeason` | Temporada con la que se compara (define la "temporada anterior" equivalente). |
| `TemporadaAnterior` | Temporada inmediatamente anterior (usada para "temporada pasada"). |
| `Inicio`, `Fin`, `FechaInicioB2B` | Fechas de la temporada (con jerarquías de fecha automáticas). Nota: la lógica de comparación por días equivalentes usa **`Fin`** como ancla, no `Inicio` (ver Notas). |
| `AlmacenB2B`, `PptoB2B` | Almacén y presupuesto B2B. |

### 2.7. Dimensión de fechas `Calendario` (calculada)

**Propósito:** Tabla de calendario marcada como dimensión de fecha del modelo. **Origen:** tabla calculada DAX con `CALENDAR` desde `2022-01-01` hasta `31/12` de (año actual + 2). Todas sus columnas son calculadas (definidas en la expresión de la partición), p. ej. `FechaKey`, `AñoMesKey`, `Año`, `Mes`, `MesTexto`, `Trimestre`, `Semana`, `InicioMes`/`FinMes`, `InicioTrimestre`/`FinTrimestre`, `InicioAño`/`FinAño`, y flags `EsHoy`, `EsPasado`, `EsFuturo`, `EsMesActual`, `EsAñoActual`. Su columna `Date` es la usada por las relaciones activas con las dos tablas de hechos y por las funciones de inteligencia de tiempo (`DATESYTD`, `SAMEPERIODLASTYEAR`).

### 2.8. Tablas de apoyo y parámetros

| Tabla | Tipo / Origen | Propósito |
|---|---|---|
| [`TemporadasUsadas`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/TemporadasUsadas.tmdl) | Calculada DAX: `DISTINCT(SELECTCOLUMNS('LOL_PBISEASON',"Code",...))` | Lista de temporadas distintas para el **selector** de temporada. La medida `TemporadaSeleccionada` lee `SELECTEDVALUE` de su columna `Code`. No tiene relación física con el resto del modelo (se usa por desconexión). |
| [`ParamTopN`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/ParamTopN.tmdl) | Calculada: `GENERATESERIES(5, 50, 5)` | Parámetro numérico (5,10,…,50) para el "Top N" del catálogo. Columna `Top N`. |
| [`OrdenSelector`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/OrdenSelector.tmdl) | Calculada: `DATATABLE` | Criterio de ordenación del catálogo: `Ventas (€)` (1), `Unidades` (2), `PVP` (3). |
| [`DimensionB2B`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/DimensionB2B.tmdl) | Calculada: `DATATABLE` | Selector de la dimensión de agrupación del catálogo: Categoría(0), Familia agregada(1), Familia(2), Subfamilia(3), Marca(4), Cliente(5), Grupo cliente(6), País(7), Agente(8), Temporada(9). |
| [`B2C1`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/B2C1.tmdl) | **Parámetro de campo** (field parameter) calculado | Permite al usuario elegir el campo por el que segmentar (Agente, RazonSocial, País, GrupodeArt, Material, familias, etc.). Columnas `B2C1`, `B2C1 Campos`, `B2C1 Orden`; contiene la medida auxiliar `B2C2` (definición del parámetro de campo, ver §4.6). |

### 2.9. Tablas auto-generadas de fechas (Time Intelligence)

El modelo tiene **Time Intelligence automático activado**, lo que generó una plantilla `DateTableTemplate_4e09d981-…` y **14 tablas `LocalDateTable_*`** ocultas, una por cada columna de tipo fecha de las tablas (`DocDate`, `DocDueDate`, `FechaComunicacion`, las fechas de `LOL_PBISEASON` y todas las columnas de período de `Calendario`). Aportan jerarquías Año>Trimestre>Mes>Día pero **no son funcionales para el análisis** y conviene revisarlas (ver Notas).

---

## 3. Relaciones

Definidas en [`relationships.tmdl`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/relationships.tmdl). Salvo indicación, la cardinalidad es **muchos-a-uno** (la columna `from` está en el lado "muchos"), dirección de filtro **única** (single) y todas las relaciones **funcionales están activas**.

### 3.1. Relaciones del modelo de negocio

| # | Desde (muchos) | Hacia (uno) | Cardinalidad | Dirección | Estado |
|---|---|---|---|---|---|
| `a1b2c3d4…` | `LOL_PBIB2BSALESORDER[DocDate]` | `Calendario[Date]` | M:1 | Única | **Activa** |
| `b2c3d4e5…` | `LOL_PBIB2BSALESORDER[CardCode]` | `LOL_PBICLIENTES[CodeCliente]` | M:1 | Única | **Activa** |
| `AutoDetected_2e9f…` | `LOL_PBIB2BSALESORDER[SlpCode]` | `LOL_PBIAGENTES[SlpCode]` | M:1 | Única | **Activa** (auto-detectada) |
| `AutoDetected_6c94…` | `LOL_PBIB2BSALESORDER[ItemCode]` | `LOL_PBIMODELITEM[ItemCode]` | M:1 | Única | **Activa** (auto-detectada) |
| `c3d4e5f6…` | `LOL_PBIB2BSALESORDER[Season]` | `LOL_PBISEASON[Code]` | M:1 | Única | **Activa** |
| `4aa9b10d…` | `VentasIncidencias[FechaDocumento]` | `Calendario[Date]` | M:1 | Única | **Activa** |
| `b0497b45…` | `VentasIncidencias[CodigoCliente]` | `LOL_PBICLIENTES[CodeCliente]` | M:1 | Única | **Activa** |
| `99f93ac6…` | `VentasIncidencias[CodigoVendedor]` | `LOL_PBIAGENTES[SlpCode]` | M:1 | Única | **Activa** |
| `6ec446ec…` | `VentasIncidencias[CodigoArticulo]` | `LOL_PBIMODELITEM[ItemCode]` | M:1 | Única | **Activa** |
| `25ad6f70…` | `VentasIncidencias[TemporadaLinea]` | `LOL_PBISEASON[Code]` | M:1 | Única | **Activa** |

> Ambas tablas de hechos (`LOL_PBIB2BSALESORDER` y `VentasIncidencias`) comparten las mismas dimensiones (clientes, agentes, artículos, temporadas, calendario), formando un esquema en estrella doble.

### 3.2. Relaciones con tablas de fecha auto-generadas (`datePartOnly`)

Todas son M:1 hacia la columna `Date` de una `LocalDateTable_*`, con `joinOnDateBehavior: datePartOnly`. Son las relaciones internas de las jerarquías de fecha automáticas:

| Desde | Hacia (LocalDateTable) |
|---|---|
| `LOL_PBISEASON[FechaInicioB2B]` | `LocalDateTable_048becd8…[Date]` |
| `LOL_PBISEASON[Fin]` | `LocalDateTable_7667dd95…[Date]` |
| `LOL_PBISEASON[Inicio]` | `LocalDateTable_02678677…[Date]` |
| `Calendario[Date]` | `LocalDateTable_c773259d…[Date]` |
| `Calendario[InicioMes]` | `LocalDateTable_12b970ad…[Date]` |
| `Calendario[FinMes]` | `LocalDateTable_614253a8…[Date]` |
| `Calendario[InicioTrimestre]` | `LocalDateTable_016ce252…[Date]` |
| `Calendario[FinTrimestre]` | `LocalDateTable_80f7c7e9…[Date]` |
| `Calendario[InicioAño]` | `LocalDateTable_e74963ca…[Date]` |
| `Calendario[FinAño]` | `LocalDateTable_91198f72…[Date]` |
| `LOL_PBIB2BSALESORDER[DocDueDate]` | `LocalDateTable_d10ef2a1…[Date]` |
| `LOL_PBIB2BSALESORDER[FechaComunicacion]` | `LocalDateTable_eef1df2a…[Date]` |

> `TemporadasUsadas`, `ParamTopN`, `OrdenSelector`, `DimensionB2B` y `B2C1` **no tienen relaciones físicas**: actúan como tablas desconectadas leídas con `SELECTEDVALUE` (patrón de parámetro/selector).

---

## 4. Medidas (76)

Las medidas se reparten en 5 tablas-contenedor. A continuación se agrupan por tabla y, dentro de cada tabla, por `displayFolder`.

### 4.1. `MedidasTemporadas` — control de la temporada seleccionada y validación

Fichero: [`MedidasTemporadas.tmdl`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasTemporadas.tmdl). Estas medidas son el "motor" del modelo: resuelven qué temporada está seleccionada, cuál es su anterior/pasada y las fechas de corte que todas las demás medidas reutilizan.

#### Carpeta `Seleccion`

**`FechaLimiteSeleccionada`** — `D MMM YYYY`
```dax
VAR FechaFiltro = MAX ( 'Calendario'[Date] )
VAR FechaCarga = TODAY ()
RETURN MIN ( FechaFiltro, FechaCarga )
```
Fecha de corte global: el menor entre la fecha máxima visible en el filtro de calendario y hoy. Evita contar ventas "futuras" más allá de hoy y se usa como `FechaCorte` en casi todas las medidas de venta.

**`TemporadaSeleccionada`**
```dax
SELECTEDVALUE ( 'TemporadasUsadas'[Code] )
```
Código de la temporada elegida en el selector desconectado `TemporadasUsadas`. Es la entrada principal del resto de la lógica.

**`TemporadaAnterior`**
```dax
VAR TemporadaSel = [TemporadaSeleccionada]
RETURN
    IF ( ISBLANK ( TemporadaSel ), BLANK (),
        CALCULATE ( SELECTEDVALUE ( 'LOL_PBISEASON'[CompareSeason] ),
            REMOVEFILTERS ( 'LOL_PBISEASON' ),
            'LOL_PBISEASON'[Code] = TemporadaSel ) )
```
Temporada de comparación de la seleccionada, leída de `LOL_PBISEASON[CompareSeason]`.

**`TemporadaAnteriorTotal`**
```dax
VAR _val = [TemporadaAnterior]
RETURN IF ( ISBLANK ( _val ), "Total", _val & " Total" )
```
Etiqueta de texto para cabeceras de columnas "Total" de la temporada anterior.

**`TemporadaPasada`**
```dax
VAR TemporadaSel = [TemporadaSeleccionada]
RETURN
    IF ( ISBLANK ( TemporadaSel ), BLANK (),
        CALCULATE ( SELECTEDVALUE ( 'LOL_PBISEASON'[TemporadaAnterior] ),
            REMOVEFILTERS ( 'LOL_PBISEASON' ),
            'LOL_PBISEASON'[Code] = TemporadaSel ) )
```
Temporada inmediatamente anterior (de `LOL_PBISEASON[TemporadaAnterior]`), distinta del concepto "anterior de comparación".

#### Carpeta `Cabeceras` (textos dinámicos para títulos de visuales)

**`TituloComparativa`**
```dax
VAR T = [TemporadaSeleccionada]
VAR F = FORMAT ( MAX ( 'Calendario'[Date] ), "D MMM YYYY", "es-ES" )
RETURN IF ( ISBLANK ( T ), "Selecciona temporada", T & " a " & F )
```
Título tipo "T2526 a 30 jun 2026"; si no hay temporada, pide seleccionarla.

**`CabeceraTemporadaActual`** — `IF ( ISBLANK(T), "Temporada", T & "+" )` — etiqueta de la columna de temporada actual.
**`CabeceraTemporadaAnterior`** = `[TemporadaAnterior]` — etiqueta = código de la temporada anterior.
**`CabeceraTemporadaAnteriorIniciales`** — `IF(ISBLANK(Ant),"T. iniciales","T. iniciales " & Ant)` — cabecera para la venta "solo inicio" (sin reposiciones) de la temporada anterior.
**`CabeceraEvolPct`** — `IF(ISBLANK(T)||ISBLANK(Ant),"Evol. %","Evol. % " & T & " vs " & Ant)` — cabecera de la columna de variación porcentual.
**`CabeceraEvol`** = `"Evol."` (texto fijo).
**`CabeceraEvolIniciales`** = `"Evol. % T. iniciales"` (texto fijo).
**`CabeceraRealizado`** = `"Realizado"` (texto fijo).
**`CabeceraTemporadaPasada`** — `IF(ISBLANK(Pas),"T. pasada",Pas)` — cabecera de la temporada pasada.

#### Carpeta `Validacion` (auditoría de las ventanas de fechas comparadas)

**`ValidacionFechaInicioTemporadaSel`** — `D MMM YYYY` — `MAX('LOL_PBISEASON'[Fin])` de la temporada seleccionada. **Ojo:** usa `[Fin]` como ancla de "inicio" (decisión de diseño documentada como "Rangos por LOL_PBISEASON[Fin] v3").
**`ValidacionFechaFinTemporadaSel`** — última `DocDate` real con ventas (no anuladas/no incidencia) de la temporada seleccionada.
**`ValidacionFechaInicioTemporadaAnt`** — `MAX('LOL_PBISEASON'[Fin])` de la temporada anterior.
**`ValidacionFechaFinTemporadaAnt`** — última `DocDate` real con ventas de la temporada anterior.
**`ValidacionDiasTemporadaSel`** — `DATEDIFF(Inicio, Corte, DAY)+1`; nº de días transcurridos de la temporada seleccionada hasta la fecha de corte.
**`ValidacionFechaCorteAnt12M`** — `EDATE(FechaCorte, -12)`; fecha de corte de la anterior por desplazamiento de 12 meses.
**`ValidacionFechaFinAntDiasEq`** — `FechaInicioAnt + DiasTemporada - 1`; fin de la ventana "días equivalentes" de la temporada anterior.
**`ValidacionRangoTemporadaAnt`** — cadena descriptiva que vuelca todos los rangos calculados (inicio sel, días eq, corte 12M) para auditarlos en un visual de texto.
**`ValidacionVersionRangos`** = `"Rangos por LOL_PBISEASON[Fin] v3"` — sello de versión de la lógica de rangos.

---

### 4.2. `MedidasVentas` — ventas del pedido B2B

Fichero: [`MedidasVentas.tmdl`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasVentas.tmdl). Patrón común: `SUM` de `ImporteBruto` (o `LineTotal_Num`) con `REMOVEFILTERS('Calendario')` y `REMOVEFILTERS([DocDate])` y un tope `DocDate <= FechaCorte`, excluyendo anulados e incidencias.

#### Carpeta `Base`

**`VentaBruta`** — `€#,##0`
```dax
VAR FechaCorte = [FechaLimiteSeleccionada]
RETURN
    CALCULATE (
        SUM ( 'LOL_PBIB2BSALESORDER'[ImporteBruto] ),
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( 'LOL_PBIB2BSALESORDER'[DocDate] ),
        'LOL_PBIB2BSALESORDER'[Canceled] <> "Y",
        'LOL_PBIB2BSALESORDER'[IsIncidence] <> "Y",
        'LOL_PBIB2BSALESORDER'[DocDate] <= FechaCorte
    )
```
Venta bruta total hasta la fecha de corte, sin anulados ni incidencias e ignorando el filtro de calendario. Medida base del modelo.

**`VentaBrutaSalesOrder`** = `[VentaBruta]` — alias (misma venta bruta), pensada para usar en visuales del informe "SalesOrder".

**`VentaNeta`** — `€#,##0` — idéntica a `VentaBruta` pero sobre `LineTotal_Num` (importe neto de línea).

#### Carpeta `Temporada`

**`VentasTemporadaSeleccionada`** — venta bruta filtrada a `Season = TemporadaSeleccionada`, hasta fecha de corte (devuelve 0 si no hay temporada).
**`VentasTemporadaSeleccionadaSalesOrder`** = `[VentasTemporadaSeleccionada]` — alias.
**`VentasTemporadaSeleccionada_Bruta`** — igual que `VentasTemporadaSeleccionada` pero añade explícitamente la exclusión de anulados/incidencias.
**`VentasTemporadaAnterior`** — venta de la temporada anterior en la **ventana de días equivalentes** (desde `FechaInicioAnt = MAX(Fin)` de la anterior, durante `DiasTemporada` días); devuelve BLANK si faltan datos o el corte es anterior al inicio.
**`VentasTemporadaAnterior_SoloInicio`** — venta de la temporada anterior **excluyendo reposiciones** (`Reposicion <> "Y"`); representa la "venta inicial" comparable.
**`VentasTemporadaAnterior_Total`** — venta **total** (toda la temporada anterior, sin recorte de días).
**`VentasTemporadaPasada_SoloInicio`** — venta total de la temporada **pasada** (`TemporadaPasada`).

#### Carpeta `Evolucion`

**`EvolTemporadaSeleccionada`** = `[VentasTemporadaSeleccionadaTotal] - [VentasTemporadaAnterior]` — variación absoluta (€) actual vs anterior (días equivalentes).
**`EvolTemporadaSeleccionada_%`** — variación **porcentual** con protección de blank/cero (formato `0.0%`).
**`EvolTemporadaSeleccionada_Completa`** = `[VentasTemporadaSeleccionadaTotal] - [VentasTemporadaAnterior_SoloInicio]` — variación vs venta inicial (sin reposiciones) de la anterior.
**`EvolTemporadaSeleccionada_%Completa`** — su versión porcentual.

#### Carpeta `YTD`

**`VentasYTD`** — `€#,##0`
```dax
CALCULATE (
    CALCULATE (
        SUM ( 'LOL_PBIB2BSALESORDER'[LineTotal_Num] ),
        'LOL_PBIB2BSALESORDER'[Canceled] <> "Y",
        'LOL_PBIB2BSALESORDER'[IsIncidence] <> "Y"
    ),
    DATESYTD ( 'Calendario'[Date] )
)
```
Ventas netas acumuladas del año hasta la fecha (year-to-date) sobre `LineTotal_Num`.
**`VentasYTD_SalesOrder`** = `[VentasYTD]` — alias.
**`VentasYTD_LY`** — YTD del año anterior con `SAMEPERIODLASTYEAR`.
**`EvolVentasYTD`** = `[VentasYTD] - [VentasYTD_LY]` — variación absoluta YTD vs año anterior.
**`EvolVentasYTD_%`** — variación porcentual YTD vs año anterior.

#### Carpeta `Total`

Variantes "Total" de las anteriores, que **no recortan por días equivalentes** (toda la temporada) y, en el caso de las Total "puras", **no excluyen anulados/incidencias** (ver Notas):

**`VentaBrutaTotal`** — venta bruta hasta la fecha de corte **sin** excluir anulados/incidencias.
**`VentasTemporadaSeleccionadaTotal`** — venta de la temporada seleccionada hasta el corte, **sin** excluir anulados/incidencias. *(Es la que alimenta las medidas de Evolución.)*
**`VentasTemporadaAnteriorTotal`** — equivalente a `VentasTemporadaAnterior` (ventana de días equivalentes) pero sin la exclusión de anulados/incidencias.
**`VentasTemporadaAnterior_SoloInicioTotal`** — temporada anterior sin reposiciones.
**`VentasTemporadaAnterior_TotalTotal`** — temporada anterior completa.
**`EvolTemporadaSeleccionadaTotal`** = `[VentasTemporadaSeleccionadaTotal] - [VentasTemporadaAnteriorTotal]`.
**`EvolTemporadaSeleccionadaTotal_%`** — su versión porcentual.
**`VentaNetaTotal`** — venta neta (`LineTotal_Num`) hasta el corte sin excluir anulados/incidencias.
**`RealizadoTemporadaAnterior_%`** (carpeta `Temporada`) = `DIVIDE([VentasTemporadaSeleccionadaTotal],[VentasTemporadaAnterior_Total])` — % realizado de la actual respecto al total de la anterior.

#### Carpeta `Validacion`

Conjunto de medidas para auditar que la ventana de "días equivalentes" coincide con el corte a 12 meses:
**`ValidacionVentasAnt_Corte12M`** = `[VentasTemporadaAnterior]`.
**`ValidacionVentasAnt_DiasEq`** — venta anterior recalculada con las fechas de validación (`ValidacionFechaInicioTemporadaAnt` … `ValidacionFechaFinAntDiasEq`), **sin** excluir anulados/incidencias.
**`ValidacionVentasAnt_DiasEq_Limpia`** — igual que la anterior pero **excluyendo** anulados/incidencias.
**`ValidacionVentasAnt_DiasEq_AllFact`** — variante que ignora todo el contexto con `FILTER(ALL(...))` y `TREATAS` de clientes; sirve para contrastar el efecto de la propagación de filtros.
**`ValidacionDifAnt_DiasEq_vs_Corte12M`** = `[ValidacionVentasAnt_DiasEq] - [ValidacionVentasAnt_Corte12M]` — diferencia entre ambos métodos (debería ser ~0).
**`ValidacionLineasAnt_DiasEq`** — `COUNTROWS` de líneas de la anterior en la ventana de días equivalentes.

---

### 4.3. `MedidasIncidencias` — devoluciones / RMA

Fichero: [`MedidasIncidencias.tmdl`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasIncidencias.tmdl). Trabajan sobre la tabla `VentasIncidencias` y reproducen la misma estructura temporal que las de ventas.

#### Carpeta `Base`
**`VentaBrutaIncidencias`** = `SUM('VentasIncidencias'[VentaBrutaML])` — importe bruto de incidencias.
**`ImporteNetoIncidencias`** = `SUM('VentasIncidencias'[ImporteNetoML])` — importe neto de incidencias.
**`CantidadIncidencias`** = `SUM('VentasIncidencias'[Cantidad])` — unidades en incidencia.

#### Carpeta `Control`
**`LineasSalesOrderExcluidasPorIncidencia`** = `CALCULATE(COUNTROWS('LOL_PBIB2BSALESORDER'),'LOL_PBIB2BSALESORDER'[IsIncidence]="Y")` — nº de líneas del pedido marcadas como incidencia (que las medidas de venta excluyen). Sirve para cuadrar.

#### Carpeta `Temporada`
**`VentasTemporadaSeleccionadaIncidencias`** — incidencias de la temporada seleccionada hasta el corte.
**`VentasTemporadaAnteriorIncidencias`** — incidencias de la temporada anterior con corte a 12 meses (`EDATE(FechaCorte,-12)`).
**`VentasTemporadaAnteriorSoloInicioIncidencias`** — incidencias de la anterior excluyendo reposiciones (`PedidoReposicionLinea <> "Y"`).
**`VentasTemporadaAnteriorTotalIncidencias`** — incidencias totales de la temporada anterior.
**`VentasTemporadaPasadaSoloInicioIncidencias`** — incidencias totales de la temporada pasada.

#### Carpeta `YTD`
**`VentasYTD_Incidencias`** = `CALCULATE(SUM('VentasIncidencias'[VentaBrutaML]),DATESYTD('Calendario'[Date]))` — incidencias acumuladas del año.

#### Carpeta `Validacion`
**`ValidacionIncidenciasAnt_Corte12M`** = `[VentasTemporadaAnteriorIncidencias]`.
**`ValidacionIncidenciasAnt_DiasEq`** — incidencias de la anterior en ventana de días equivalentes.
**`ValidacionDifIncidenciasAnt_DiasEq_vs_Corte12M`** — diferencia entre ambos métodos.
**`ValidacionLineasIncidenciasAnt_DiasEq`** — `COUNTROWS` de incidencias de la anterior en días equivalentes.

---

### 4.4. `MedidasFormato` — colores condicionales

Fichero: [`MedidasFormato.tmdl`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasFormato.tmdl). Carpeta `Colores`. Devuelven códigos de color hex para formato condicional.

**`ColorEvol`**
```dax
IF ( ISBLANK ( [EvolTemporadaSeleccionada] ), "#595959",
    IF ( [EvolTemporadaSeleccionada] >= 0, "#00703C", "#C00000" ) )
```
Verde si la evolución (€) es positiva, rojo si negativa, gris si está en blanco.

**`ColorEvolPct`** — igual, pero según `[EvolTemporadaSeleccionada_%]`.

---

### 4.5. `MedidasCatalogo` — catálogo visual HTML de productos

Fichero: [`MedidasCatalogo.tmdl`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/MedidasCatalogo.tmdl). Carpeta `Catálogo B2B`.

**`Catálogo Top N`** = `SELECTEDVALUE( ParamTopN[Top N], 10 )` — Top N de productos a mostrar por grupo (por defecto 10).

**`HTML Catálogo B2B`** — medida de texto que genera **HTML completo** (con `<style>` embebido) para un visual *HTML Content*. Descripción del propio modelo:

> *Catálogo visual de productos B2B (foto, ranking con medallas, badge de marca). Se agrupa por la dimensión elegida en `DimensionB2B`, se ordena por `OrdenSelector` y muestra Top N (`ParamTopN`) por grupo. Ventas = `ImporteBruto` (excluye anulados/incidencias, hasta fecha de corte, respeta la temporada seleccionada). Devuelve HTML: usar en el visual "HTML Content".*

Lógica resumida: con `SWITCH` sobre `DimensionB2B[Orden]` (0–9) construye pares (grupo, modelo-color) con ventas y unidades mediante `SUMMARIZE`/`ADDCOLUMNS`, aplicando los filtros base (`Canceled<>"Y"`, `IsIncidence<>"Y"`, `DocDate<=FechaCorte`, temporada seleccionada). Calcula el criterio de orden (`Ventas`/`Unidades`/`PVP`), toma los 12 grupos top y dentro de cada grupo los `Top N` modelos; por cada modelo-color obtiene descripción, categoría, PVP, marca (`GrupodeArt`), imagen (`URL_imagen_producto`) y dibuja una tarjeta con ranking (medalla), barra de progreso y silueta SVG de respaldo si no hay foto. Usa la paleta de marca (rosa `#DFA0C9`, negro, gris). *(Por su longitud, ver el DAX íntegro en el fichero fuente, líneas 10–130.)*

---

### 4.6. `B2C1` — medida del parámetro de campo

Fichero: [`B2C1.tmdl`](../SalesOrder/B2BSalesOrder.SemanticModel/definition/tables/B2C1.tmdl).

**`B2C2`** — definición del **parámetro de campo** (field parameter): una tabla en línea que mapea etiquetas a columnas reales mediante `NAMEOF`, permitiendo cambiar dinámicamente el campo de segmentación:
```dax
{
    ("Agente_Cajero", NAMEOF('LOL_PBIAGENTES'[Agente_Cajero]), 0),
    ("RazonSocial",   NAMEOF('LOL_PBICLIENTES'[RazonSocial]), 1),
    ("Pais",          NAMEOF('LOL_PBICLIENTES'[Pais]), 2),
    ("GrupodeArt",    NAMEOF('LOL_PBIMODELITEM'[GrupodeArt]), 3),
    ("Material",      NAMEOF('LOL_PBIMODELITEM'[Material]), 4),
    ("Descripcion_familia_agregada", NAMEOF('LOL_PBIMODELITEM'[Descripcion_familia_agregada]), 5),
    ("Nombre_familia", NAMEOF('LOL_PBIMODELITEM'[Nombre_familia]), 6),
    ("Subfamilia",    NAMEOF('LOL_PBIMODELITEM'[Subfamilia]), 7)
}
```
> La misma expresión está también como `source` de la partición calculada de `B2C1`; la "medida" `B2C2` es la representación de DevMode del field parameter.

---

## 5. RLS / Roles

**No existe seguridad a nivel de fila (RLS) en este modelo.** No se ha encontrado ningún bloque `role`, ni `tablePermission`, ni `filterExpression` en la definición. Todos los usuarios con acceso al informe ven la totalidad de los datos.

---

## 6. Notas y observaciones

1. **Medidas "alias" redundantes.** Existen pares idénticos creados como alias para distintos informes: `VentaBrutaSalesOrder = [VentaBruta]`, `VentasTemporadaSeleccionadaSalesOrder = [VentasTemporadaSeleccionada]`, `VentasYTD_SalesOrder = [VentasYTD]`. Funcionan, pero duplican superficie de mantenimiento; conviene consolidar o documentar por qué existen.

2. **Inconsistencia "Total" vs base en el filtrado de anulados/incidencias.** Las medidas de la carpeta `Temporada`/`Base` excluyen `Canceled="Y"` e `IsIncidence="Y"`, pero las de la carpeta `Total` (`VentaBrutaTotal`, `VentasTemporadaSeleccionadaTotal`, `VentasTemporadaAnteriorTotal`, `VentaNetaTotal`…) **no** las excluyen. Como las medidas de **Evolución** (`EvolTemporadaSeleccionada`, sus `%` y `ColorEvol`) se basan en `VentasTemporadaSeleccionadaTotal` (sin filtrar) restando `VentasTemporadaAnterior` (sí filtrada), se está comparando una venta "sucia" contra otra "limpia". Revisar si es intencional; puede sesgar la variación.

3. **Datos numéricos almacenados como texto.** En `LOL_PBIB2BSALESORDER` casi todos los importes/cantidades vienen como `string` desde SQL; solo `ImporteBruto`, `LineTotal_Num` y `ImporteBruto_LC` son numéricos (los dos primeros derivados en M con `try…otherwise 0`). `Qty`, los `PVP_*` y los descuentos siguen siendo texto y se convierten con `VALUE`/`INT`/`Number.From` en el momento de calcular, lo que puede ocultar errores de parseo (quedan en 0/blank silenciosamente).

4. **Lógica de comparación anclada en `LOL_PBISEASON[Fin]`.** El "inicio" de la ventana de días equivalentes usa `MAX(Fin)` de la temporada (sello `ValidacionVersionRangos = "Rangos por LOL_PBISEASON[Fin] v3"`), no `Inicio`. Es una decisión deliberada (hay todo un bloque de medidas de validación para auditarla), pero es contraintuitiva y conviene tenerla presente al interpretar resultados. Existen dos definiciones de "anterior": `CompareSeason` (comparación) y `TemporadaAnterior` (pasada), que no deben confundirse.

5. **Time Intelligence automático infla el modelo.** Hay 1 plantilla + 14 `LocalDateTable_*` ocultas generadas por `__PBI_TimeIntelligenceEnabled = 1`, una por cada columna de fecha (incluidas las múltiples columnas de período de `Calendario`). Dado que ya existe una dimensión `Calendario` propia y marcada, estas tablas automáticas son redundantes: se recomienda **desactivar la fecha/hora automática** para reducir tamaño y evitar jerarquías de fecha duplicadas.

6. **Tablas desconectadas (parámetros).** `TemporadasUsadas`, `ParamTopN`, `OrdenSelector`, `DimensionB2B` y `B2C1` no tienen relaciones y se leen con `SELECTEDVALUE`. Es el patrón correcto para selectores, pero implica que el filtrado de temporada se hace **vía medida** (`Season = TemporadaSeleccionada`) y no por relación; cualquier visual que no pase por estas medidas no quedará filtrado por temporada.

7. **Dependencia fuerte de `FechaLimiteSeleccionada`.** Casi todas las medidas de venta dependen de esta medida (que incluye `TODAY()`), por lo que los resultados **cambian con la fecha de ejecución** y no son reproducibles históricamente sin fijar el calendario. Es esperado para un panel "vivo", pero relevante para validaciones.

8. **`cultures/es-ES.tmdl` solo contiene metadatos lingüísticos de Q&A** (sinónimos auto-generados, `State: "Generated"`), no traducciones ni descripciones de medidas. No aporta documentación funcional.
