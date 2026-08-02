# Modelo semántico **SgiRetail** — Documentación técnica

> 📁 **Esta documentación está replicada en todas las ramas.** Describe el modelo tal y como está en la rama **`sgiretail`**. En otras ramas la carpeta `SgiRetail/` puede existir pero en un estado anterior: para trabajar sobre el modelo, sitúate en la rama indicada. Índice: [README.md](README.md).

> Panel de **Retail** de Lola Casademunt: análisis de ventas de tiendas físicas y online, tráfico/afluencia, objetivos comerciales, conversión, comparativa de temporadas y catálogo visual de producto.

## Resumen del modelo

| Propiedad | Valor |
|---|---|
| Nombre del modelo | `Model` (proyecto **SgiRetail**) |
| Compatibility level | 1600 |
| Cultura / idioma | `es-ES` |
| Formato | TMDL (Power BI Desktop, DevMode/TMDLView) |
| Modo de almacenamiento | Import |
| Rama git | `sgiretail` |
| Time Intelligence automático | **Activado** (`__PBI_TimeIntelligenceEnabled = 1`) |
| Nº de tablas (total) | **31** |
| — Tablas de datos (origen SQL) | 6 (`LOL_PBISGIRETAIL`, `LOL_PBITIENDAS`, `LOL_PBIMODELITEM`, `LOL_PBISEASON`, `LOL_PBIOBJETIVOSRETAIL`, `LOL_PBITRAFICOTIENDAS`) |
| — Tablas calculadas / técnicas | 25 (calendario, selectores, parámetros, dimensiones de campo, tablas-contenedor de medidas, auto date/time ocultas) |
| Nº de medidas documentadas | **238** |
| Columnas calculadas DAX | 2 (`LOL_PBIMODELITEM[Modelo-Color]`, `Calendario[Temporada]`) + columnas de las auto date/time |
| Orígenes de datos | **SQL Server** `Sql.Database("192.168.0.232", "hana_etl_admin")`, esquema `dbo` (réplica del ERP/SAP-HANA). El resto de tablas son **calculadas DAX** (`CALENDAR`, `GENERATESERIES`, `DATATABLE`, literales `{ }`) |

### Arquitectura (esquema en estrella)

La tabla de hechos central es **`LOL_PBISGIRETAIL`** (líneas de ticket de venta). A su alrededor:

- **Dimensiones**: `LOL_PBITIENDAS` (tiendas), `LOL_PBIMODELITEM` (artículos/producto), `LOL_PBISEASON` (temporadas), `Calendario` (fecha, marcada como tabla de tiempo).
- **Hechos secundarios**: `LOL_PBITRAFICOTIENDAS` (afluencia por tienda) y `LOL_PBIOBJETIVOSRETAIL` (presupuesto/objetivos por tienda y fecha).
- **Tablas auxiliares de UI**: selectores (`PeriodoSelector`, `FechaHastaSelector`, `OrdenSelector`), parámetros (`ParamTopN`), parámetros de campo (`DimensionesB2C1/2/3`, `MedidasSelector`, `MedidasB2C1`).
- **Contenedores de medidas** (tablas calculadas de una fila `Row("Column", BLANK())` que solo alojan medidas): `MedidasRetail`, `MedidasTrafico`, `MedidasObjetivosRetail`, `MedidasCanales`, `MedidasCalculadasRetailOnline`, `MedidasDesempeño`, `MedidasIndicadores`, `MedidasCatalogo`.

El motor de cálculo de "periodo" no usa Time Intelligence nativo (YTD real) sino un **periodo dinámico** controlado por dos selectores (`PeriodoSelector` + `FechaHastaSelector`) y la medida raíz [`FechaInicioPeriodoSeleccionado`](#medidascalculadasretailonline-motor-de-periodo). El año anterior se calcula siempre con **desplazamiento fijo de −364 días** (comparación por día de la semana equivalente).

---

## Tablas

### Tablas de datos (origen SQL `hana_etl_admin`)

#### `LOL_PBISGIRETAIL` — Hechos: líneas de venta (tickets)
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/LOL_PBISGIRETAIL.tmdl)

Tabla de hechos principal. Cada fila es una **línea de ticket** de venta retail (TPV).

- **Origen**: `Sql.Database("192.168.0.232","hana_etl_admin")` → `dbo.LOL_PBISGIRETAIL` (import directo, sin transformaciones).
- **Columnas numéricas (medibles)**: `LineTotal` (importe con IVA), `LineTotalWithoutTax` (importe sin IVA), `Quantity` (unidades), `Precio_PVPR`, `PrecioTarifa`.
- **Claves de relación**: `WhsCode` → tienda, `ItemCode` → artículo, `Temporada` → temporada, `TicketData` (dateTime) → calendario.
- **Atributos**: `Cliente`, `Ticket`, `TicketHora`, `TipoDocumento` (p. ej. `VTD`, `VRMA` = devoluciones), `SalesPerson`, `Moneda`, `DocEntry`, `DocNum`, `LineNum`, `LineUID`, `ObjType`, `CodeBars`, `DescripcionOferta`, `WhsCode_Servicio`, `FechaComunicacion`.
- **Observación**: las columnas de importe/cantidad están en `double`; el resto en `string`. `TicketData` es la única fecha real (las demás fechas relevantes vienen de `Calendario`).

#### `LOL_PBITIENDAS` — Dimensión: tiendas
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/LOL_PBITIENDAS.tmdl)

Maestro de tiendas / puntos de venta.

- **Origen**: `dbo.LOL_PBITIENDAS` con **una transformación M**: añade la columna `OrdenTipoTienda` (1=TIENDA, 2=CORNER, 3=ONLINE, 99=resto) mediante `Table.AddColumn`. Es columna de **origen** (no calculada DAX) para poder usarla como *Sort By* de `TipoTienda` sin dependencia circular.
- **Claves**: `TiendaID` (clave hacia ventas y objetivos), `IDCuentaPersona` (clave hacia tráfico).
- **Atributos de jerarquía organizativa**: `TipoTienda` (ordenada por `OrdenTipoTienda`), `NombreTienda`, `ResponsableTienda`, `Territorio`, `AreaManager`.
- **Fechas con auto date/time**: `FechaApertura`, `FechaCierre`, `FechaComunicacion` (cada una genera una `LocalDateTable_*` oculta).
- Otros: `Int`, `UltimaSincronizacionServer`, `VersionBD`, `VersionTpv`.

#### `LOL_PBIMODELITEM` — Dimensión: artículos / catálogo de producto
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/LOL_PBIMODELITEM.tmdl)

Maestro de artículos (muy ancho, ~50 columnas de atributos de producto).

- **Origen**: `dbo.LOL_PBIMODELITEM` (import directo).
- **Clave**: `ItemCode` (relación con ventas).
- **Atributos de clasificación**: `Categoria`, `Nombre_familia`, `Descripcion_familia_agregada`, `Subfamilia`, `GrupodeArt` (= marca, p. ej. LOLA / MAITE), `Capsula`, `TIPO`.
- **Identificación de modelo**: `Referencia_modelo`, `ID_Modelo`, `Descripcion_modelo`, `Alias`, `Codigo_de_color`, `Descripcion_color`, `Codigo_de_talla`.
- **Imagen**: `URL_imagen_producto` (`dataCategory: ImageUrl`) — usada por las medidas de catálogo HTML.
- **Precios (texto)**: `PVP_ESPANA_Y_PORTUGAL`, `PVP_EUROPA`, `PVP_GRECIA`, `PVP_POLONIA`, `PVP_SUECIA`, `PVP_SUIZA` — almacenados como **string** (requieren conversión `VALUE`/`SUBSTITUTE` en DAX).
- Otros muchos atributos logísticos (composición, peso, partida arancelaria, proveedor, sostenibilidad, etc.).

**Columna calculada DAX:**

```dax
Modelo-Color =
CONCATENATE(
    LOL_PBIMODELITEM[Referencia_modelo],
    "." & LOL_PBIMODELITEM[Codigo_de_color]
)
```
*Concatena referencia de modelo + código de color (`REF.COLOR`). Es la clave de agrupación de producto que usan las medidas de catálogo (`HTML Catálogo`, `HTML Comparador Temporadas`) para identificar cada modelo-color de forma única.*

#### `LOL_PBISEASON` — Dimensión: temporadas
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/LOL_PBISEASON.tmdl)

Maestro de temporadas comerciales.

- **Origen**: `dbo.LOL_PBISEASON`.
- **Clave**: `Code` (relación con `LOL_PBISGIRETAIL[Temporada]`).
- **Comparativa**: `CompareSeason` (código de la temporada con la que comparar — usado por `HTML Comparador Temporadas`), `TemporadaAnterior`.
- **Rangos de fecha**: `Inicio`, `Fin` (definen el rango de la temporada — los usa la columna calculada `Calendario[Temporada]`), `FechaInicioB2B` (cada una con su auto date/time).
- Otros: `Name`, `AlmacenB2B`, `PptoB2B`.

#### `LOL_PBIOBJETIVOSRETAIL` — Hechos: objetivos / presupuesto
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/LOL_PBIOBJETIVOSRETAIL.tmdl)

Objetivos de venta por tienda y fecha.

- **Origen**: `dbo.LOL_PBIOBJETIVOSRETAIL` (la anotación `PBI_ResultType = Exception` sugiere que la última actualización del previsualizado dio error, posible aviso de refresco).
- **Columnas**: `Importe` (objetivo en €, formato moneda), `Fecha`, `TiendaID`, `RowGuid`.
- **Relaciones**: doble — `Fecha`→`Calendario` (inactiva) y `TiendaID`→`LOL_PBITIENDAS` (bidireccional). Las medidas de objetivos activan ambas con `USERELATIONSHIP`.

#### `LOL_PBITRAFICOTIENDAS` — Hechos: tráfico / afluencia
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/LOL_PBITRAFICOTIENDAS.tmdl)

Conteo de visitantes (contadores de personas) por tienda, fecha y hora.

- **Origen**: `dbo.LOL_PBITRAFICOTIENDAS`.
- **Columnas**: `Entradas` (entradas a la tienda), `Exterior` (paso por delante), `Salidas`, `Aforo`, `RatioConversion`, `TiempoMedio`, `count`, `Fecha`, `Hora`, `TiendaID`.
- **Observación crítica**: TODAS las métricas (`Entradas`, `Exterior`, etc.) están almacenadas como **`string`**, por lo que las medidas deben convertirlas con `SUMX(... IFERROR(VALUE(...), BLANK()))`.
- **Relación**: `TiendaID`→`LOL_PBITIENDAS[IDCuentaPersona]` (¡no a `TiendaID`!) y `Fecha`→`Calendario`.

---

### Tablas calculadas y técnicas

#### `Calendario` — Dimensión de tiempo
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/Calendario.tmdl)

Tabla de calendario maestra (marcada `dataCategory: Time`). Es la dimensión de fecha "oficial" del modelo (frente a las auto date/time).

- **Partición calculada**: `CALENDAR(FechaInicio, FechaFin)` enriquecida con `ADDCOLUMNS`, donde:
  ```dax
  VAR FechaInicio = DATE(2023, 1, 1)
  VAR FechaFin    = MAX ( TODAY (), MAX ( LOL_PBISGIRETAIL[TicketData] ) )
  ```
  El fin **ya no es fijo** (antes `DATE(2027,12,31)`): el calendario llega siempre hasta hoy — o hasta la última venta si hubiera tickets con fecha futura — sin arrastrar años vacíos por delante.
- **Columnas generadas**: claves (`FechaKey`, `AñoMesKey`, `AñoSemanaKey`), niveles de año/mes/trimestre/semana/día (con textos y órdenes), inicios/fines de periodo (`InicioMes`, `FinMes`, `InicioTrimestre`, etc.) y banderas relativas a hoy (`EsHoy`, `EsPasado`, `EsFuturo`, `EsMesActual`, `EsAñoActual`).
- ⚠️ Tanto el rango como las banderas `EsHoy/EsPasado/...` usan **`TODAY()`** → la tabla depende de la fecha de refresco. Sin refrescar, el calendario se queda corto respecto al día actual.

**Columna calculada DAX:**

```dax
Temporada = VAR d = Calendario[Date] RETURN CALCULATE(MAX(LOL_PBISEASON[Name]), FILTER(LOL_PBISEASON, LOL_PBISEASON[Inicio] <= d && LOL_PBISEASON[Fin] >= d))
```
*Asigna a cada fecha la temporada (`LOL_PBISEASON[Name]`) cuyo rango Inicio–Fin la contiene. Permite segmentar ventas por temporada a través de la relación Calendario→ventas, sin depender de la columna `Temporada` de la tabla de hechos.*

#### `FechaHastaSelector` — Selector de "fecha hasta"
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/FechaHastaSelector.tmdl)

Tabla **desconectada** (sin relación) derivada de `Calendario` con `SELECTCOLUMNS`. Columnas: `Date`, `FechaKey`, `FechaTexto` (formato `dd mmmm yyyy`, ordenada por `FechaKey`). El usuario elige aquí la fecha de corte; la lee la medida `FechaHastaSeleccionada`.

#### `PeriodoSelector` — Selector de granularidad de periodo
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/PeriodoSelector.tmdl)

Tabla desconectada literal con valores `Año, Trimestre, Mes, Semana, Dia` (columna `Periodo`, ordenada por `Orden`). La lee `PeriodoSeleccionado`. ⚠️ Define el valor **"Dia"** (no "Día") y **no incluye "Anio"** (el `SWITCH` de `FechaInicioPeriodoSeleccionado` espera `"Anio"`, ver Notas).

#### `OrdenSelector` — Selector de criterio de orden del catálogo
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/OrdenSelector.tmdl)

Tabla desconectada `DATATABLE` con `Ventas (€)` / `Unidades` / `PVP` (columna `Orden`, ordenada por `Idx` oculta). La leen con `SELECTEDVALUE` las medidas `HTML Catálogo` y `HTML Comparador Temporadas`.

#### `ParamTopN` — Parámetro numérico Top N
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/ParamTopN.tmdl)

Parámetro `GENERATESERIES(1, 50, 1)`, columna `'Top N'`. Controla cuántos productos muestra el catálogo. Sin selección = 10 (vía `[Catálogo Top N]`).

#### `DimensionesB2C1` / `DimensionesB2C2` / `DimensionesB2C3` — Parámetros de campo
[B2C1](../SgiRetail/SgiRetail.SemanticModel/definition/tables/DimensionesB2C1.tmdl) · [B2C2](../SgiRetail/SgiRetail.SemanticModel/definition/tables/DimensionesB2C2.tmdl) · [B2C3](../SgiRetail/SgiRetail.SemanticModel/definition/tables/DimensionesB2C3.tmdl)

Tres **parámetros de campo** (field parameters) idénticos. Cada uno expone la misma lista de 13 dimensiones (`NAMEOF`) para que el usuario elija la dimensión de desglose en distintos visuales: `TipoTienda`, `NombreTienda`, `ResponsableTienda`, `Territorio`, `AreaManager` (de tiendas); `Categoria`, `Descripcion_familia_agregada`, `Nombre_familia`, `Subfamilia` (de producto); `Año`, `MesTexto`, `NombreSemana`, `NombreDíaCorto` (de calendario). El índice de orden (`DimensionesB2C1 Orden` 0–12) lo leen las medidas de catálogo (`HTML Catálogo`, `HTML Comparador Temporadas`) con `SELECTEDVALUE` para conmutar la dimensión de agrupación.

#### `MedidasSelector` — Parámetro de campo de medidas
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasSelector.tmdl)

Parámetro de campo (oculto) que agrupa 53 medidas en tres grupos (`Retail`, `Objetivos`, `Trafico`) con etiqueta amigable y orden. Permite al usuario elegir qué medida ver en un visual genérico. Apunta vía `NAMEOF` a medidas de `MedidasRetail`, `MedidasObjetivosRetail` y `MedidasTrafico`.

#### `MedidasB2C1` — Parámetro de campo de columnas base
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasB2C1.tmdl)

Parámetro de campo (oculto) que expone 4 columnas numéricas crudas: `Quantity`, `LineTotal`, `LineTotalWithoutTax` (de ventas) e `Importe` (de objetivos).

#### `DateTableTemplate_*` y `LocalDateTable_*` — Auto date/time (Time Intelligence automático)
[Plantilla](../SgiRetail/SgiRetail.SemanticModel/definition/tables/DateTableTemplate_ad24ab92-1f21-4e82-b3c6-ce1f9e3f6ad1.tmdl)

Tablas **ocultas generadas automáticamente** por Power BI al tener activado el Time Intelligence. Hay **1 plantilla** + **6 `LocalDateTable_*`** (una por cada columna de fecha de las tablas de datos: `LOL_PBITIENDAS` ×3, `LOL_PBISEASON` ×3). Cada una tiene columnas `Año/NroMes/Mes/NroTrimestre/Trimestre/Día` y la jerarquía `Jerarquía de fechas`. Se recomienda **desactivarlas** (ver Notas).

LocalDateTables presentes: `c09b8fda...` (FechaApertura), `d8f78174...` (FechaCierre), `5e8c4207...` (FechaComunicacion tienda), `d1c9393b...` (FechaInicioB2B), `6806e327...` (Fin temporada), `4dfc2629...` (Inicio temporada).

---

## Relaciones
[Archivo relationships.tmdl](../SgiRetail/SgiRetail.SemanticModel/definition/relationships.tmdl)

### Relaciones de negocio (entre tablas de datos)

| # | Origen (lado *many*) | → Destino (lado *one*) | Cardinalidad | Dirección filtro | Activa | Notas |
|---|---|---|---|---|---|---|
| 1 | `LOL_PBISGIRETAIL[TicketData]` | `Calendario[Date]` | muchos→1 | Simple | ✅ Activa | Fecha de venta → calendario (datePartOnly) |
| 2 | `LOL_PBISGIRETAIL[WhsCode]` | `LOL_PBITIENDAS[TiendaID]` | muchos→1 | Simple | ✅ Activa | Venta → tienda |
| 3 | `LOL_PBISGIRETAIL[ItemCode]` | `LOL_PBIMODELITEM[ItemCode]` | muchos→1 | Simple | ✅ Activa | Venta → artículo |
| 4 | `LOL_PBISGIRETAIL[Temporada]` | `LOL_PBISEASON[Code]` | muchos→1 | Simple | ✅ Activa | Venta → temporada |
| 5 | `LOL_PBIOBJETIVOSRETAIL[TiendaID]` | `LOL_PBITIENDAS[TiendaID]` | muchos→1 | **Bidireccional** | ✅ Activa | Objetivos → tienda (`bothDirections`) |
| 6 | `LOL_PBIOBJETIVOSRETAIL[Fecha]` | `Calendario[Date]` | muchos→1 | Simple | ⛔ **Inactiva** | Activada por `USERELATIONSHIP` en medidas de objetivos |
| 7 | `LOL_PBITRAFICOTIENDAS[TiendaID]` | `LOL_PBITIENDAS[IDCuentaPersona]` | muchos→1 | Simple | ✅ Activa | Tráfico → tienda (⚠️ enlaza con `IDCuentaPersona`, no `TiendaID`) |
| 8 | `LOL_PBITRAFICOTIENDAS[Fecha]` | `Calendario[Date]` | muchos→1 | Simple | ✅ Activa | Tráfico → calendario |

> Nota sobre la relación #6: aparece como inactiva en el modelo y, además, las medidas de objetivos la **re-activan** con `USERELATIONSHIP(LOL_PBIOBJETIVOSRETAIL[Fecha], Calendario[Date])` junto con `USERELATIONSHIP(...[TiendaID], LOL_PBITIENDAS[TiendaID])`. Es la causa de que `Objetivos` use siempre `CALCULATE` con doble `USERELATIONSHIP`.

### Relaciones técnicas (a las auto date/time, todas `datePartOnly`)

| Origen | → Destino | Estado |
|---|---|---|
| `LOL_PBITIENDAS[FechaApertura]` | `LocalDateTable_c09b8fda…[Date]` | Activa |
| `LOL_PBITIENDAS[FechaCierre]` | `LocalDateTable_d8f78174…[Date]` | Activa |
| `LOL_PBITIENDAS[FechaComunicacion]` | `LocalDateTable_5e8c4207…[Date]` | Activa |
| `LOL_PBISEASON[FechaInicioB2B]` | `LocalDateTable_d1c9393b…[Date]` | Activa |
| `LOL_PBISEASON[Fin]` | `LocalDateTable_6806e327…[Date]` | Activa |
| `LOL_PBISEASON[Inicio]` | `LocalDateTable_4dfc2629…[Date]` | Activa |

> Las tablas de selector/parámetro (`FechaHastaSelector`, `PeriodoSelector`, `OrdenSelector`, `ParamTopN`, parámetros de campo) están **desconectadas** a propósito: se leen con `SELECTEDVALUE`/`ISFILTERED`, no por relación.

---

## Medidas

> Convención general del modelo:
> - El sufijo **`_YTD`** NO es Year-To-Date real: significa "del **periodo seleccionado**" (rango `[FechaInicioPeriodoSeleccionado] … [FechaHastaSeleccionada]`).
> - El sufijo **`_LY`** = mismo periodo desplazado **−364 días** (no `SAMEPERIODLASTYEAR`).
> - `_Var` = diferencia absoluta; `_Var_%` = variación relativa con `DIVIDE`.

### MedidasCalculadasRetailOnline — Motor de periodo
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasCalculadasRetailOnline.tmdl)

Tabla base que define el **motor de fechas** del que dependen casi todas las demás medidas.

#### `PeriodoSeleccionado`
```dax
PeriodoSeleccionado = SELECTEDVALUE(PeriodoSelector[Periodo], "Mes")
```
Lee la granularidad elegida en el segmentador `PeriodoSelector`. Por defecto "Mes".

#### `FechaHastaSeleccionada`
```dax
FechaHastaSeleccionada =
IF(
    ISFILTERED( FechaHastaSelector[Date] ),
    MAX( FechaHastaSelector[Date] ),
    TODAY()
)
```
Fecha de corte (límite superior) del periodo. Si el usuario no ha filtrado el selector, usa **`TODAY()`** (depende de la fecha actual).

#### `FechaInicioPeriodoSeleccionado`
```dax
FechaInicioPeriodoSeleccionado =
VAR FechaHasta = [FechaHastaSeleccionada]
VAR Periodo = [PeriodoSeleccionado]
RETURN
    SWITCH(
        Periodo,
        "Dia", FechaHasta,
        "Semana", FechaHasta - WEEKDAY(FechaHasta, 2) + 1,
        "Mes", DATE(YEAR(FechaHasta), MONTH(FechaHasta), 1),
        "Trimestre", DATE(YEAR(FechaHasta), (QUARTER(FechaHasta) - 1) * 3 + 1, 1),
        "Anio", DATE(YEAR(FechaHasta), 1, 1),
        DATE(YEAR(FechaHasta), MONTH(FechaHasta), 1)
    )
```
Calcula la fecha de **inicio** del periodo según la granularidad. ⚠️ Espera el valor `"Anio"`, pero `PeriodoSelector` ofrece `"Año"` → la rama de año no se activa y cae al *default* (mes). Ver Notas.

#### `RangoPeriodoSeleccionado`
```dax
RangoPeriodoSeleccionado =
VAR FechaInicio = [FechaInicioPeriodoSeleccionado]
VAR FechaHasta = [FechaHastaSeleccionada]
RETURN
    FORMAT(FechaInicio, "dd mmm yyyy") & " a " & FORMAT(FechaHasta, "dd mmm yyyy")
```
Texto descriptivo "dd mmm yyyy a dd mmm yyyy" para mostrar el rango activo en títulos.

#### `EsFechaEnPeriodoSeleccionado`
```dax
EsFechaEnPeriodoSeleccionado =
VAR FechaInicio = [FechaInicioPeriodoSeleccionado]
VAR FechaHasta = [FechaHastaSeleccionada]
VAR FechaActual = MAX(Calendario[Date])
RETURN
    IF(FechaActual >= FechaInicio && FechaActual <= FechaHasta, 1, 0)
```
Bandera 1/0: indica si la fecha del contexto cae dentro del periodo. Útil para filtrar visuales por fila de calendario.

#### Atajos de periodo
- `Ventas_PeriodoSeleccionado` / `Ventas_PeriodoSeleccionado_LY`: `[Ventas]` en el rango del periodo y su −364 (formato "EUR …").
- `Cantidad_PeriodoSeleccionado`, `Entradas_PeriodoSeleccionado`, `Exterior_PeriodoSeleccionado`: equivalentes para cantidad y tráfico. Son alias funcionales de las versiones `_YTD` de otras tablas.

---

### MedidasRetail — KPIs de venta retail
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasRetail.tmdl)

Núcleo de KPIs de venta. Patrón repetido para cada métrica: base → `_YTD` (periodo) → `_LY` (−364) → `_Var` → `_Var_%`.

#### Ventas (con IVA)
```dax
Ventas = SUM(LOL_PBISGIRETAIL[LineTotal])
```
Venta bruta total (importe con IVA). Es la medida base de todo el modelo.

```dax
Ventas_YTD =
CALCULATE([Ventas], DATESBETWEEN(Calendario[Date], [FechaInicioPeriodoSeleccionado], [FechaHastaSeleccionada]))
```
Ventas dentro del periodo seleccionado.

```dax
Ventas_YTD_LY =
VAR FechaInicio = [FechaInicioPeriodoSeleccionado] - 364
VAR FechaHasta = [FechaHastaSeleccionada] - 364
RETURN CALCULATE([Ventas], DATESBETWEEN(Calendario[Date], FechaInicio, FechaHasta))
```
Mismo periodo del año anterior (desplazamiento −364 días).

```dax
Ventas_YTD_Var = [Ventas_YTD] - [Ventas_YTD_LY]
Ventas_YTD_Var_% = DIVIDE([Ventas_YTD] - [Ventas_YTD_LY], [Ventas_YTD_LY])
```
Variación absoluta y porcentual del periodo vs año anterior.

#### Ventas sin IVA
`Ventas_SinIVA = SUM(LOL_PBISGIRETAIL[LineTotalWithoutTax])` y su familia `_YTD / _YTD_LY / _YTD_Var / _YTD_Var_%`, con la misma lógica de periodo. Sirven para márgenes/base imponible.

#### Cantidad (unidades)
`Cantidad = SUM(LOL_PBISGIRETAIL[Quantity])` y familia `Cantidad_YTD / _YTD_LY / _YTD_Var / _YTD_Var_%`. Unidades vendidas.

#### Transacciones (tickets)
```dax
NumeroTransacciones =
CALCULATE(
    DISTINCTCOUNT(LOL_PBISGIRETAIL[Ticket]),
    NOT LOL_PBISGIRETAIL[TipoDocumento] IN {"VTD", "VRMA"}
)
```
Nº de tickets únicos **excluyendo devoluciones** (`VTD`, `VRMA`). Familia: `NumeroTransacciones_YTD`, `NumeroTransacciones_LY`, `NumeroTransacciones_Var`, `NumeroTransacciones_Var_%`.

#### Ticket medio
```dax
PromedioVentaTransaccion = DIVIDE([Ventas], [NumeroTransacciones])
```
Importe medio por ticket (ventas / transacciones). Familia `_YTD` (`DIVIDE([Ventas_YTD],[NumeroTransacciones_YTD])`), `_LY`, `_Var`, `_Var_%`.

#### Clientes únicos
```dax
NumeroClientesUnicos = DISTINCTCOUNT(LOL_PBISGIRETAIL[Cliente])
```
Nº de clientes distintos. Familia `_YTD / _LY / _Var / _Var_%`.

#### UPT (Units Per Transaction)
```dax
UPT = DIVIDE([Cantidad], [NumeroTransacciones])
```
Unidades por ticket (indicador de venta cruzada). Familia `UPT_YTD / UPT_LY / UPT_Var / UPT_Var_%`.

#### Devoluciones
```dax
NumeroDevoluciones_YTD =
CALCULATE(
    DISTINCTCOUNT(LOL_PBISGIRETAIL[Ticket]),
    LOL_PBISGIRETAIL[TipoDocumento] IN {"VTD", "VRMA"},
    DATESBETWEEN(Calendario[Date], [FechaInicioPeriodoSeleccionado], [FechaHastaSeleccionada])
)
```
Tickets de devolución (`VTD`/`VRMA`) en el periodo. `NumeroDevoluciones_YTD_LY` es su versión −364.

```dax
Tasa_Devolucion_YTD =
VAR TotalTickets =
    CALCULATE(DISTINCTCOUNT(LOL_PBISGIRETAIL[Ticket]),
        DATESBETWEEN(Calendario[Date], [FechaInicioPeriodoSeleccionado], [FechaHastaSeleccionada]))
RETURN DIVIDE([NumeroDevoluciones_YTD], TotalTickets)
```
% de tickets que son devoluciones (devoluciones / total tickets, incluidas ventas y devoluciones). `Tasa_Devolucion_YTD_LY` (−364) y `Tasa_Devolucion_YTD_Var` (diferencia ×100, en "puntos" `\p`).

---

### MedidasTrafico — Afluencia y conversión
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasTrafico.tmdl)

#### Entradas / Exterior
```dax
Entradas =
SUMX(LOL_PBITRAFICOTIENDAS, IFERROR(VALUE(LOL_PBITRAFICOTIENDAS[Entradas]), BLANK()))
```
Suma de personas que **entran** a la tienda. Convierte el texto a número con `VALUE`+`IFERROR` (la columna origen es string). `Exterior` es idéntica sobre la columna `Exterior` (paso por delante del escaparate). Ambas con familia `_YTD / _YTD_LY / _YTD_Var` y la variación `Evolucion_Entradas_YTD_Porcentaje` / `Evolucion_Exterior_YTD_Porcentaje`.

#### Tasas
```dax
Tasa_Conversion_YTD = DIVIDE([NumeroTransacciones_YTD], [Entradas_YTD])
```
% de visitantes que compran (transacciones / entradas). Tiene `Tasa_Conversion_YTD_LY` (recalcula transacciones y entradas a −364) y `Tasa_Conversion_YTD_Var` (diferencia ×100 puntos).

```dax
Tasa_Atraccion_YTD = DIVIDE([Entradas_YTD], [Exterior_YTD])
```
% de viandantes que entran (entradas / exterior). Con `Tasa_Atraccion_YTD_LY` y `Tasa_Atraccion_YTD_Var`.

---

### MedidasObjetivosRetail — Objetivos y cumplimiento
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasObjetivosRetail.tmdl)

#### Objetivos
```dax
Objetivos =
CALCULATE(
    SUM(LOL_PBIOBJETIVOSRETAIL[Importe]),
    USERELATIONSHIP(LOL_PBIOBJETIVOSRETAIL[TiendaID], LOL_PBITIENDAS[TiendaID]),
    USERELATIONSHIP(LOL_PBIOBJETIVOSRETAIL[Fecha], Calendario[Date])
)
```
Suma del objetivo/presupuesto, **activando** las dos relaciones de la tabla de objetivos. `Objetivos_YTD` añade el `DATESBETWEEN` del periodo; `Objetivos_YTD_LY` lo desplaza −364. Familia completa `_YTD_Var`, `_YTD_Var_%`.

#### Cumplimiento y desviación
```dax
CumplimientoObjetivo_% = DIVIDE([Ventas_YTD], [Objetivos_YTD])
DesviacionObjetivo = [Ventas_YTD] - [Objetivos_YTD]
EvolucionObjetivo = [Ventas_YTD] - [Objetivos_YTD]
EvolucionObjetivo_% = DIVIDE([Ventas_YTD] - [Objetivos_YTD], [Objetivos_YTD])
```
- `CumplimientoObjetivo_%`: % de objetivo alcanzado (ventas / objetivo).
- `DesviacionObjetivo` y `EvolucionObjetivo`: **idénticas** (ventas − objetivo); duplicado/alias (ver Notas).
- `EvolucionObjetivo_%`: desviación relativa sobre el objetivo.

---

### MedidasCanales — Desglose por canal (TipoTienda)
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasCanales.tmdl)

60 medidas que replican los KPIs filtrando por `LOL_PBITIENDAS[TipoTienda]`. Los **5 canales**: `TIENDA`, `CORNER` (etiquetado "ECI"/El Corte Inglés), `ONLINE`, `MARKETPLACE`, `OUTLET`.

Patrón por canal (ej. ECI/Corner):
```dax
Ventas_YTD_ECI       = CALCULATE([Ventas_YTD],    LOL_PBITIENDAS[TipoTienda] = "CORNER")
Ventas_YTD_LY_ECI    = CALCULATE([Ventas_YTD_LY], LOL_PBITIENDAS[TipoTienda] = "CORNER")
Ventas_YTD_Var_%_ECI = DIVIDE([Ventas_YTD_ECI] - [Ventas_YTD_LY_ECI], [Ventas_YTD_LY_ECI])
Objetivos_YTD_ECI    = CALCULATE([Objetivos_YTD], LOL_PBITIENDAS[TipoTienda] = "CORNER")
CumplimientoObjetivo_%_ECI = DIVIDE([Ventas_YTD_ECI], [Objetivos_YTD_ECI])
```
Lo mismo para `_Online` (="ONLINE"), `_Marketplace` (="MARKETPLACE"), `_Outlet` (="OUTLET"), `_Tienda` (="TIENDA").

> ⚠️ **Inconsistencia de código de canal**: las medidas de ventas/objetivos del canal "ECI" filtran por `"CORNER"`, pero las medidas de detalle `PromedioVentaTransaccion_YTD_ECI`, `UPT_YTD_ECI` y `Conversion_YTD_ECI` filtran por `"ECI"` (que no existe como `TipoTienda`) → devolverán BLANK. Ver Notas.

#### Share (cuota sobre total)
```dax
Share_%_Tienda      = DIVIDE([Ventas_YTD_Tienda], [Ventas_YTD])
Share_%_ECI         = DIVIDE([Ventas_YTD_ECI], [Ventas_YTD])
Share_%_Online      = DIVIDE([Ventas_YTD_Online], [Ventas_YTD])
Share_%_Marketplace = DIVIDE([Ventas_YTD_Marketplace], [Ventas_YTD])
Share_%_Outlet      = DIVIDE([Ventas_YTD_Outlet], [Ventas_YTD])
```
Peso de cada canal sobre la venta total del periodo.

#### Cabeceras y textos para tarjetas
```dax
ChannelHeader_Tienda =
IF(ISBLANK([Ventas_YTD_Tienda]), "Tienda",
   "Tienda 🏬 " & FORMAT([Share_%_Tienda], "0%") & " del total")
```
Título dinámico de tarjeta con icono y % del total (variantes `_ECI` 🏢, `_Online` 🌐, `_Marketplace` 📦, `_Outlet` 🏷️).

```dax
Pilla_VsOBJ_ECI =
VAR Pct = [CumplimientoObjetivo_%_ECI] - 1
VAR OBJ = [Objetivos_YTD_ECI]
VAR Sign = IF(Pct >= 0, "+", "")
RETURN IF(ISBLANK(OBJ), BLANK(), Sign & FORMAT(Pct, "0%") & " vs OBJ")
```
Etiqueta tipo "píldora" "+X% vs OBJ" (cumplimiento − 100%). Hay `Pilla_VsOBJ_*` y `Pilla_VsPY_*` ("+X% vs PY", basada en `Ventas_YTD_Var_%`) por canal.

```dax
Texto_Progreso_Tienda =
VAR Pct = [CumplimientoObjetivo_%_Tienda]
VAR OBJ = [Objetivos_YTD_Tienda]
RETURN IF(ISBLANK(OBJ), BLANK(),
   FORMAT(Pct, "0%") & " del objetivo  ·  OBJ " & FORMAT(OBJ, "#,0"))
```
Texto "X% del objetivo · OBJ N" por canal (`_Tienda`, `_ECI`, `_Online`, `_Marketplace`).

#### Métricas de detalle por canal
- `PromedioVentaTransaccion_YTD_*` (ticket medio por canal).
- `UPT_YTD_*` (UPT por canal).
- `Conversion_YTD_*` (`Tasa_Conversion_YTD` por canal).
- `Atraccion_YTD_Tienda` (`Tasa_Atraccion_YTD` para TIENDA).

---

### MedidasIndicadores — Textos de indicadores (totales y por canal)
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasIndicadores.tmdl)

Genera **textos con semáforos de emoji** para tarjetas de cabecera.

```dax
Texto_Objetivo =
VAR Pct = [CumplimientoObjetivo_%]
VAR OBJ = [Objetivos_YTD]
VAR Icono = IF(Pct >= 1, "🟢 ✅", IF(Pct >= 0.8, "🟡 ⚠️", "🔴 ❌"))
RETURN IF(ISBLANK(OBJ), BLANK(),
   Icono & " " & FORMAT(Pct, "0%") & "| OBJ. " & FORMAT(OBJ, "#,0"))
```
Semáforo de cumplimiento: verde ≥100%, amarillo ≥80%, rojo <80%. Variantes por canal: `Texto_Objetivo_ECI/_Online/_Marketplace/_Outlet/_Tienda`.

```dax
Texto_LY =
VAR Pct = [Ventas_YTD_Var_%]
VAR LY = [Ventas_YTD_LY]
VAR Icono = IF(Pct > 0, "🟢 📈", "🔴 📉")
RETURN IF(ISBLANK(LY), BLANK(),
   Icono & " " & IF(Pct>0,"+"&FORMAT(Pct,"0%"),FORMAT(Pct,"0%")) & " vs PY " & FORMAT(LY, "#,0"))
```
Texto de evolución vs año anterior con flecha. Variantes por canal `Texto_LY_*`.

Otras:
- `Pilla_VsOBJ_Total` / `Pilla_VsPY_Total`: píldoras del total (equivalentes a las de `MedidasCanales` pero a nivel global).
- `Texto_Progreso_Total`: "X% del objetivo · OBJ N" del total.
- `NombreTienda_Selected = SELECTEDVALUE(LOL_PBITIENDAS[NombreTienda], "— Selecciona tienda —")`: nombre de tienda seleccionada o texto guía.

---

### MedidasDesempeño — Evolución multi-periodo y colores condicionales
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasDesempeño.tmdl)

80 medidas para un panel de "desempeño" con micro-tarjetas Año/Mes/Semana/Día y formato condicional.

#### Share dentro del visual
```dax
Share_Pct =
VAR TotalGlobal =
    CALCULATE([Ventas_YTD],
        ALLSELECTED(LOL_PBITIENDAS[TipoTienda]),
        ALLSELECTED(LOL_PBITIENDAS[NombreTienda]),
        ALLSELECTED(LOL_PBIMODELITEM[Categoria]),
        ALLSELECTED(LOL_PBIMODELITEM[Descripcion_familia_agregada]))
RETURN DIVIDE([Ventas_YTD], TotalGlobal)
```
Cuota de la fila sobre el total visible (`ALLSELECTED` sobre las dimensiones de desglose). `Share_Bar = MIN(1, [Share_Pct])` la acota a 100% para barras de progreso.

#### Familias de evolución (4 métricas × 4 periodos)
Cuatro métricas — **VN** (Ventas Netas), **TM** (Ticket Medio), **UPT**, **Conv** (Conversión) — cada una en cuatro periodos calculados ad-hoc desde `FechaHastaSeleccionada`: **Anio** (desde 1-ene), **Mes** (desde 1 del mes), **Sem** (desde lunes), **Dia** (solo ese día). Todas comparan contra −364. Ejemplos:

```dax
VN_Evol_Mes =
VAR FH = [FechaHastaSeleccionada]
VAR FI = DATE(YEAR(FH), MONTH(FH), 1)
VAR FHLY = FH - 364
VAR FILY = DATE(YEAR(FHLY), MONTH(FHLY), 1)
VAR A = CALCULATE([Ventas], DATESBETWEEN(Calendario[Date], FI, FH))
VAR L = CALCULATE([Ventas], DATESBETWEEN(Calendario[Date], FILY, FHLY))
RETURN DIVIDE(A - L, L)
```
% de evolución de ventas del mes en curso vs mismo periodo año anterior.

```dax
TM_Evol_Mes =
... RETURN DIVIDE(DIVIDE(VA, TA) - DIVIDE(VL, TL), DIVIDE(VL, TL))
```
Evolución del ticket medio (ventas/transacciones) entre periodos. `UPT_Evol_*` usa cantidad/transacciones; `Conv_Evol_*` usa transacciones/entradas. (Series completas: `VN_Evol_Anio/Mes/Sem/Dia`, `TM_Evol_*`, `UPT_Evol_*`, `Conv_Evol_*`.)

#### Colores, fondos y textos condicionales
```dax
Color_VN_Mes = SWITCH(TRUE(),
    ISBLANK([VN_Evol_Mes]), "#8089A2",
    [VN_Evol_Mes] >= 0, "#1F8A5B",
    [VN_Evol_Mes] >= -0.10, "#B57A0E",
    "#C73838")
```
Color de texto según el signo (verde ≥0, ámbar ≥−10%, rojo <−10%, gris si vacío). Existe la familia completa `Color_<Métrica>_<Periodo>` y la equivalente de fondo `Bg_<Métrica>_<Periodo>` (con tonos pastel `#EBF5EE / #FDF4CF / #FCEAEA / #F0F2F8`).

```dax
Txt_VN_Mes = IF(ISBLANK([VN_Evol_Mes]), "—", FORMAT([VN_Evol_Mes], "+0%;-0%;0%"))
```
Texto formateado con signo o "—" si vacío (familia `Txt_<Métrica>_<Periodo>`).

```dax
Lbl_Anio = "Año " & FORMAT(YEAR([FechaHastaSeleccionada]), "0000")
Lbl_Mes  = FORMAT([FechaHastaSeleccionada], "MMM.")
Lbl_Sem  = "S " & FORMAT(WEEKNUM([FechaHastaSeleccionada], 2), "00")
Lbl_Dia  = FORMAT([FechaHastaSeleccionada], "dd/MM")
```
Etiquetas dinámicas de cada micro-tarjeta de periodo.

Colores/fondos sobre KPIs ya existentes: `Color_Evol_VN`, `Color_Evol_OBJ`, `Color_Evol_TM`, `Color_Evol_UPT`, `Color_Evol_Conv` y sus `Bg_Evol_*` — pintan tarjetas según `Ventas_YTD_Var_%`, `EvolucionObjetivo_%`, `PromedioVentaTransaccion_Var_%`, `UPT_Var_%`, `Tasa_Conversion_YTD_Var` (este último con umbral en puntos: ≥0 / ≥−3).

---

### MedidasCatalogo — Catálogo visual de producto (HTML)
[Archivo TMDL](../SgiRetail/SgiRetail.SemanticModel/definition/tables/MedidasCatalogo.tmdl)

7 medidas que generan **HTML** para el visual "HTML Content" (catálogo con fotos, rankings y barras). Todas respetan los filtros del informe y el periodo activo.

#### `Catálogo Top N`
```dax
'Catálogo Top N' = SELECTEDVALUE( ParamTopN[Top N], 10 )
```
*displayFolder: `Catálogo Fotos\1. Configuración`* — Nº de productos a mostrar (10 por defecto).

#### `Catálogo HTML Productos`
*displayFolder: `Catálogo Fotos`* — Tarjetas con foto agrupadas por `TipoTienda` (TIENDA, CORNER ECI, ONLINE), Top N por `[Ventas]`. Usa `TOPN` sobre `Referencia_modelo`, `CALCULATETABLE`+`KEEPFILTERS` para filtrar el canal, y enriquece con `URL_imagen_producto`, marca (`GrupodeArt`), familia, categoría, PVP y tienda top. Cada tarjeta es un `<div class='cat-c'>` con `<img>` (y respaldo "sin imagen" vía `onerror`).

#### `Catálogo Imagen HTML`
*displayFolder: `Catálogo Fotos\2. Auxiliares`* — Auxiliar reutilizable: devuelve el `<img>` del producto del contexto actual (con borde azul y respaldo). Útil dentro de una matriz o tooltip.

#### `Catálogo HTML Tabla`
*displayFolder: `Catálogo Fotos`* — Versión tabla HTML compacta (miniatura + modelo, marca, categoría/familia, tienda, PVP, uds, importe) por `TipoTienda`, ordenada por importe. Misma lógica de datos que `Catálogo HTML Productos` pero renderiza `<table>`.

#### `HTML Catálogo`
*displayFolder: `Catálogo Fotos\2. Auxiliares`* — La medida de catálogo **más completa**. Lee `[Catálogo Top N]`, el índice de dimensión (`DimensionesB2C1[... Orden]`, 0–12) y el criterio de orden (`OrdenSelector[Orden]`). Construye con un gran `UNION` de 13 ramas (`SUMMARIZE` por cada dimensión posible × `Modelo-Color`) la tabla de pares grupo/modelo, la filtra al periodo con `DATESBETWEEN`, calcula el valor según `Ventas (€)` / `Unidades` / `PVP` (parseando el PVP texto con `SUBSTITUTE`+`VALUE`/`1.000.000`), toma `TOPN(12)` grupos y Top N productos por grupo, y emite tarjetas con ranking (medallas), color de marca (MAITE rosa #E11A6F / LOLA azul #10182F), silueta SVG según categoría, barra de progreso e imagen. Identidad de marca **45 Years** (rosa #E11A6F, navy #10182F).

#### `HTML Comparador Temporadas`
*displayFolder: `Catálogo Fotos\2. Auxiliares`* — Catálogo a **dos columnas** que compara la temporada seleccionada (`LOL_PBISEASON[Code]`) contra su `CompareSeason`. Usa `REMOVEFILTERS(LOL_PBISEASON)` + `LOL_PBISGIRETAIL[Temporada] IN {_A,_B}` para traer ambas temporadas, y replica la lógica de grupos/Top N de `HTML Catálogo` por columna. Si no hay temporada elegida muestra "Elige UNA temporada".

#### `TEST FOTO PUBLICA`
*displayFolder: `Catálogo Fotos\2. Auxiliares`* — Medida de **prueba**: HTML con una imagen pública de Wikimedia para verificar que el visual "HTML Content" puede cargar imágenes externas. No es de producción (ver Notas).

---

## RLS / Roles

**No existen roles de seguridad a nivel de fila (RLS).** El `model.tmdl` no define ningún bloque `role`, ni hay ficheros de roles ni expresiones de filtro de tabla por usuario. El modelo es de acceso completo para todos los usuarios con permiso sobre el informe.

---

## Notas y observaciones

1. **"YTD" es un nombre engañoso.** Ningún `_YTD` usa `TOTALYTD`/`DATESYTD` reales: significan "periodo seleccionado" (rango dinámico `FechaInicioPeriodoSeleccionado…FechaHastaSeleccionada`). Y "LY" no es `SAMEPERIODLASTYEAR` sino un **desplazamiento fijo de −364 días** (alinea día de semana, pero descuadra en años bisiestos y desplaza la fecha de fin de mes). Documentar/renombrar conviene para evitar malentendidos.

2. **Bug latente en el selector de periodo "Año".** `FechaInicioPeriodoSeleccionado` hace `SWITCH` sobre el valor `"Anio"`, pero `PeriodoSelector` ofrece la etiqueta **`"Año"`**. Al no coincidir, seleccionar "Año" cae al *default* (= comportamiento de "Mes"). Habría que unificar a `"Año"` o `"Anio"` en ambos sitios. (Igualmente "Dia" vs "Día": aquí sí coinciden ambos como "Dia".)

3. **Inconsistencia del código de canal ECI/CORNER.** En `MedidasCanales`, las medidas de ventas/objetivos del Corner filtran `TipoTienda = "CORNER"`, pero `PromedioVentaTransaccion_YTD_ECI`, `UPT_YTD_ECI` y `Conversion_YTD_ECI` filtran `TipoTienda = "ECI"`, valor que **no existe** en los datos → devuelven BLANK. Además, `Texto_Objetivo_Outlet`/`Texto_LY_Outlet` existen en `MedidasIndicadores` pero `MedidasSelector` no incluye los KPIs de Outlet ni Marketplace en su lista (solo Retail/Objetivos/Trafico).

4. **Medidas duplicadas / alias.** `DesviacionObjetivo` y `EvolucionObjetivo` tienen DAX idéntico (`[Ventas_YTD] - [Objetivos_YTD]`). `Ventas_PeriodoSeleccionado`, `Cantidad_PeriodoSeleccionado`, `Entradas_PeriodoSeleccionado` y `Exterior_PeriodoSeleccionado` (en `MedidasCalculadasRetailOnline`) son funcionalmente equivalentes a las `*_YTD` de `MedidasRetail`/`MedidasTrafico`. Conviene consolidar para reducir mantenimiento.

5. **Dependencia de `TODAY()`.** `FechaHastaSeleccionada` (sin filtro) y las banderas `EsHoy/EsPasado/EsMesActual/EsAñoActual` de `Calendario` dependen de la fecha de ejecución/refresco. Los resultados "por defecto" cambian cada día; las pruebas deben fijar una fecha en el selector para ser reproducibles.

6. **Numéricos almacenados como texto.** Todo `LOL_PBITRAFICOTIENDAS` (Entradas, Exterior, Salidas, Aforo…) y los PVP de `LOL_PBIMODELITEM` son `string`. Obliga a `SUMX(... VALUE())` en tráfico y a `SUBSTITUTE`+`VALUE` (con división por 1.000.000 para corregir el formato de miles) en los PVP del catálogo — frágil ante cambios de formato regional. Lo ideal sería tipar estas columnas en el ETL/M.

7. **Time Intelligence automático activado** (`__PBI_TimeIntelligenceEnabled = 1`) genera 7 tablas ocultas (`DateTableTemplate_*` + 6 `LocalDateTable_*`) que inflan el modelo y no se usan (el modelo ya tiene `Calendario`). Recomendable **desactivarlo** y eliminar las auto date/time.

8. **Relación de tráfico atípica.** `LOL_PBITRAFICOTIENDAS[TiendaID]` enlaza con `LOL_PBITIENDAS[IDCuentaPersona]` (no con `TiendaID`). Es intencionado (el contador de personas usa otra clave), pero conviene tenerlo presente al depurar discrepancias de tráfico por tienda.

9. **`PBI_ResultType = Exception` en `LOL_PBIOBJETIVOSRETAIL`.** El metadato del último previsualizado indica error en la consulta M de objetivos; verificar que el refresco completo funciona y que la tabla trae datos.

10. **Relación bidireccional objetivos↔tiendas.** La relación #5 es `bothDirections`. Combinada con la doble `USERELATIONSHIP` de las medidas de objetivos, puede producir resultados sutiles si se cruzan filtros de tienda y objetivos; revisar si la dirección simple sería suficiente.

11. **GUIDs/lineageTags "de juguete".** Muchas medidas creadas por código tienen `lineageTag` con patrones artificiales (`12345678-…`, `aabbccdd-…`, `0a1b2c3d-…`). No afecta al funcionamiento, pero delata generación programática y dificulta el rastreo de linaje.

12. **Marca "45 Years".** Las medidas HTML de catálogo usan la paleta corporativa (rosa `#E11A6F`, navy `#10182F`) y distinguen sub-marcas MAITE (rosa) y LOLA (navy) por `GrupodeArt`. `TEST FOTO PUBLICA` es una medida de diagnóstico que debería retirarse antes de producción.
