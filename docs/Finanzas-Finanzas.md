# Finanzas — Documentación del modelo

> 📁 **Esta documentación está replicada en todas las ramas.** Describe el modelo tal y como está en la rama **`finanzas`**. En otras ramas la carpeta `Finanzas/` puede existir pero en un estado anterior: para trabajar sobre el modelo, sitúate en la rama indicada. Índice: [README.md](README.md).

Panel **financiero** de Lola Casademunt: **Balance**, **Cuenta de Pérdidas y Ganancias** (oficial) y **P&G de Gestión**, con comparativa contra el año anterior y desglose por dimensión analítica (canal, tienda, sucursal, trimestre, mes, año).

- **Proyecto:** [Finanzas](../Finanzas/) · `Finanzas.pbip`
- **Modelo:** `Finanzas.SemanticModel` · **Informe:** `Finanzas.Report`
- **compatibilityLevel:** 1606 · **Cultura:** es-ES · **Modo:** Import
- **Rama:** `finanzas`
- **RLS:** ninguno

> **Modelo en migración.** Los apuntes contables se están moviendo de la vista HANA `JournalEntryItem` a la tabla de staging SQL `LOL_PBIFINANCIALENTRIES`. Todas las medidas ya leen la tabla nueva; quedan piezas apuntando a la vieja (ver §7).

---

## 1. Origen de datos

| Tabla | Origen | Papel |
|---|---|---|
| `LOL_PBIFINANCIALENTRIES` | `Sql.Database("192.168.0.232","hana_etl_admin")` → `dbo.LOL_PBIFINANCIALENTRIES` | **Hechos** — apuntes de diario (grano: línea de asiento) |
| `LOL_PBIFINANCIALMODEL` | `Sql.Database(…)` → `dbo.LOL_PBIFINANCIALMODEL` | **Dimensión clave** — plan de cuentas jerárquico + orden de presentación |
| `JournalEntryItem` | `SapHana.Database("192.168.0.231:30015")`, cubo `sap.sbololasucursal.fin` | **Legado** — misma información en HANA, con nombres de columna en español |
| `CostCenter` | HANA `sap.sbololasucursal.adm`, filtrado a `CostCenterDimensionCode = 1` | Dimensión 1 (**canal**) |
| `CostCenter_Dim2` | HANA, filtrado a `CostCenterDimensionCode = 2` | Dimensión 2 (**tienda**) |
| `Calendario` | Tabla calculada `CALENDAR(2023-01-01, 2029-12-31)` | Dimensión fecha |
| `TablaFechasFiltro` | Tabla calculada `DISTINCT(SELECTCOLUMNS('Calendario',…))` | **Slicer de fechas** (ver §4) |
| `Dimensión Finanzas` | Tabla calculada — *field parameter* | Selector de la dimensión de desglose |
| `KPI Export (Dim)` | Tabla calculada `UNION(SELECTCOLUMNS(SUMMARIZE(ALL(…))))` | Puente de valores para el desglose por dimensión |
| `Páginas` | `GENERATESERIES(1,4,1)` | Paginación del listado por tienda |
| `Medidas Finanzas` | `{BLANK()}` | Tabla-contenedor de las 118 medidas |

Más 1 plantilla y 7 tablas `LocalDateTable_*` de la fecha/hora automática.

### La migración HANA → SQL

`LOL_PBIFINANCIALENTRIES` es la versión ETL de `JournalEntryItem`. El mapeo de columnas es directo pero cambia el idioma:

| `JournalEntryItem` (HANA, es) | `LOL_PBIFINANCIALENTRIES` (SQL, en) |
|---|---|
| `Código de cuenta` | `AccountCode` |
| `Fecha de contabilización` | `PostingDate` |
| `Fecha de documento` | `DocumentDate` |
| `Fecha de vencimiento` | `DueDate` |
| `Abono (ML)` / `Cargo (ML)` | `CreditLC` / `DebitLC` |
| `Haber (MS)` / `Debe (MS)` | `CreditSC` / `DebitSC` |
| `Código 1…5 de regla de distribución` | `DistributionRuleCode1…5` |
| `Nombre sucursal` | `BranchCode` |
| `Código del tipo de documento` | `DocumentTypeCode` |
| `Código de interlocutor comercial` | `BusinessPartnerCode` |
| `Tipo de diario` | `JournalType` |
| `Clave interna de operación de diario` | `JournalTransactionInternalKey` |

La tabla nueva añade además `LineUID`, `JournalTransactionLineInternalKey`, `FinancialPeriodInternalKey`, `Series`/`SeriesNumber`, `ProjectCode`, `IndicatorCode`, `ControlAccountCode`, `RemarkInRow`, `Remarks`, `Reference1…3` y `FechaComunicacion` (marca de refresco del ETL).

---

## 2. `LOL_PBIFINANCIALMODEL` — el plan de cuentas jerárquico

Es la pieza más importante del modelo: no es solo un maestro de cuentas, es **la estructura de presentación del informe financiero**.

### Columnas de origen

| Columna | Papel |
|---|---|
| `CODIGO_CUENTA`, `CODIGO_INTERNO_CUENTA`, `NOMBRE_CUENTA` | Identificación de la cuenta |
| `CODIGO_MODELO`, `NOMBRE_MODELO` | Qué informe financiero es (balance, P&G, …) |
| `NIVEL`, `NIVEL_1` … `NIVEL_5` | Los cinco niveles de la jerarquía de presentación |
| **`VISUAL_ORDER`** | **Orden absoluto de la línea en el informe** — el eje sobre el que se calculan todos los subtotales |
| `ORDER_N2`, `ORDER_N3` | Orden dentro de los niveles 2 y 3 (`NIVEL_2`/`NIVEL_3` se ordenan por ellos) |
| `NUMERADOR`, `CLAVE_CUENTA_SUPERIOR`, `SubSum`, `Dummy`, `Descr`, `U_LOL_CLASSACCT` | Metadatos de la estructura |

### Columnas calculadas

| Columna | Qué hace |
|---|---|
| `Nivel5_Limpio` | `NIVEL_5` con espacios duros (`UNICHAR(160)`) sustituidos y recortado; si queda vacío, cae a `CODIGO_CUENTA-NOMBRE_CUENTA`. |
| `Nivel6_Limpio` | Igual, pero devuelve `BLANK()` cuando es una fila de subtotal (`SubSum = "Y"`) — así los subtotales no repiten cuenta al desplegar el último nivel. |
| **`CORRECION`** | `IF ( CODIGO_MODELO = 4 \|\| CODIGO_MODELO = 6, -1, 1 )` — **inversor de signo**. Ver §3. |
| `PyG_POSICION_A RESULTADO DE EXPLOTACION` | `LOOKUPVALUE(VISUAL_ORDER, NIVEL_1, "A) RESULTADO DE EXPLOTACIÓN")` — la posición del subtotal A, como valor disponible en cada fila. |
| `PyG_POSICION_B RESULTADO FINANCIERO` | Ídem para `"B) RESULTADO FINANCIERO"`. |
| `PyG_POSICION_C RESULTADO ANTES DE IMPUESTOS` | Ídem para `"C) RESULTADO ANTES DE IMPUESTOS (A+B)"`. |
| `PyG_POSICION_D RESULTADO DEL EJERICIO` | Ídem para `"D) RESULTADO DEL EJERCICIO (C+17)"`. |
| `PyGG_POSICION_MARGENBRUTO` | Ídem para `"MARGEN BRUTO"` (P&G de gestión). |
| `PyGG_POSICION TOTALGASTOS` | Ídem para `"TOTAL GASTOS DE EXPLOTACIÓN"`. |
| `NIVEL3_BOLD` | Cosmético: para la línea `"A) RESULTADO DE EXPLOTACIÓN"` devuelve el mismo texto compuesto con caracteres **Unicode Mathematical Bold** (`UNICHAR(119808)`…), truco para simular negrita dentro de una matriz. Para el resto devuelve `NIVEL_3` tal cual. |

### Jerarquía y medidas de profundidad

La jerarquía `PLAN_DE_CUENTA` encadena `NIVEL_1 → NIVEL_2 → NIVEL_3 → NIVEL_4 → Nivel5_Limpio → Nivel6_Limpio → NIVEL3_BOLD`.

Tres medidas viven en la propia tabla y controlan qué filas se muestran al desplegar la matriz:

```dax
EntityBrowseDepth =                          -- cuántos niveles están expandidos
      ISINSCOPE ( LOL_PBIFINANCIALMODEL[NIVEL_1] )
    + ISINSCOPE ( LOL_PBIFINANCIALMODEL[NIVEL_2] )
    + ISINSCOPE ( LOL_PBIFINANCIALMODEL[NIVEL_3] )
    + ISINSCOPE ( LOL_PBIFINANCIALMODEL[NIVEL_4] )
    + ISINSCOPE ( LOL_PBIFINANCIALMODEL[Nivel5_Limpio] )
    + ISINSCOPE ( LOL_PBIFINANCIALMODEL[Nivel6_Limpio] )

EntityRowDepth = MAX ( LOL_PBIFINANCIALMODEL[NIVEL] ) + 1   -- hasta dónde llega esta cuenta

Remove Blanks = SWITCH ( TRUE (),
    AND ( ISINSCOPE ( …[Nivel6_Limpio] ), ISBLANK ( VALUES ( …[Nivel6_Limpio] ) ) ), BLANK (),
    1 )
```

`EntityBrowseDepth <= EntityRowDepth` es la condición que usan todas las medidas `Total Base YTD *` para **no repetir el importe de una cuenta en niveles más profundos de los que tiene** (patrón *ragged hierarchy*). `Remove Blanks` está pensada como filtro de visual para esconder las filas vacías del último nivel, pero **hoy no la referencia ningún visual del informe**.

---

## 3. Reglas de negocio clave

1. **El importe es `CreditLC − DebitLC`.** Es la base de absolutamente todo:
   ```dax
   Sum Amount = SUM ( LOL_PBIFINANCIALENTRIES[CreditLC] ) - SUM ( LOL_PBIFINANCIALENTRIES[DebitLC] )
   ```
   En contabilidad los ingresos son acreedores y los gastos deudores, así que esta resta da los ingresos en positivo y los gastos en negativo de forma natural.

2. **`CORRECION` invierte el signo del balance.** Para los modelos 4 y 6 (activo/pasivo), la convención de presentación es la contraria, así que `Ventas_YTD` multiplica por `AVERAGE(CORRECION)`:
   ```dax
   Ventas_YTD =
   VAR FechasSeleccionadas = VALUES ( TablaFechasFiltro[FechaFiltro] )
   RETURN CALCULATE (
       ( SUM ( LOL_PBIFINANCIALENTRIES[CreditLC] ) - SUM ( LOL_PBIFINANCIALENTRIES[DebitLC] ) )
           * AVERAGE ( LOL_PBIFINANCIALMODEL[CORRECION] ),
       TREATAS ( FechasSeleccionadas, 'Calendario'[Date] )
   )
   ```

3. **Los subtotales se calculan por rango de `VISUAL_ORDER`, no por jerarquía.** Un subtotal como "A) Resultado de explotación" es *la suma de todas las líneas cuya posición visual está por debajo de la suya*:
   ```dax
   PyG_A RESULTADO DE EXPLOTACIÓN =
   CALCULATE (
       SUM ( LOL_PBIFINANCIALENTRIES[CreditLC] ) - SUM ( LOL_PBIFINANCIALENTRIES[DebitLC] ),
       LOL_PBIFINANCIALMODEL[VISUAL_ORDER] < AVERAGE ( LOL_PBIFINANCIALMODEL[PyG_POSICION_A RESULTADO DE EXPLOTACION] ),
       REMOVEFILTERS ( …[ORDER_N3], …[NIVEL_3], …[NIVEL_4], …[Nivel5_Limpio], …[Nivel6_Limpio] )
   )
   ```
   El `REMOVEFILTERS` de los niveles inferiores es imprescindible: sin él, la fila del subtotal solo vería su propio contexto y devolvería su propio importe (cero).

   `B` se calcula como el rango *entre* A y B; `C = A + B`; `D = C +` el rango entre C y D.

4. **`Sum_Switch` decide, fila a fila, si mostrar el importe normal o un subtotal.** Como en la matriz conviven líneas de detalle y líneas de subtotal, un `SWITCH` sobre `VISUAL_ORDER` enruta cada fila:
   ```dax
   PyG Sum_Switch =
   VAR __groupord = SELECTEDVALUE ( LOL_PBIFINANCIALMODEL[VISUAL_ORDER] )
   RETURN SWITCH ( __groupord,
       MIN ( …[PyG_POSICION_A RESULTADO DE EXPLOTACION] ),      [PyG_A RESULTADO DE EXPLOTACIÓN],
       MIN ( …[PyG_POSICION_B RESULTADO FINANCIERO] ),          [PyG_B RESULTADO FINANCIERO],
       MIN ( …[PyG_POSICION_C RESULTADO ANTES DE IMPUESTOS] ),  [PyG_C RESULTADO ANTES IMPUESTOS],
       MIN ( …[PyG_POSICION_D RESULTADO DEL EJERICIO] ),        [PyG_D RESULTADO EJERCICIO],
       [Ventas_YTD] )                                           -- caso normal
   ```
   Y encima de él, `Total Base YTD PYG` aplica el filtro de profundidad de jerarquía. Este par (`Sum_Switch` + `Total Base YTD *`) se repite para cada uno de los tres informes: `PyG`, `PyGG` y balance.

5. **El P&G va por fecha de contabilización.** La relación activa con el `Calendario` es `LOL_PBIFINANCIALENTRIES[PostingDate] → Calendario[Date]` (no `DocumentDate`). Es el criterio contable correcto: un asiento pertenece al periodo en que se contabiliza.

6. **El año anterior se calcula sobre la selección real del usuario**, no sobre el contexto del visual: todas las medidas `_LY` leen `VALUES(TablaFechasFiltro[FechaFiltro])` y aplican `SAMEPERIODLASTYEAR` sobre esa tabla. Ver §4.

---

## 4. `TablaFechasFiltro` — por qué existe un calendario duplicado

`TablaFechasFiltro` es una copia de las fechas del `Calendario` (`DISTINCT(SELECTCOLUMNS('Calendario', "FechaFiltro", 'Calendario'[Date]))`) conectada a él con relación **1:1 bidireccional**.

Es el patrón de *slicer desacoplado*: el usuario filtra sobre `TablaFechasFiltro`, que propaga al `Calendario` y de ahí a los hechos. Pero además las medidas pueden **leer la selección original** con `VALUES(TablaFechasFiltro[FechaFiltro])` incluso después de haber hecho `REMOVEFILTERS('Calendario')`. Eso permite:

- Calcular el año anterior con `SAMEPERIODLASTYEAR(FechasSeleccionadas)` respetando exactamente el rango que eligió el usuario.
- Construir los títulos dinámicos: la tabla aloja 8 medidas de texto (`Titulo VENTAS_YTD`, `Titulo VENTAS_YTD_LY`, `Titulo %CN`, `Titulo %CN_LY`, `TITULO EVOL`, `TITULO EVOL %CN`, `TITULO CONCEPTO`, `FechaInicioPeriodoSeleccionado`) que devuelven el año o rango de años seleccionado (`"2025"`, `"2024–2025"`) para rotular las columnas de la matriz.

---

## 5. Desglose por dimensión: `Dimensión Finanzas` + `KPI Export (Dim)`

El informe permite reagrupar los KPIs por seis dimensiones distintas con un solo segmentador. Se implementa con tres piezas:

**`Dimensión Finanzas`** — *field parameter* con seis opciones y su orden:

| Orden | Etiqueta | Campo |
|---:|---|---|
| 0 | Canal | `Código 1 de regla de distribución` |
| 1 | Tienda | `Código 2 de regla de distribución` |
| 2 | Sucursal | `Nombre sucursal` |
| 3 | Trimestre | `Calendario[Date].[Trimestre]` |
| 4 | Mes | `Calendario[Date].[Mes]` |
| 5 | Año | `Calendario[Date].[Año]` |

**`KPI Export (Dim)`** — tabla calculada que hace `UNION` de los valores distintos de las seis dimensiones, cada bloque etiquetado con su `Dimensión Finanzas Orden`. Es decir: una tabla larga `(orden, valor)` que contiene *todos* los valores posibles de *todas* las dimensiones.

**Las medidas `(vis)`** — cada KPI del informe tiene una variante que lee el orden seleccionado y aplica `TREATAS` de los valores de esa dimensión sobre la columna correspondiente de los hechos:

```dax
CN (vis) =
VAR SelOrden = SELECTEDVALUE ( 'Dimensión Finanzas'[Dimensión Finanzas Orden], 0 )
VAR DimVals  = CALCULATETABLE (
                   VALUES ( 'KPI Export (Dim)'[Dimensión] ),
                   KEEPFILTERS ( 'KPI Export (Dim)'[Dimensión Finanzas Orden] = SelOrden ) )
RETURN SWITCH ( SelOrden,
    0, CALCULATE ( [PyG 01.ImporteNeto], TREATAS ( DimVals, LOL_PBIFINANCIALENTRIES[DistributionRuleCode1] ) ),
    1, CALCULATE ( [PyG 01.ImporteNeto], TREATAS ( DimVals, LOL_PBIFINANCIALENTRIES[DistributionRuleCode2] ) ),
    2, CALCULATE ( [PyG 01.ImporteNeto], TREATAS ( DimVals, LOL_PBIFINANCIALENTRIES[BranchCode] ) ),
    3, CALCULATE ( [PyG 01.ImporteNeto], TREATAS ( DimVals, 'Calendario'[Date].[Trimestre] ) ),
    4, CALCULATE ( [PyG 01.ImporteNeto], TREATAS ( DimVals, 'Calendario'[Mes] ) ),
    5, CALCULATE ( [PyG 01.ImporteNeto], TREATAS ( DimVals, 'Calendario'[Año] ) ) )
```

Este bloque de ~20 líneas está **repetido tal cual en unas 25 medidas** (`CN`, `AP`, `OP`, `NETOAI`, sus `_LY`, y todas las `… DIM` de P&G y P&G Gestión). Ver §7.

`Dimensión Finanzas` aloja además 4 medidas de apoyo (`Param DimFin (texto)`, `DimFin Orden (vis)`, `Título Descripción (vis)` — que resuelve el nombre legible del centro de coste vía `CostCenter`/`CostCenter_Dim2` —, y `Filtro Dimensión Activa (vis)`), y `KPI Export (Dim)` una (`Filtro Dimensión (TREATAS)`).

**`Páginas`** (`GENERATESERIES(1,4,1)`) pagina el listado: sus medidas `Max Páginas` / `Mostrar Página` permiten 4 páginas solo cuando la dimensión elegida es "Tienda", y 1 en el resto.

---

## 6. Medidas (118 en `Medidas Finanzas` + 19 auxiliares)

Organizadas en carpetas:

### `01 Base` (8)
El núcleo de balance. `Sum Amount`, `Importe Neto` (ambos `CreditLC − DebitLC`), `Ventas_YTD` y `VENTAS_YTD_LY` (con `CORRECION` y ventana del slicer), `Total Base YTD` / `Total Base YTD_LY` (los anteriores con el filtro de profundidad de jerarquía), `Balance Evol.` y `Balance Porc Evol`.

### `02 P&G` (24) y `02 P&G\Por dimension` (17)
Cuenta de resultados oficial. Los cuatro subtotales `PyG_A/B/C/D` y sus `_LY`, el enrutador `PyG Sum_Switch` (+`_LY`), los envoltorios `Total Base YTD PYG` (+`_LY`), la evolución (`PyG Evol.`, `PyG Porc Evol`) y dos líneas de referencia usadas como denominador:

- **`PyG 01.ImporteNeto`** — cifra de negocio: `ORDER_N3 = 5003`, excluyendo `DocumentTypeCode` `"-3"` y `"-2"`.
- **`PyG 04.Aprovisionamiento`** — `ORDER_N3 = 5011`.

`PyG %CN` = `DIVIDE([Total Base YTD PYG], [PyG 01.ImporteNeto])` da el porcentaje sobre cifra de negocio de cada línea del P&G. La subcarpeta `Por dimension` replica todo lo anterior con el patrón `(vis)`/`DIM` de §5.

### `03 P&G Gestion` (22) y `03 P&G Gestion\Por dimension` (20)
Versión de gestión, con su propio corte de cuentas:

| Medida | Selector |
|---|---|
| `PyGG CN` | `ORDER_N3 = 7003` |
| `PyGG CONSUMO MERCADERIAS` | `ORDER_N3 = 7013` |
| `PyGG TOTAL GASTOS` | `NIVEL_1 = "GASTOS"` |
| `PyGG Alquiler` | `ORDER_N3 = 7033` |
| `PyGG Personas` | `ORDER_N3 = 7027` |
| `PyGG MARGEN BRUTO` | `[PyGG CN] + [PyGG CONSUMO MERCADERIAS]` |

Más los ratios de gestión: `PyGG MargenBruto sobre ventas %`, `PyGG Alquiler sobre ventas %`, `PyGG Personas sobre ventas %`, `PyGG EBITDA SOBRE VENTAS`, `PyGG %CN` (+`_LY`, +`DIM`) y las evoluciones.

### `04 Margenes y KPIs` (23)
Los KPIs de tarjeta, todos en variante `(vis)` (sensibles a la dimensión seleccionada):

| KPI | Base |
|---|---|
| `CN (vis)` / `CN LY (vis)` | Cifra de negocio |
| `AP (vis)` / `AP LY (vis)` | Aprovisionamiento |
| `OP (vis)` / `OP LY (vis)` | Resultado de explotación |
| `NETOAI (vis)` / `NETOAI LY (vis)` | Resultado antes de impuestos |
| `Margen Bruto (vis)` | `DIVIDE ( CN + AP, CN )` |
| `Margen Operativo (vis)` | `DIVIDE ( OP, CN )` |
| `Margen Neto A.I. (vis)` | `DIVIDE ( NETOAI, CN )` |
| `Δ CN % (vis)`, `Δ MB (p)`, `Δ MO (p)`, `Δ MN (p)` | Variaciones interanuales (las `Δ M*` en **puntos**, no en %) |

### `05 Diagnostico` (4)
Medidas de trabajo, no de producción: `AñoPrevio_test`, `DBG Filas año previo`, `Media_Saldo_MesFin_Últimos12_PorCuenta` (saldo medio de cuentas 430/431/432 con interlocutor `M*` sobre los 12 cierres mensuales anteriores) y su variante `TABLA`.

---

## 7. Notas y deuda técnica

1. **Migración a medias.** Las medidas ya leen `LOL_PBIFINANCIALENTRIES`, pero siguen dependiendo de `JournalEntryItem` (HANA):
   - Las particiones calculadas de **`Dimensión Finanzas`** y **`KPI Export (Dim)`** se construyen con `NAMEOF`/`SUMMARIZE` sobre columnas de `JournalEntryItem`.
   - `Título Descripción (vis)` y `Filtro Dimensión (TREATAS)` hacen `TREATAS` contra columnas de `JournalEntryItem`.
   - `DBG Filas año previo` cuenta filas de `JournalEntryItem`.
   - `Media_Saldo_MesFin_Últimos12_PorCuenta` mezcla las dos: filtra importes de `LOL_PBIFINANCIALENTRIES` pero usa `FILTER ( ALL ( JournalEntryItem ), … )` como envoltorio.

   Mientras `JournalEntryItem` siga en el modelo, el refresco necesita conexión a HANA. Para completar la migración hay que reescribir esas cinco piezas contra la tabla SQL. **Ojo:** `Dimensión Finanzas` y `KPI Export (Dim)` son quienes alimentan todo el desglose por dimensión, así que el cambio afecta a las ~25 medidas `(vis)`/`DIM`.

2. **Dos relaciones a `LOL_PBIFINANCIALMODEL`.** Existe la del modelo nuevo (`LOL_PBIFINANCIALENTRIES[AccountCode]`, **bidireccional**) y la del legado (`JournalEntryItem[Código de cuenta]`). Al retirar HANA hay que quitar la segunda. La bidireccionalidad de la primera es necesaria para que el filtro de plan de cuentas llegue a los apuntes, pero conviene revisarla si se añaden más tablas de hechos.

3. **Medidas duplicadas.** `CN (vis)2`, `Margen Bruto (vis)2`, `Margen Operativo (vis)2` son copias literales de sus originales (`Margen Bruto (vis)2` incluso llama a `[CN (vis)]`, no a `[CN (vis)2]`). Consolidar.

4. **El bloque `SWITCH`+`TREATAS` está repetido ~25 veces.** Cualquier cambio en las dimensiones (añadir una, cambiar la columna de origen) obliga a tocar 25 medidas. Se podría reducir a una medida de filtro reutilizable — `Filtro Dimensión (TREATAS)` en `KPI Export (Dim)` ya es un intento en esa dirección, pero no se usa. Es el mejor candidato a refactor.

5. **`PyG Porc Evol` divide por el año actual, no por el anterior.**
   ```dax
   PyG Porc Evol = DIVIDE ( [Total Base YTD PYG] - [Total Base YTD_LY PYG], [Total Base YTD PYG] )
   ```
   La convención habitual de variación interanual es dividir por el periodo **anterior**. `Balance Porc Evol` sí usa `[VENTAS_YTD_LY]` como denominador. Verificar cuál es el criterio querido: hoy no son coherentes entre sí.

6. **`PyG_D RESULTADO EJERCICIO` compara columnas sin agregar.** Usa `LOL_PBIFINANCIALMODEL[VISUAL_ORDER] > LOL_PBIFINANCIALMODEL[PyG_POSICION_C…]` (referencia directa a columna) mientras las medidas A y B usan `AVERAGE(…)`. Además, a diferencia de A/B/C, su `REMOVEFILTERS` **omite `NIVEL_4`**. Ambas cosas parecen descuidos: revisar que el resultado del ejercicio cuadre.

7. **Inconsistencia en `PyG 01.ImporteNeto` vs su `_LY`.** La medida del año actual excluye `DocumentTypeCode` `"-3"` y `"-2"`; la `_LY` **no** los excluye. La variante `PyG 01.ImporteNeto_LY_complete` sí los excluye y usa `PREVIOUSYEAR` en vez de `SAMEPERIODLASTYEAR`. Hay tres definiciones distintas de la misma cifra conviviendo.

8. **`Calendario` fijo hasta 2029.** `CALENDAR(DATE(2023,1,1), DATE(2029,12,31))`: arrastra años futuros vacíos. En SgiRetail se resolvió con `MAX(TODAY(), MAX(<fecha de hechos>))`; se puede aplicar el mismo patrón.

9. **Fecha/hora automática activada** con 7 `LocalDateTable_*`. Con `Calendario` propio son redundantes.

10. **`Páginas` está desconectada y depende de texto.** `Max Páginas` decide comparando `[Param DimFin (texto)]` con la cadena `"Tienda"` (y con `CONTAINSSTRING` del nombre de columna). Es frágil: renombrar la etiqueta del *field parameter* rompe la paginación en silencio. Mejor comparar contra `[DimFin Orden (vis)] = 1`.

11. **`NIVEL3_BOLD` usa Unicode Mathematical Bold.** Funciona visualmente pero esos caracteres no son texto ordinario: rompen la ordenación alfabética, la búsqueda y la exportación a Excel. Si el objetivo es solo destacar la fila, el formato condicional del visual es más limpio.

12. **`cultures/es-ES.tmdl`** (19.691 líneas) solo contiene metadatos lingüísticos de Q&A auto-generados.

---

## 8. Informe

`Finanzas.Report`, 8 páginas:

| # | Página | Contenido |
|---:|---|---|
| 1 | **Balance** | Matriz del balance por plan de cuentas + tarjetas de total y evolución (`Total Base YTD`, `Total Base YTD_LY`, `Balance Evol.`, `Balance Porc Evol`). |
| 2 | **Balance Operativo** | Misma estructura, acotada a las cuentas operativas. |
| 3 | **Pérdida y Ganancias** | Matriz del P&G oficial con las columnas año / año anterior / evolución / %CN, rotuladas con las medidas de título. |
| 4 | **PyG Tarjetas** | Panel de KPIs `(vis)`: cifra de negocio, márgenes bruto/operativo/neto y sus deltas, todo reagrupable por la dimensión seleccionada. Incluye un **visual de Python**. |
| 5 | **PyG Listado Agrupado** | El P&G desglosado por la dimensión activa (medidas `… DIM`). |
| 6 | **PyG Gestión** | Cuenta de gestión: margen bruto, total gastos, %CN y evolución. |
| 7 | **PyG Gestión Listado Agrupado** | La de gestión desglosada por dimensión, con los ratios de alquiler, personas y EBITDA sobre ventas. |
| 8 | **Página 1** | Página de trabajo: solo la tarjeta de `Media_Saldo_MesFin_Últimos12_PorCuenta` y un segmentador. |

> La **página 4 depende de un visual de Python**, lo que exige runtime de Python (en el servicio, una puerta de enlace personal configurada). Tenerlo en cuenta al publicar.

---

### Anexo · Consultar el modelo en vivo (DAX)

Con Power BI Desktop abierto: localizar el proceso `msmdsrv` y su puerto (`Get-NetTCPConnection -OwningProcess <pid> -State Listen`), conectar con el cliente ADOMD del GAC (`Data Source=localhost:<puerto>`) y ejecutar DAX. Tras editar `.tmdl`, Desktop **no** relee en caliente: cerrar (sin guardar) y reabrir el `.pbip`.
