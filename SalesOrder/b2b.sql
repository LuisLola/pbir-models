WITH PedidosBase AS (
  SELECT
    'Pedido' AS "TipoRegistro",
    T0."DocEntry",
    T1."LineNum",
    CAST(T0."DocEntry" AS NVARCHAR(30)) || '-' || CAST(T1."LineNum" AS NVARCHAR(30)) AS "LineID",
    T0."DocNum",
    T0."CANCELED" AS "Cancelado",
    T0."DocStatus" AS "EstadoDocumento",
    T1."LineStatus" AS "EstadoLinea",
    T0."CardCode" AS "ClienteKey",
    T1."SlpCode" AS "AgenteKey",
    T1."WhsCode" AS "AlmacenKey",
    T1."ItemCode" AS "ArticuloKey",
    T1."U_GSP_SEASON" AS "TemporadaKey",
    T1."U_LOL_REP" AS "ReposicionKey",
    CAST(T0."DocDate" AS DATE) AS "FechaDocumento",
    CAST(T0."DocDueDate" AS DATE) AS "FechaEntrega",
    CAST(T0."U_GSP_UPDATE_DATE" AS DATE) AS "FechaComunicacion",
    T0."UserSign" AS "UsuarioSapKey",
    CAST(NULL AS NVARCHAR(30)) AS "IncidenciaKey",
    CAST(T1."Quantity" AS DECIMAL(19,6)) AS "Unidades",
    CAST(T1."OpenQty" AS DECIMAL(19,6)) AS "UnidadesPendientes",
    T1."Currency" AS "MonedaDocumento",
    COALESCE(T1."Rate", 1) AS "TipoCambioLinea",
    CAST(T1."PriceBefDi" AS DECIMAL(19,6)) AS "PrecioBrutoDoc",
    CAST(T1."Price" AS DECIMAL(19,6)) AS "PrecioNetoDoc",
    CAST(T1."DiscPrcnt" AS DECIMAL(19,6)) AS "DescuentoComercialPct",
    CAST(COALESCE(T0."DiscPrcnt", 0) AS DECIMAL(19,6)) AS "DescuentoCabeceraPct",
    CAST(T1."PriceBefDi" * T1."Quantity" AS DECIMAL(19,6)) AS "ImporteBrutoDoc",
    CAST((T1."PriceBefDi" - T1."Price") * T1."Quantity" AS DECIMAL(19,6)) AS "ImporteDescuentoComercialDoc",
    CAST(T1."Price" * T1."Quantity" AS DECIMAL(19,6)) AS "ImporteNetoLineaDoc",
    CAST(T1."LineTotal" AS DECIMAL(19,6)) AS "ImporteNetoLineaEUR",
    CAST(COALESCE(T0."TotalExpns", 0) AS DECIMAL(19,6)) AS "PortesCabeceraEUR"
  FROM ORDR T0
  JOIN RDR1 T1 ON T0."DocEntry" = T1."DocEntry"
  JOIN OCRD T2 ON T0."CardCode" = T2."CardCode"
  WHERE
    T2."GroupCode" IN ('105', '108')
    AND T0."CANCELED" = 'N'
    AND SUBSTRING(T1."ItemCode", 5, 2) NOT IN ('98', '99')
    AND COALESCE(T1."U_GSP_SEASON", '') = '[%0]'
    AND NOT EXISTS (
      SELECT 1
      FROM "@GSP_BSLOGINCIDE" TT0
      LEFT JOIN "@GSP_TCLOGINCIDELIN" TT1
        ON TT0."Code" = TT1."U_GSP_LOGINCIDECODE"
      WHERE
        TT0."U_GSP_DOCENTRY" = T1."DocEntry"
        AND TT1."U_GSP_MODELNUMLINE" = T1."U_GSP_MODELNUMLINE"
        AND T1."LineStatus" = 'C'
    )
),
Pedidos AS (
  SELECT
    B.*,
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."PrecioBrutoDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."PrecioBrutoDoc"
    END AS "PrecioBrutoEUR",
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."PrecioNetoDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."PrecioNetoDoc"
    END AS "PrecioNetoEUR",
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."ImporteBrutoDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."ImporteBrutoDoc"
    END AS "ImporteBrutoEUR",
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."ImporteDescuentoComercialDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."ImporteDescuentoComercialDoc"
    END AS "ImporteDescuentoComercialEUR",
    CASE
      WHEN SUM(B."ImporteNetoLineaEUR") OVER (PARTITION BY B."DocEntry") = 0 THEN 0
      ELSE
        (B."ImporteNetoLineaEUR" * B."DescuentoCabeceraPct" / 100)
    END AS "ImporteDescuentoCabeceraEUR",
    B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100) AS "ImporteDespuesDescuentoCabeceraEUR",
    CASE
      WHEN SUM(B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100)) OVER (PARTITION BY B."DocEntry") = 0 THEN 0
      ELSE B."PortesCabeceraEUR" *
        (B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100)) /
        SUM(B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100)) OVER (PARTITION BY B."DocEntry")
    END AS "ImportePortesDistribuidosEUR"
  FROM PedidosBase B
),
IncidenciasBase AS (
  SELECT
    'Incidencia' AS "TipoRegistro",
    O."DocEntry",
    COALESCE(L."U_GSP_MODELNUMLINE", HLINE."LineNum") AS "LineNum",
    CAST(O."DocEntry" AS NVARCHAR(30)) || '-' || CAST(COALESCE(L."U_GSP_MODELNUMLINE", HLINE."LineNum") AS NVARCHAR(30)) AS "LineID",
    O."DocNum",
    'Y' AS "Cancelado",
    O."DocStatus" AS "EstadoDocumento",
    'C' AS "EstadoLinea",
    O."CardCode" AS "ClienteKey",
    O."SlpCode" AS "AgenteKey",
    COALESCE(HLINE."WhsCode", '4-C001L') AS "AlmacenKey",
    L."U_GSP_ITEMCODE" AS "ArticuloKey",
    O."U_GSP_SEASON" AS "TemporadaKey",
    O."U_GSP_REP" AS "ReposicionKey",
    CAST(O."DocDate" AS DATE) AS "FechaDocumento",
    CAST(O."DocDueDate" AS DATE) AS "FechaEntrega",
    CAST(O."U_GSP_UPDATE_DATE" AS DATE) AS "FechaComunicacion",
    T."U_GSP_USER" AS "UsuarioSapKey",
    T."U_GSP_CODEINCIDE" AS "IncidenciaKey",
    CAST(COALESCE(L."U_GSP_QTY", 0) - COALESCE(L."U_GSP_QTYOLD", 0) AS DECIMAL(19,6)) AS "Unidades",
    CAST(0 AS DECIMAL(19,6)) AS "UnidadesPendientes",
    HLINE."Currency" AS "MonedaDocumento",
    COALESCE(HLINE."Rate", 1) AS "TipoCambioLinea",
    CAST(HLINE."PriceBefDi" AS DECIMAL(19,6)) AS "PrecioBrutoDoc",
    CAST(HLINE."Price" AS DECIMAL(19,6)) AS "PrecioNetoDoc",
    CAST(HLINE."DiscPrcnt" AS DECIMAL(19,6)) AS "DescuentoComercialPct",
    CAST(COALESCE(HHDR."DiscPrcnt", 0) AS DECIMAL(19,6)) AS "DescuentoCabeceraPct",
    CAST(ABS(COALESCE(L."U_GSP_QTY", 0) - COALESCE(L."U_GSP_QTYOLD", 0)) * HLINE."PriceBefDi" AS DECIMAL(19,6)) AS "ImporteBrutoDoc",
    CAST(ABS(COALESCE(L."U_GSP_QTY", 0) - COALESCE(L."U_GSP_QTYOLD", 0)) * (HLINE."PriceBefDi" - HLINE."Price") AS DECIMAL(19,6)) AS "ImporteDescuentoComercialDoc",
    CAST((COALESCE(L."U_GSP_QTY", 0) - COALESCE(L."U_GSP_QTYOLD", 0)) * HLINE."Price" AS DECIMAL(19,6)) AS "ImporteNetoLineaDoc",
    CAST(
      (COALESCE(L."U_GSP_QTY", 0) - COALESCE(L."U_GSP_QTYOLD", 0)) *
      CASE WHEN HLINE."Currency" <> 'EUR' THEN HLINE."Price" / NULLIF(HLINE."Rate", 0) ELSE HLINE."Price" END
      AS DECIMAL(19,6)
    ) AS "ImporteNetoLineaEUR",
    CAST(CASE WHEN COALESCE(O."TotalExpns", 0) = 0 THEN 0 ELSE COALESCE(HHDR."TotalExpns", 0) END AS DECIMAL(19,6)) AS "PortesCabeceraEUR"
  FROM "@GSP_BSLOGINCIDE" T
  JOIN ORDR O ON T."U_GSP_DOCENTRY" = O."DocEntry"
  JOIN OCRD C ON C."CardCode" = O."CardCode"
  JOIN "@GSP_TCLOGINCIDELIN" L ON T."Code" = L."U_GSP_LOGINCIDECODE"
  LEFT JOIN (
    SELECT
      L1."DocEntry",
      L1."ItemCode",
      L1."LineNum",
      L1."WhsCode",
      L1."Currency",
      L1."Rate",
      L1."PriceBefDi",
      L1."Price",
      L1."DiscPrcnt",
      ROW_NUMBER() OVER (PARTITION BY L1."DocEntry", L1."ItemCode", L1."LineNum" ORDER BY L1."LogInstanc" DESC) AS "rn"
    FROM ADO1 L1
    JOIN ADOC H1
      ON H1."DocEntry" = L1."DocEntry"
      AND H1."LogInstanc" = L1."LogInstanc"
      AND H1."ObjType" = '17'
  ) HLINE
    ON HLINE."DocEntry" = O."DocEntry"
    AND HLINE."ItemCode" = L."U_GSP_ITEMCODE"
    AND HLINE."LineNum" = L."U_GSP_MODELNUMLINE"
    AND HLINE."rn" = 1
  LEFT JOIN (
    SELECT H2."DocEntry", H2."DiscPrcnt", H2."DiscSum", H2."TotalExpns"
    FROM ADOC H2
    JOIN (
      SELECT "DocEntry", MIN("LogInstanc") AS "MinLog"
      FROM ADOC
      WHERE "ObjType" = '17'
      GROUP BY "DocEntry"
    ) M2
      ON M2."DocEntry" = H2."DocEntry"
      AND M2."MinLog" = H2."LogInstanc"
    WHERE H2."ObjType" = '17'
  ) HHDR ON HHDR."DocEntry" = O."DocEntry"
  LEFT JOIN "@GSP_BSINCIDENCE" INC ON T."U_GSP_CODEINCIDE" = INC."Code"
  WHERE
    C."GroupCode" IN ('105', '108')
    AND T."U_GSP_OBJTYPE" = '17'
    AND L."U_GSP_ITEMCODE" IS NOT NULL
    AND T."U_GSP_DATE" > '20240101'
    AND INC."U_U_LOL_VIEWBI2GO" = 'Y'
    AND (COALESCE(L."U_GSP_QTY", 0) - COALESCE(L."U_GSP_QTYOLD", 0)) < 0
    AND SUBSTRING(L."U_GSP_ITEMCODE", 5, 2) NOT IN ('98', '99')
    AND COALESCE(O."U_GSP_SEASON", '') = '[%0]'
),
Incidencias AS (
  SELECT
    B.*,
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."PrecioBrutoDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."PrecioBrutoDoc"
    END AS "PrecioBrutoEUR",
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."PrecioNetoDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."PrecioNetoDoc"
    END AS "PrecioNetoEUR",
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."ImporteBrutoDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."ImporteBrutoDoc"
    END AS "ImporteBrutoEUR",
    CASE
      WHEN B."MonedaDocumento" <> 'EUR' THEN B."ImporteDescuentoComercialDoc" / NULLIF(B."TipoCambioLinea", 0)
      ELSE B."ImporteDescuentoComercialDoc"
    END AS "ImporteDescuentoComercialEUR",
    B."ImporteNetoLineaEUR" * B."DescuentoCabeceraPct" / 100 AS "ImporteDescuentoCabeceraEUR",
    B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100) AS "ImporteDespuesDescuentoCabeceraEUR",
    CASE
      WHEN SUM(B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100)) OVER (PARTITION BY B."DocEntry") = 0 THEN 0
      ELSE B."PortesCabeceraEUR" *
        (B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100)) /
        SUM(B."ImporteNetoLineaEUR" * (1 - B."DescuentoCabeceraPct" / 100)) OVER (PARTITION BY B."DocEntry")
    END AS "ImportePortesDistribuidosEUR"
  FROM IncidenciasBase B
)
SELECT
  "TipoRegistro",
  "DocEntry",
  "LineNum",
  "LineID",
  "DocNum",
  "Cancelado",
  "EstadoDocumento",
  "EstadoLinea",
  "ClienteKey",
  "AgenteKey",
  "AlmacenKey",
  "ArticuloKey",
  "TemporadaKey",
  "ReposicionKey",
  "FechaDocumento",
  "FechaEntrega",
  "FechaComunicacion",
  "UsuarioSapKey",
  "IncidenciaKey",
  "Unidades",
  "UnidadesPendientes",
  "MonedaDocumento",
  "TipoCambioLinea",
  "PrecioBrutoDoc",
  "PrecioBrutoEUR",
  "PrecioNetoDoc",
  "PrecioNetoEUR",
  "DescuentoComercialPct",
  "DescuentoCabeceraPct",
  "ImporteBrutoDoc",
  "ImporteBrutoEUR",
  "ImporteDescuentoComercialDoc",
  "ImporteDescuentoComercialEUR",
  "ImporteNetoLineaDoc",
  "ImporteNetoLineaEUR",
  "ImporteDescuentoCabeceraEUR",
  "ImporteDespuesDescuentoCabeceraEUR",
  "ImportePortesDistribuidosEUR",
  "ImporteDespuesDescuentoCabeceraEUR" + "ImportePortesDistribuidosEUR" AS "ImporteTotalEUR"
FROM Pedidos
UNION ALL
SELECT
  "TipoRegistro",
  "DocEntry",
  "LineNum",
  "LineID",
  "DocNum",
  "Cancelado",
  "EstadoDocumento",
  "EstadoLinea",
  "ClienteKey",
  "AgenteKey",
  "AlmacenKey",
  "ArticuloKey",
  "TemporadaKey",
  "ReposicionKey",
  "FechaDocumento",
  "FechaEntrega",
  "FechaComunicacion",
  "UsuarioSapKey",
  "IncidenciaKey",
  "Unidades",
  "UnidadesPendientes",
  "MonedaDocumento",
  "TipoCambioLinea",
  "PrecioBrutoDoc",
  "PrecioBrutoEUR",
  "PrecioNetoDoc",
  "PrecioNetoEUR",
  "DescuentoComercialPct",
  "DescuentoCabeceraPct",
  "ImporteBrutoDoc",
  "ImporteBrutoEUR",
  "ImporteDescuentoComercialDoc",
  "ImporteDescuentoComercialEUR",
  "ImporteNetoLineaDoc",
  "ImporteNetoLineaEUR",
  "ImporteDescuentoCabeceraEUR",
  "ImporteDespuesDescuentoCabeceraEUR",
  "ImportePortesDistribuidosEUR",
  "ImporteDespuesDescuentoCabeceraEUR" + "ImportePortesDistribuidosEUR" AS "ImporteTotalEUR"
FROM Incidencias;