/* =====================================================================
   Vista de envíos para el Panel de Seguimiento de Transportes
   Origen: dbo.LOL_TRANSPORT_TRACKING   (SQL Server, 192.168.0.232 / hana_etl_admin)
   ---------------------------------------------------------------------
   Si el esquema no es dbo, ajústalo en ambos sitios.
   ===================================================================== */
CREATE OR ALTER VIEW dbo.vw_envios AS
WITH base AS (
    SELECT
        e.*,
        -- 2.1 transportista normalizado (TIPSA* -> TIPSA)
        CASE WHEN e.transportista LIKE 'TIPSA%' THEN 'TIPSA' ELSE e.transportista END AS transportista_norm,
        -- 2.3 entregado
        CASE WHEN e.fecha_entregado IS NOT NULL OR e.estado = 'Entregado' THEN 1 ELSE 0 END AS es_entregado,
        -- 2.2 punto de inicio (date-level) por prioridad: tránsito fiable -> recogido fiable -> lanzamiento
        CAST(
            COALESCE(
                CASE WHEN e.fecha_en_transito IS NOT NULL AND CAST(e.fecha_en_transito AS DATE) <> CAST(e.fecha_entregado AS DATE) THEN e.fecha_en_transito END,
                CASE WHEN e.fecha_recogido    IS NOT NULL AND CAST(e.fecha_recogido    AS DATE) <> CAST(e.fecha_entregado AS DATE) THEN e.fecha_recogido    END,
                e.fecha_lanzamiento
            ) AS DATE
        ) AS f_inicio
    FROM dbo.LOL_TRANSPORT_TRACKING e
)
SELECT
    base.*,
    -- 2.6 devuelto
    CASE WHEN estado = 'Devuelto' OR fecha_devuelto IS NOT NULL THEN 1 ELSE 0 END AS es_devuelto,
    -- 2.5 incidencia bloqueante (incidencia Y no entregado)
    CASE WHEN (fecha_incidencia IS NOT NULL OR estado = 'Incidencia') AND es_entregado = 0 THEN 1 ELSE 0 END AS es_incidencia_bloqueante,
    -- mes de lanzamiento (último día del mes, para eje temporal)
    EOMONTH(fecha_lanzamiento) AS mes_lanzamiento,
    -- 2.2 tiempo de entrega (solo si entregado, coherente y < 60 días naturales)
    CASE
        WHEN fecha_entregado IS NOT NULL
         AND CAST(fecha_entregado AS DATE) >= f_inicio
         AND DATEDIFF(DAY, f_inicio, CAST(fecha_entregado AS DATE)) < 60
        THEN DATEDIFF(DAY, f_inicio, CAST(fecha_entregado AS DATE))
    END AS dias_entrega_natural,
    CASE
        WHEN fecha_entregado IS NOT NULL
         AND CAST(fecha_entregado AS DATE) >= f_inicio
         AND DATEDIFF(DAY, f_inicio, CAST(fecha_entregado AS DATE)) < 60
        THEN DATEDIFF(DAY, f_inicio, CAST(fecha_entregado AS DATE))
           - DATEDIFF(WEEK, f_inicio, CAST(fecha_entregado AS DATE)) * 2   -- quita sáb+dom (viernes->lunes = 1)
    END AS dias_entrega_laborables,
    -- 2.7 fases del ciclo (días con decimales; filtra >= 0 en las medidas)
    DATEDIFF(MINUTE, fecha_lanzamiento,  fecha_en_transito) / 1440.0 AS dur_lanz_transito,
    DATEDIFF(MINUTE, fecha_en_transito,  fecha_en_reparto)  / 1440.0 AS dur_transito_reparto,
    DATEDIFF(MINUTE, fecha_en_reparto,   fecha_entregado)   / 1440.0 AS dur_reparto_entrega
FROM base;
