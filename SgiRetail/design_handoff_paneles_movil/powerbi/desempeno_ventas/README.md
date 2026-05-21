# Página "Desempeño de Ventas" · Vista escritorio

Esta carpeta contiene todo lo necesario para implementar la página de escritorio (vista única con tabla expandible jerárquica, estilo BI2GO).

## Archivos

| Archivo | Propósito |
|---|---|
| `CLAUDE_CODE_PROMPT_DESKTOP.md` | Prompt listo para pegar en Claude Code. Empieza por aquí. |
| `DESKTOP_LAYOUT_GUIDE.md` | Guía manual paso a paso por visual (si prefieres construirlo a mano). |
| `measures-desempeno.dax` | Medidas DAX adicionales (time-buckets + semáforos). |

## Imagen de referencia

`../../screenshots/desempeno-ventas-desktop.png` — la captura definitiva del diseño completo.

## Resumen del diseño

```
┌────────────────────────────────────────────────────────────────────┐
│ HEADER · 64px                                                       │
│ Logo LC + "Desempeño de ventas" ♥        [Año|Mes|Sem|Día] [← fecha]│
├────────────────────────────────────────────────────────────────────┤
│ STRIP DE 4 KPIs · 180px                                             │
│ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                    │
│ │V.NETA   │ │T.MEDIO  │ │UPT      │ │%CONV    │ con sparkline      │
│ │navy hero│ │blanco   │ │blanco   │ │blanco   │ + 4 buckets        │
│ └─────────┘ └─────────┘ └─────────┘ └─────────┘                    │
├────────────────────────────────────────────────────────────────────┤
│ FILTER CHAIN · 56px                                                 │
│ Desempeño por: [Tipo Tienda▾] [Tienda▾] [Categoría▾] [Familia▾]    │
├────────────────────────────────────────────────────────────────────┤
│ MATRIX EXPANDIBLE                                                    │
│ ─ TOTALES        │100%│55.013│96.000│-43%│ ...                      │
│ + TIENDA         │ 40%│22.015│37.950│-42%│ ...                      │
│ + CORNER ECI     │ 36%│19.745│33.350│-41%│ ...                      │
│ ─ ONLINE         │ 18%│ 9.983│13.520│-26%│ ...                      │
│   ─ T.Web (Prest)│ 73%│ 7.280│10.400│-30%│ ...                      │
│       Prenda     │ 66%│ 4.789│     0│ +0%│ ...                      │
│       Accesorio  │ 34%│ 2.491│     0│ +0%│ ...                      │
│   ECI Online     │ 27%│ 2.703│ 3.120│-13%│ ...                      │
│ + OUTLET         │  6%│ 3.270│11.180│-71%│ ...                      │
├────────────────────────────────────────────────────────────────────┤
│ FOOTER · 40px                                                        │
│ ● Positivo  ● Atención  ● Crítico    Lola Casademunt · CdM comercial │
└────────────────────────────────────────────────────────────────────┘
```

## Concepto clave

**Usa el visual MATRIX**, no Table. Es el único que soporta jerarquías expandibles con iconos `+`/`−`. Con `Drill mode → Expand` activado tienes el comportamiento BI2GO replicado.

## Orden de trabajo recomendado

1. Lee `CLAUDE_CODE_PROMPT_DESKTOP.md` y pégalo en Claude Code.
2. Claude Code te preguntará por nombres de dimensiones y medidas existentes — respóndele.
3. Crea las medidas DAX nuevas (las del `.dax` que no tengas).
4. Construye la página visual a visual, validando con `screenshots/desempeno-ventas-desktop.png` después de cada bloque.
5. Verifica que el drill expand funcione (click `+` debe abrir el nivel siguiente).
6. Aplica conditional formatting a todas las columnas de Evol % para activar el semáforo.

## Tiempo estimado

- Si Claude Code va bien: **2-3 horas** para tener la página completa.
- Si lo haces a mano siguiendo `DESKTOP_LAYOUT_GUIDE.md`: **3-5 horas**.

## Lo que NO está en este handoff (porque Power BI no lo soporta bien)

- Puntos de color delante del % en las píldoras (no soportado en Matrix).
- Sparklines integrados dentro del Card visual (usa Line chart al lado).
- Esquinas redondeadas en headers de Matrix.
- Navegador de fecha con flechas dinámicas — requiere bookmarks complejos. Lo más pragmático es usar un slicer entre fechas estándar y omitir las flechas.

Si necesitas TODO al pixel, considera una app web custom en vez de Power BI. Pero eso queda fuera del alcance de este handoff.
