# Prompt para Claude Code · SÓLO versión móvil

> Pega este prompt en Claude Code después de abrirlo en la carpeta donde tienes
> el handoff (`design_handoff_paneles_movil/`).

---

## Prompt (cópialo tal cual)

```
Tengo un panel de Power BI Desktop YA FUNCIONANDO con sus medidas DAX
creadas y conectadas al modelo de datos. NO necesito que crees medidas
nuevas ni toques el modelo.

Tu trabajo es ayudarme a montar SOLO la versión móvil del panel,
siguiendo el diseño descrito en esta carpeta.

CONTEXTO:
- Las medidas ya existen en el modelo (Venta Neta, Objetivo, Venta PY,
  Ticket Medio, UPT, % Conversión, % Atracción, % vs Objetivo, % vs PY,
  etc.). Si la guía DAX menciona una medida con otro nombre, asume que
  ya tengo el equivalente y úsalo tal cual.
- El tema JSON puede estar o no aplicado — pregúntame antes de tocarlo.
- IGNORA por ahora la versión de escritorio. La haremos en una segunda
  fase. Sólo trabajamos el panel móvil.

LO QUE NECESITO DE TI (en este orden, esperando confirmación entre pasos):

1. Lee:
   - `README.md` (visión general y design tokens)
   - `powerbi/MOBILE_LAYOUT_GUIDE.md` (receta paso a paso)
   - `screens.jsx` y `screens-detail.jsx` (referencia visual)

2. Pregúntame los NOMBRES REALES de mis medidas si difieren de las
   genéricas usadas en la documentación. Lista lo que necesitas saber
   antes de empezar (medidas, dimensiones, relaciones).

3. Una vez tengamos los nombres, genérame un guion VISUAL POR VISUAL
   para construir la Página ① "Móvil – Total" en Power BI Desktop. Cada
   instrucción debe incluir:
   - Tipo exacto del visual (Card, Shape, Bar chart 100% Stacked, etc.)
   - Qué medida o dimensión asignar
   - Propiedades de formato: fontFamily, fontSize, color hex, position x/y,
     size w/h, background, border, conditional formatting
   - Filtro aplicado al visual (si lo necesita)
   - Workaround concreto si el diseño pide algo que Power BI no soporta

   Hazlo SECUENCIALMENTE bloque por bloque (header → period → hero KPI →
   grid 2×2 → tarjetas de canal). No me des todo de golpe; espera que
   te confirme cada bloque antes del siguiente.

4. Cuando la página ① esté lista, pasamos a la ② (detalle de canal Tienda)
   y luego a la ③ (detalle de tienda concreta, con drillthrough).

5. AL FINAL: dame la checklist para activar el Mobile Layout y verificar
   que se ve bien en Power BI Mobile app.

REGLAS:
- No reinventes la rueda. Sigue al pie de la letra MOBILE_LAYOUT_GUIDE.md.
- Si algo de la guía te parece mejorable, dilo, pero no lo cambies sin
  preguntar.
- Sé concreto con coordenadas y tamaños — odio el "ajusta a mano".
- Si Power BI no permite algo, dilo con claridad y propón el workaround
  más cercano al diseño original.

Empieza por el paso 1 + 2: léete los archivos y pregúntame los nombres
de mis medidas.
```

---

## Notas para ti

- Después de que Claude Code te haya terminado la versión móvil, vuelve aquí y te genero el **mockup hi-fi de la versión escritorio** (manteniendo el mismo lenguaje visual: header rosa, hero navy, tarjetas claras, semáforo, etc.).
- Si Claude Code te pregunta nombres de medidas y no las recuerdas: en Power BI Desktop, panel de campos a la derecha → busca por carpetas de medidas. Cópialas y pégalas en el chat.
- Si algo no entra en el lienzo móvil de 320×640, **reduce font-size en la versión desktop** del visual (no en mobile layout — ahí no se puede).
