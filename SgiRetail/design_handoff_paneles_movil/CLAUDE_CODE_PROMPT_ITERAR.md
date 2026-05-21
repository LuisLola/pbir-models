# Prompt para Claude Code · Cuando el resultado NO se ve como el diseño

Cuando Claude Code te diga "todo implementado" pero al abrir Power BI Desktop el panel no se parece al mockup, **NO sigas adelante** — pídele que itere visualmente.

---

## Adjunta la imagen de referencia

Antes de mandar este prompt, sube a Claude Code la captura **`screenshots/all-3-levels.png`** (está en este mismo handoff). Es la referencia visual definitiva — las 3 pantallas (① Total, ② Canal Tienda, ③ Detalle Tienda) lado a lado a escala real.

Si Claude Code es CLI y no acepta imágenes, sube además **una captura de cómo se ve tu panel actual en Power BI Desktop** y pega la ruta del archivo en el mensaje.

---

## Prompt (cópialo tal cual)

```
El panel móvil que has generado en mi .pbip NO se parece visualmente al
diseño. Mira las dos imágenes:

A) screenshots/all-3-levels.png   ← cómo DEBE verse (referencia)
B) [adjunta o describe cómo se ve ahora en Power BI Desktop]

NO me digas "está implementado" — está implementado a nivel de datos,
pero no a nivel visual. Hay diferencias claras de:
  - Tamaño de fuente del número hero (debe ser muy grande, ≥36pt)
  - Color de fondo del bloque hero (debe ser navy #10182F, no blanco)
  - Color del header (debe ser fucsia #E11A6F)
  - Espaciado entre tarjetas de canal (deben verse separadas con borde fino)
  - Píldoras de estado con punto de color + fondo pastel (verde/amarillo/rojo)
  - Barra de progreso fucsia debajo del número hero

Tu tarea ahora es:

1. Listar en una tabla las DIFERENCIAS visuales entre A y B, una a una.
   Sé concreto: "El número 174.741 se ve a 18pt en mi panel, debe ser 42pt"
   en lugar de "el número es más pequeño".

2. Por cada diferencia, decirme:
   - QUÉ propiedad del JSON del .pbip hay que cambiar (path completo,
     ej: `report.json` → `sections[0].visualContainers[3].config.singleVisual...`)
   - QUÉ valor poner exactamente
   - SI requiere un workaround (porque Power BI no soporta esa propiedad
     directamente), cuál es

3. Aplicar los cambios y darme el .pbip corregido.

4. Antes de declarar "hecho", ABRIR mentalmente el resultado y compararlo
   con la imagen de referencia. Si todavía hay diferencias obvias,
   itera otra vez.

Reglas:
- NO añadas medidas DAX nuevas, las mías ya funcionan.
- NO toques el modelo de datos.
- SOLO cambia la presentación visual: posiciones, tamaños, colores,
  fuentes, fondos, bordes, conditional formatting.
- Si una propiedad del JSON del .pbip no la conoces, dilo claramente
  y propón una alternativa en lugar de inventarla.
- Si Power BI Desktop no soporta algo del diseño (gradientes, letter-spacing,
  border-radius por lado, etc.), DILO y propón el workaround del MOBILE_LAYOUT_GUIDE.md
  sección 5.

Empieza por hacer la TABLA DE DIFERENCIAS de la página ①. Pausa
después de la tabla; espera que confirme antes de aplicar cambios.
```

---

## Si Claude Code sigue sin entender

Es posible que Claude Code esté trabajando con una representación abstracta
del .pbip (JSON de configuración) y NO esté visualizando realmente cómo se
renderiza. En ese caso:

1. **Hazle abrir el .pbip en Power BI Desktop tú mismo** y haz screenshots.
2. **Compártelos pantalla a pantalla** con el chat.
3. **Pídele cambios concretos visual por visual**, no globales.

Ejemplo de mensaje de iteración:

```
Adjunto screenshot de mi Power BI Desktop página "Móvil – Total" tal
como se ve AHORA, comparado con screenshots/all-3-levels.png (columna ①).

Diferencias que veo y que tienes que arreglar:

1. El header rosa NO está. Aparece un texto "LOLA CASADEMUNT" sobre fondo
   blanco. → Añade un Shape rectangle de fondo `#E11A6F` size 390x64
   detrás del wordmark.

2. El número 174.741 se ve sobre fondo blanco. → Debería tener fondo navy
   `#10182F`. Añade un Shape rectangle detrás del Card.

3. Las píldoras "-14% vs OBJ" se ven como texto plano. → Convierte cada
   una en un Card pequeño con:
   - Background condicional usando medida `[Bg vs Obj]`
   - Font color condicional usando medida `[Color vs Obj]`
   - Rounded corners 6 en Visual border

Aplica estos 3 cambios y dame el .pbip actualizado.
```

---

## Conceptos que ayudan a Claude Code

- **Shape Rectangle** = la herramienta para hacer fondos de color sólido
  detrás de Cards (no existe `background-color` en visuales).
- **Bar chart 100% Stacked** = el truco para hacer barras de progreso.
- **Conditional formatting con Field value** = la única forma de aplicar
  colores dinámicos basados en medidas DAX.
- **Drillthrough page** = la forma de navegar de canal → detalle.
- **Mobile Layout** = vista separada, sólo reorganiza visuales — no permite
  crear nuevos.

Si Claude Code se desvía de estos conceptos, recuérdaselos.
