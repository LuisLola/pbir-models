# Prompt para Claude Code

Copia y pega esto en Claude Code (o usa cualquier variante; lo importante es señalar la carpeta `design_handoff_paneles_movil/`).

---

## Prompt corto (pégalo tal cual)

```
Necesito que me ayudes a implementar un panel móvil para Power BI Desktop.

Toda la documentación está en la carpeta `design_handoff_paneles_movil/`. Lee primero:
1. `README.md` (visión general + design tokens)
2. `powerbi/MOBILE_LAYOUT_GUIDE.md` (instrucciones paso a paso por visual)
3. `powerbi/theme.json` (tema a importar)
4. `powerbi/measures.dax` (medidas DAX a copiar)

Los archivos `.jsx` y `.html` son la referencia visual canónica — abre `Paneles Movil PowerBI.html` y `Flujo Navegacion Conservadora.html` en un navegador para verlos.

Empieza por:
1. Confirmar que entiendes la estructura de mi modelo de datos (tabla FactVentas, DimCanal, DimTienda, DimFecha). Pregúntame los nombres reales si no coinciden.
2. Adaptar `measures.dax` a esos nombres reales y generarme la versión corregida.
3. Adaptar `theme.json` si quiero modo claro / oscuro o quiero cambiar la paleta.
4. Generarme un guion exacto, visual por visual, para construir la Página ① "Móvil – Total" en Power BI Desktop. Incluye: tipo de visual, propiedades de formato (font, color, position, size), filtros aplicados y posibles workarounds.

Trabaja en una pasada por página: primero la ①, luego la ②, luego la ③. No me lo des todo de golpe.
```

---

## Prompt largo (más explícito, si quieres dárselo todo de una vez)

```
Eres un experto en Power BI que me va a guiar a implementar un panel móvil
para Lola Casademunt. Tengo un diseño hi-fi terminado en HTML/React y necesito
trasladarlo a Power BI Desktop.

Restricciones reales:
- Power BI Mobile Layout no es CSS — es un canvas fijo de ~320×640 px donde
  arrastro visuales del desktop. NO hay flex/grid; cada visual se posiciona
  manualmente.
- No puedo crear visuales nuevos en mobile layout; sólo reorganizar los que
  ya existen en la página desktop.
- Tipografías custom (Manrope, Instrument Serif) no funcionan en Power BI
  Service. Usaremos Segoe UI como sustituto.
- Padding/letter-spacing/gradientes/border-radius real no son soportados —
  hay que simularlos con Shape rectangles y workarounds.

Tu trabajo:
1. Leer toda la documentación en `design_handoff_paneles_movil/`.
2. Preguntarme por mi modelo de datos real (nombres de tablas, columnas,
   relaciones). NO asumas, pregunta.
3. Adaptar las medidas DAX a mi modelo.
4. Generar instrucciones paso a paso, visual por visual, para la página ①
   "Móvil – Total". Cada instrucción debe incluir:
   - Tipo de visual exacto (Card, Shape rectangle, Bar chart 100% Stacked, etc.)
   - Datos asignados (qué medida / qué dimensión)
   - Propiedades de formato: font-size, color hex, position x/y, size w/h,
     background, border, conditional formatting.
   - Workaround si el diseño requiere algo que PBI no soporta directamente.
5. Esperar mi confirmación tras cada página antes de pasar a la siguiente.

Empezamos. Lee la documentación y pregúntame los nombres de mi modelo.
```

---

## Ficheros que debe leer Claude Code (en este orden)

| # | Archivo | Por qué |
|---|---|---|
| 1 | `README.md` | Visión general + design tokens + niveles de navegación |
| 2 | `powerbi/MOBILE_LAYOUT_GUIDE.md` | Instrucciones paso a paso por visual |
| 3 | `powerbi/theme.json` | Tema a importar en Power BI |
| 4 | `powerbi/measures.dax` | Medidas DAX listas (ajustar a tu modelo) |
| 5 | `screens.jsx` | Referencia visual de la pantalla ① |
| 6 | `screens-detail.jsx` | Referencia visual de las pantallas ② y ③ |

---

## Qué NO esperar de Claude Code

- **No puede abrir tu `.pbix`** ni manipularlo directamente — Power BI usa un formato binario propietario.
- **No puede ver previews de Power BI** — sólo te guía con texto.
- **No puede crear visuales custom de Power BI** (los `.pbiviz`).

Lo que **sí** puede hacer:
- Generar y refinar el `theme.json` y el `.dax`.
- Darte el guion exacto de propiedades de cada visual.
- Generarte una **plantilla DAX** para tu modelo real.
- Resolver problemas concretos cuando te quedes atascado.
- Convertir el diseño a otros frameworks (React Native, SwiftUI, Flutter) si decides salirte de Power BI.
