# Documentación pública de CryptoPulse

MkDocs Material publica el producto local hasta S12 y su correspondencia con la plantilla U2. GitHub Pages aloja documentación; los servicios de datos se ejecutan en Docker local.

## Validación y vista previa

```powershell
py -m pip install -r site/requirements.txt
py -m mkdocs build --strict -f site/mkdocs.yml
py -m mkdocs serve -f site/mkdocs.yml
```

El workflow `.github/workflows/cryptopulse-docs.yml` compila y publica al cambiar `site/**` en main. La navegación se define en `site/mkdocs.yml`.

## Evidencias

`docs/assets/images` contiene diagramas técnicos SVG. `docs/assets/downloads` conserva resultados estructurados fechados. `resources/informe-integral-cryptopulse.md` es la copia pública del informe, con accesos locales retirados.

Al añadir capturas reales, guardar fecha, herramienta, condiciones y qué se puede concluir en `evidencias.md`. Distinguir una ejecución guardada de un estado actual. Los diagramas no se presentan como capturas de herramientas.

La implementación adapta la fuente de sensores a telemetría real de mercado conforme al brief. La equivalencia académica y la sustentación individual corresponden al docente.

## Evidencia visual incorporada

El 30/10/2026 se levantó el producto local y se añadieron 14 capturas reales en `docs/assets/evidencias/`, documentadas en `docs/evidencias.md`. Integración y S12 PASS. `docs/assets/downloads/ejecucion-capturas-2026-10-30.json` conserva fechas y contexto; el sitio público sigue siendo documentación estática.
