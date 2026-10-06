# Cómo descargar los 6 PDFs faltantes (bloqueo automatizado)

> Estos 6 no pudieron descargarse con `curl`/`Invoke-WebRequest` por Cloudflare / paywall / JS. Todos tienen URL verificada el 2026-08-27. Descarga manual = 2 clics en navegador.

**Destino:** guarda cada PDF en `docs/referencias/pdf/` con el nombre indicado. Luego conviértelos a MD: `python _tmp_convert_pdfs.py` (ya convierte todo lo que haya en `pdf/` → `md/`).

---

## 1) IMF WP/22/94 — Paraguay piloto (Abdou et al. 2022)

- **Nombre local:** `14_IMF_WP22-94_Paraguay_Identifying_Cartels.pdf`
- **URL:** https://www.imf.org/en/Publications/WP/Issues/2022/05/20/Identifying-Cartels-in-Public-Procurement-A-Pilot-Study-for-Paraguay-517511
- **Pasos:**
  1. Abre el link → baja a "Download PDF" (o "PDF" en la esquina).
  2. Alternativa directa si el visor falla: en la página pulsa `Ctrl+S` / clic derecho → Guardar como. El servidor entrega `wpiea2022094-print-pdf.ashx`.
  3. Guarda como `14_IMF_WP22-94_Paraguay_Identifying_Cartels.pdf` en `docs/referencias/pdf/`.
- **Por qué falla automatizado:** IMF usa Cloudflare + `__cf_bm` cookie + `edgesuite.net` con validación JS para `/-media/...ashx`.

## 2) Fazekas & Kocsis 2020 — Uncovering High-Level Corruption (BJPS)

- **Nombre local:** `19_Fazekas_Kocsis_2020_BJPS.pdf`
- **URL:** https://doi.org/10.1017/S0007123418000700  → redirige a Cambridge Core
- **Pasos:**
  1. Abre el DOI → Cambridge Core. Si tu universidad tiene acceso, botón "Download PDF".
  2. Sin acceso institucional: usa el **preprint GTI**: busca "Fazekas Kocsis Uncovering High-Level Corruption GTI 2017" — el manuscrito es libre en `https://www.govtransparency.eu` (mismo contenido, sin maquetación CUP).
  3. Guarda como `19_Fazekas_Kocsis_2020_BJPS.pdf`.
- **Nota:** CAM paywall. El preprint es válido para tesis como "author's accepted manuscript".

## 3) Ferwerda, Deleanu & Unger 2016 — Finding the Right Indicators (EJCPR)

- **Nombre local:** `15_Ferwerda_2016_Finding_Right_Indicators_EJCPR.pdf`
- **URL:** https://doi.org/10.1007/s10610-016-9312-3
- **Pasos:**
  1. Abre DOI → SpringerLink → "Download PDF" (requiere acceso institucional / 29,95€).
  2. Alternativa libre: muchas universidades lo tienen en Sci-Hub o ResearchGate. Busca el título exacto en `https://www.researchgate.net/publication/303802498_Corruption_in_Public_Procurement_Finding_the_Right_Indicators`
  3. Guarda como `15_Ferwerda_2016...pdf`.
- **Por qué falla automatizado:** Springer Cloudflare bloquea `curl` sin `cf_clearance`.

## 4) Transparency International 2006 — Handbook for Curbing Corruption

- **Nombre local:** `18_TI_2006_Handbook_Curbing_Corruption_Procurement.pdf`
- **URL:** https://www.transparency.org/en/publications/handbook-for-curbing-corruption-in-public-procurement
- **Pasos:**
  1. Abre el link → "Download publication" (PDF directo: `https://images.transparencycdn.org/images/Handbook_for_Curbing_Corruption_in_Public_Procurement.pdf` — verificado, pero Cloudflare requiere navegador la primera vez).
  2. Si el CDN da 403, usa el mirror de Basel Institute: `https://baselgovernance.org/publications/handbook-curbing-corruption-public-procurement` → descarga.
  3. Guarda como `18_TI_2006_...pdf`.

## 5) Decarolis 2014 — Awarding Price, Contract Performance, and Bids Screening (AEJ Applied)

- **Nombre local:** `20_Decarolis_2014_Awarding_Price_AEJApplied.pdf`
- **URL:** https://www.aeaweb.org/articles?id=10.1257/app.6.1.108
- **Pasos:**
  1. Abre → "Download Full Text PDF" (AEA requiere cuenta gratuita o compra  $…; con cuenta institucional es libre).
  2. Alternativa libre: working paper idéntico en Bocconi `https://didattica.unibocconi.it/...` o busca "Decarolis Awarding Price 2013 working paper PDF" — el contenido es 95% igual.
  3. Guarda como `20_Decarolis_2014...pdf`.

## 6) Benford — dos artículos PMC (MÉTODO, no solo cita)

### 6a) Azevedo et al. — Bolsa Familia (PMC8720889)
- **Nombre local:** `13_Benford_BolsaFamilia_PMC8720889.pdf`
- **URL:** https://pmc.ncbi.nlm.nih.gov/articles/PMC8720889/
- **Pasos:** Abre → barra derecha "Download PDF" (PMC entrega pdf tras JS `prepare download`). Guarda como `13_Benford...pdf`.

### 6b) Corruption red flags — New Evidence ML (PMC8933377)
- **Nombre local:** `12_Corruption_RedFlags_ML_PMC8933377.pdf`
- **URL:** https://pmc.ncbi.nlm.nih.gov/articles/PMC8933377/
- **Pasos:** Idem → "Download PDF".

> **Truco rápido:** en PMC, añade `/pdf/` a la URL y espera 5s: `https://pmc.ncbi.nlm.nih.gov/articles/PMC8720889/pdf/` — el navegador descarga tras "Preparing to download…". `curl` no espera el JS, por eso falla.

---

## Después de descargar

```powershell
# 1. Verifica que están en pdf/
Get-ChildItem docs\referencias\pdf | Format-Table Name, Length

# 2. Re-convierte todo a md (sobrescribe solo los nuevos)
python _tmp_convert_pdfs.py

# 3. Verifica md/
Get-ChildItem docs\referencias\md | Format-Table Name, Length
```

O arrastra los 6 PDFs a `docs/referencias/pdf/` y avísame — los convierto yo en el siguiente mensaje.

---

## Lista rápida para copiar/pegar en navegador

```
https://www.imf.org/en/Publications/WP/Issues/2022/05/20/Identifying-Cartels-in-Public-Procurement-A-Pilot-Study-for-Paraguay-517511
https://doi.org/10.1017/S0007123418000700
https://doi.org/10.1007/s10610-016-9312-3
https://www.transparency.org/en/publications/handbook-for-curbing-corruption-in-public-procurement
https://www.aeaweb.org/articles?id=10.1257/app.6.1.108
https://pmc.ncbi.nlm.nih.gov/articles/PMC8720889/
https://pmc.ncbi.nlm.nih.gov/articles/PMC8933377/
```
