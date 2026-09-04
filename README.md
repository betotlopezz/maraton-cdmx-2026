Mexico City Marathon 2026 — Results Scraper & Analysis

Python scraper that extracts the official results and intermediate split times for the 22,568 runners who crossed the finish line at the 2026 Mexico City Marathon (Telcel), along with the cleaned, anonymized dataset and an analysis of the most interesting findings.

📊 Looking for the findings, not the code? Read the full analysis here: https://hljorgea.substack.com/p/mexico-city-marathon-2026-facts-and
— or check the Wiki for the quick summary.

How the scraper works: 

The data source is the event's public results platform (resultados.marcate.events). The scraper runs in two stages:

Stage 1 — runner list by category. Loops through the 22 official categories (male/female, by age group, wheelchair, blind/visually impaired) to build the full list of bib numbers.
Stage 2 — per-runner detail and splits. For each bib, it queries the full result detail: overall position, position within gender and category, official time (gun time and chip time), pace, and the 8 intermediate split times at each timing mat (5K, 10K, 15K, 21K, 25K, 30K, 35K, 40K).

Notable technical details:

Concurrent: uses ThreadPoolExecutor (8 workers by default) to query multiple runners in parallel.
Resilient to interruptions: saves progress every 100 records and can resume where it left off if the process is interrupted (built with Google Colab in mind, where the runtime can restart).
Retries: each query retries up to 3 times before giving up on that runner.
How to run it
bash
pip install requests pandas tqdm
python scraper_maraton_cdmx_2026_splits.py

The script generates two files in the directory it's run from:

bibs_maraton_cdmx_2026.csv — list of bibs by category (stage 1).
resultados_splits_maraton_cdmx_2026.csv — final result with splits (stage 2).

Note: the CSV published in this repo (..._anonimizado.csv) is the anonymized version of that output — names were replaced with identifiers like Corredor_00001.

Data dictionary (CSV)
bib	Runner: bib number (anonymized as Corredor_XXXXX).
nombre: Runner name (anonymized, same as bib).
categoria: Official category (age and gender), e.g. Master Varonil (35 a 39 años).
rama: Male / Female.
posicion_overall: Overall position in the absolute ranking.
posicion_cuenta: Total runners considered in the overall ranking.
posicionRama / posicionRama_de: Position within gender / total in that gender.
posicionCategoria / posicionCategoria_de: Position within category / total in that category.
guntime: Official time from the starting gun.
tiempoChip: Net time according to the runner's chip.
segundosGuntime: guntime converted to seconds.
pace: Average pace (min/km).
paso: Pace per kilometer, formatted mm:ss.
equipo: Team or club, when the runner registered one.
split_5K … split_40K: Cumulative time when crossing each timing mat. Empty if the runner has no reading at that checkpoint.

About the data

Results come from the event's public results platform; this project has no official affiliation with the marathon's organizers. The data is used strictly for statistical analysis, and participant names were anonymized before publishing.
