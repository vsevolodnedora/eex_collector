# eex_collector

Automatically collects daily natural‑gas spot prices from
[EEX](https://www.eex.com/en/market-data/natural-gas/spot) and stores them as CSV files
in [`./data`](./data). Used for a personal energy‑market analysis / forecasting project.

## What it collects

For the **Title Transfer Facility (TTF)** hub, two daily price series:

- **EOD** — End‑of‑Day day‑ahead settlement price.
- **EGSI** — European Gas Spot Index.

Prices are daily values, expanded to an **hourly** grid (one row per hour). For redundancy
and to confirm freshness, **each file holds a rolling ~20‑day window**, so the same hour
appears (identically) in many consecutive files.

## How it works

- A scheduled **GitHub Actions** workflow ([`.github/workflows/collect.yml`](.github/workflows/collect.yml))
  runs `main.py` once a day (cron `0 12 * * *`, i.e. 12:00 UTC) and commits any new data as
  `Add data`. It also runs on push and can be triggered manually (`workflow_dispatch`).
- `main.py` fetches each series from the EEX data API in **one request per zone**, builds the
  hourly series, merges EOD + EGSI on the timestamp, and writes the CSV.
- **Endpoint:** `https://api.eex-group.com/pub/market-data/table-data`
  (the legacy `webservice-eex.gvsi.com` host was decommissioned ~May 2026).

## Output format

Files are named `data/ttf_<YYYY-MM-DD>_<freq>.csv`, where `<freq>` is the inferred sampling
frequency (`h` for a clean hourly grid). Columns:

| column | meaning |
|--------|---------|
| `date` | UTC‑naive hourly timestamp |
| `EGSI` | European Gas Spot Index price |
| `EOD`  | End‑of‑Day day‑ahead settlement price |

- **Timestamps** are UTC. The gas day starts at **06:00 Europe/Berlin** (= 04:00 UTC in
  summer / 05:00 UTC in winter); DST transitions are handled (23h / 25h days stay continuous).
- A missing/unsettled value is left empty (`NaN`), never written as `0`.

> **⚠️ Price units.** Stored prices are the raw EUR/MWh price **multiplied by 9.7694**
> (a bcm→TWh factor; dimensionally not a real unit). It is kept only so the whole dataset stays
> on one scale. **Divide by 9.7694 to recover the actual TTF price in EUR/MWh.**

## Running manually / backfilling

```bash
pipenv install
pipenv run python main.py            # default: last 20 days
LOOKBACK_DAYS=45 pipenv run python main.py   # wider window, e.g. to backfill a gap
```

Or trigger the workflow from the **Actions** tab (`Run workflow`) with a `lookback_days` input.
The endpoint serves ~45 days of rolling history, so larger look‑backs need a separate source.

## Data‑history caveats

The collector was rewritten on **2026‑02‑13**; files differ before/after:

- **EOD/EGSI columns are swapped in files written before 2026‑02‑13.** Tell them apart by
  column order: `date,EOD,EGSI` (old, **swapped**) vs `date,EGSI,EOD` (current, correct).
- Some older files encode non‑trading days as price `0` (treat as `NaN`) and have a missing
  (autumn) or duplicate (spring) hour at DST switches — all fixed in the current code.
- `data/ttf_2026-06-08_backfill_h.csv` is a one‑off 45‑day fill of the 2026‑05‑03 → 06‑08
  collection outage (caused by the old API endpoint being decommissioned).

## Credits

Inspired by [gruijter/com.gruijter.powerhour](https://github.com/gruijter/com.gruijter.powerhour)
(see `lib/providers/EEX.js`); originally translated from JavaScript to Python with the help of an LLM.
