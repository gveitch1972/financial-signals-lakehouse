# Power BI Setup — Financial Signals Lakehouse

## Report File

`FinancialSignals.pbix` — saved on the Windows VM. Also accessible via VirtualBox shared folder mapped to:
`/Users/G/projects/Databricks/financial-signals-lakehouse/powerbi`

## Connection

- **Method:** Native Databricks connector (Get Data → Databricks)
- **Auth:** Personal Access Token
- **Catalog:** `fin_signals_dev`
- **Schema:** `gold`
- **Compute:** All-Purpose single-node cluster (8GB / 4 core) — switched from Serverless Warehouse which was too expensive. Set auto-termination to 30-60 mins.

## Tables Loaded

All five Gold tables imported via native Databricks connector:

| Table | Used In |
|---|---|
| `gold.cross_signal_summary` | KPI cards, date slicer spine, regime timeline |
| `gold.daily_market_snapshot` | Scatter plot |
| `gold.fx_trend_signals` | FX signals page |
| `gold.macro_indicator_trends` | Macro context page |
| `gold.top_movers_why` | Top movers page |

## Data Model Relationships

| From (many) | To (one) | Cardinality | Cross-filter | Active |
|---|---|---|---|---|
| `daily_market_snapshot[snapshot_date]` | `cross_signal_summary[as_of_date]` | Many to One | Single | Yes |
| `fx_trend_signals[rate_date]` | `cross_signal_summary[as_of_date]` | Many to One | Single | Yes |
| `top_movers_why[as_of_date]` | `cross_signal_summary[as_of_date]` | Many to One | Single | No (ambiguous path via daily_market_snapshot) |

`cross_signal_summary` is the date spine. `macro_indicator_trends` left unrelated (different cadence).

## DAX Measures

Created in `daily_market_snapshot` table:

```dax
Abs Drawdown % = ABS(AVERAGE(daily_market_snapshot[drawdown_from_90d_high_pct]))
```

Used in scatter chart Size field (drawdown values are negative — Power BI can't size bubbles with negative values).

## Theme

File: `theme_financial_dark.json` — loaded from VirtualBox shared folder.

Applied via `View → Browse for themes`.

Valid top-level properties (Power BI is strict — visualStyles wildcards cause validation errors):
- `name`, `dataColors`, `background`, `foreground`, `tableAccent`, `textClasses` only

Colours:
- Background: `#1A1A2E` (dark navy)
- Foreground: `#E0E0E0`
- Table accent: `#00B4D8` (cyan)
- Data colours: cyan, green, red, amber, light blue, purple, orange, bright blue

## Pages Built

### Page 1 — Market Risk Dashboard

**Visuals:**
- 4x KPI Card (lightning bolt / new card visual):
  - `cross_signal_summary[risk_regime]`
  - `cross_signal_summary[market_stress_symbols]`
  - `cross_signal_summary[market_down_symbols]`
  - `cross_signal_summary[market_up_symbols]`
- Date slicer — `cross_signal_summary[as_of_date]`, dropdown, sorted descending
- Scatter chart:
  - Values: `daily_market_snapshot[symbol]`
  - X Axis: `daily_market_snapshot[return_30d_pct]` (Average)
  - Y Axis: `daily_market_snapshot[rolling_30d_volatility]` (Average)
  - Size: `Abs Drawdown %` DAX measure
  - Tooltips: `daily_market_snapshot[stress_flag]`
  - Legend: `daily_market_snapshot[symbol]` (coloured by symbol, stress_flag in tooltip)

**Pending:** Clean up card labels ("Sum of..." → proper names), scatter title

### Page 2 — FX Signals

**Visuals:**
- KPI Card: Count of `fx_trend_signals[stress_flag]` — "Stressed Pairs"
- KPI Card: `cross_signal_summary[fx_strengthening_pairs]`
- KPI Card: `cross_signal_summary[fx_weakening_pairs]`
- Date slicer — `cross_signal_summary[as_of_date]`, dropdown, sorted descending
  - Edit interactions: slicer does NOT filter the line chart (full history shown)
- Line chart: X=`rate_date`, Y=`rate` (Average), Legend=`currency_pair`
- Currency pair slicer — `fx_trend_signals[currency_pair]`

### Page 3 — Macro Context

**Visuals:**
- Bar chart: X=`indicator_name`, Y=`year_over_year_pct` (Average), Legend=`trend_direction`
- Line chart: X=`observation_date`, Y=`observation_value` (Average), Legend=`indicator_name`
- Country slicer — filtered to: CHN, DEU, GBR, JPN, USA (World Bank 3-letter codes)
- Indicator name slicer

### Page 4 — Top Movers

**Visuals:**
- Clustered bar chart: Y=`symbol`, X=`return_30d_pct` (Average), Legend=`stress_flag`
- Table: `symbol`, `return_30d_pct`, `day_change_pct`, `stress_flag`, `why_summary`
  - `why_summary` in Tooltips for hover reveal (text too long for clean table display)

### Page 5 — Regime Timeline

**Visuals:**
- Line chart: X=`as_of_date`, Y=`market_avg_return_30d_pct`
- Bar chart: X=`as_of_date`, Y=`market_stress_symbols`
- Risk regime slicer — `cross_signal_summary[risk_regime]` (calm/elevated/stress)
  - Filtering to "stress" shows COVID crash March 2020 — strong demo moment
- Table: `as_of_date`, `risk_regime`, `market_avg_return_30d_pct`, `market_stress_symbols`, `fx_stress_pairs`

## Pending Polish

- [ ] Rename card labels on all pages ("Sum of..." → clean names via "Rename for this visual")
- [ ] Replace scatter auto-title with "Risk / Return — 30 Day"
- [ ] Resize `risk_regime` card on Page 1 (currently large empty box)
- [ ] Fix Regime Timeline line chart X axis (showing "Year" hierarchy instead of `as_of_date`)
- [ ] Fix Regime Timeline bar chart (X/Y axes swapped)
- [ ] Explore 3D Scatter custom visual (Akvelon) from AppSource as upgrade to Page 1 scatter

## Demo Flow

1. **Market Risk Dashboard** — set date to recent date, show regime + scatter. GLD.US isolated top-left is the talking point.
2. **Top Movers** — show `why_summary` tooltip. "Plain-English explanation generated by the pipeline."
3. **Regime Timeline** — filter slicer to "stress" → COVID crash March 2020 appears. "-20% return, 6 stressed instruments."
4. **FX Signals** — isolate a pair with the currency slicer.
5. **Macro Context** — filter to GBR, show inflation trend.
