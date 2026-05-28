# DAX Measures — Financial Signals Lakehouse

Create these in the model view. Suggested home table is `cross_signal_summary`.

---

## Regime & Status

```dax
// Current risk regime (latest date)
Current Regime =
VAR LatestDate = MAX(cross_signal_summary[as_of_date])
RETURN
    CALCULATE(
        FIRSTNONBLANK(cross_signal_summary[risk_regime], 1),
        cross_signal_summary[as_of_date] = LatestDate
    )
```

```dax
// Regime colour hex — use in conditional formatting
Regime Colour =
SWITCH(
    [Current Regime],
    "calm",     "#4CAF50",
    "elevated", "#FFB703",
    "stress",   "#FF4C4C",
    "#90E0EF"
)
```

```dax
// Total stressed instruments today
Stressed Instruments =
VAR LatestDate = MAX(cross_signal_summary[as_of_date])
RETURN
    CALCULATE(
        MAX(cross_signal_summary[market_stress_symbols]),
        cross_signal_summary[as_of_date] = LatestDate
    )
```

```dax
// Market breadth % (up / total)
Market Breadth % =
VAR LatestDate = MAX(cross_signal_summary[as_of_date])
VAR Up =
    CALCULATE(
        MAX(cross_signal_summary[market_up_symbols]),
        cross_signal_summary[as_of_date] = LatestDate
    )
VAR Total =
    CALCULATE(
        MAX(cross_signal_summary[market_symbols_count]),
        cross_signal_summary[as_of_date] = LatestDate
    )
RETURN
    DIVIDE(Up, Total)
```

---

## Market Snapshot

```dax
// Count of stressed instruments (from detail table, respects slicers)
Stress Count =
CALCULATE(
    COUNTROWS(daily_market_snapshot),
    daily_market_snapshot[stress_flag] = TRUE()
)
```

```dax
// Day change colour for conditional formatting
Day Change Colour =
IF(
    SELECTEDVALUE(daily_market_snapshot[day_change_pct]) >= 0,
    "#4CAF50",
    "#FF4C4C"
)
```

```dax
// Return 30d colour
Return 30d Colour =
IF(
    SELECTEDVALUE(daily_market_snapshot[return_30d_pct]) >= 0,
    "#4CAF50",
    "#FF4C4C"
)
```

```dax
// Drawdown severity label
Drawdown Label =
VAR dd = SELECTEDVALUE(daily_market_snapshot[drawdown_from_90d_high_pct])
RETURN
    SWITCH(
        TRUE(),
        dd <= -20, "Severe",
        dd <= -10, "Moderate",
        dd <= -5,  "Mild",
        "Near High"
    )
```

---

## FX

```dax
// FX stressed pairs today
FX Stressed Pairs =
VAR LatestDate = MAX(cross_signal_summary[as_of_date])
RETURN
    CALCULATE(
        MAX(cross_signal_summary[fx_stress_pairs]),
        cross_signal_summary[as_of_date] = LatestDate
    )
```

```dax
// FX trend signal colour
FX Trend Colour =
SWITCH(
    SELECTEDVALUE(fx_trend_signals[trend_signal]),
    "strengthening", "#4CAF50",
    "weakening",     "#FF4C4C",
    "#FFB703"
)
```

---

## Macro

```dax
// Macro consensus label
Macro Consensus =
VAR LatestDate =
    CALCULATE(
        MAX(macro_indicator_trends[observation_date])
    )
VAR Up =
    CALCULATE(
        COUNTROWS(macro_indicator_trends),
        macro_indicator_trends[trend_direction] = "up",
        macro_indicator_trends[observation_date] = LatestDate
    )
VAR Down =
    CALCULATE(
        COUNTROWS(macro_indicator_trends),
        macro_indicator_trends[trend_direction] = "down",
        macro_indicator_trends[observation_date] = LatestDate
    )
RETURN
    SWITCH(
        TRUE(),
        Up > Down * 1.5, "Expanding",
        Down > Up * 1.5, "Contracting",
        "Mixed"
    )
```

---

## Top Movers

```dax
// Direction label for bar chart colouring
Mover Direction =
IF(
    SELECTEDVALUE(top_movers_why[return_30d_pct]) >= 0,
    "Up",
    "Down"
)
```

```dax
// Absolute return for bar length (direction handled by colour)
Abs Return 30d =
ABS(SELECTEDVALUE(top_movers_why[return_30d_pct]))
```

---

## Model Relationships

Set these up in Model View (all many-to-one on date):

| From (many side)             | To (one side)                       |
|------------------------------|-------------------------------------|
| daily_market_snapshot[snapshot_date] | cross_signal_summary[as_of_date] |
| fx_trend_signals[rate_date]  | cross_signal_summary[as_of_date]    |
| top_movers_why[as_of_date]   | cross_signal_summary[as_of_date]    |

`macro_indicator_trends` is on a different cadence (monthly/annual) — leave unrelated or create a separate Date table for it.
