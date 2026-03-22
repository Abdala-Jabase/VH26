
# Define after app is created


import pandas as pd
from datetime import datetime
from split_types import parse_eth_transactions
from fifo_calc import calculate_fifo_ledger_and_transactions
from lifo_calc import calculate_lifo_ledger_and_transactions
from hifo_calc import calculate_hifo_ledger_and_transactions
from flask import Flask, render_template_string, request

app = Flask(__name__)

@app.template_filter("wallet_breaks")
def wallet_breaks(value):
    if not isinstance(value, str):
        return value
    return value.replace(" | ", "<br>")


@app.template_filter("currency")
def currency(value):
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return "$0.00"
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.2f}"


def _parse_float_arg(name, default):
    raw = request.args.get(name, str(default)).strip()
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _parse_date_arg(name):
    raw = request.args.get(name, "").strip()
    if not raw:
        return None
    return pd.to_datetime(raw, errors="coerce")


def _apply_timeframe(df, timeframe, custom_start, custom_end):
    if df.empty:
        return df, None, None

    start_dt = None
    end_dt = None
    now = pd.Timestamp(datetime.now()).normalize()

    if timeframe == "ytd":
        start_dt = pd.Timestamp(year=now.year, month=1, day=1)
    elif timeframe == "last_30d":
        start_dt = now - pd.Timedelta(days=30)
    elif timeframe == "last_90d":
        start_dt = now - pd.Timedelta(days=90)
    elif timeframe == "custom":
        if custom_start is not None and not pd.isna(custom_start):
            start_dt = custom_start.normalize()
        if custom_end is not None and not pd.isna(custom_end):
            end_dt = custom_end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    filtered = df.copy()
    if start_dt is not None:
        filtered = filtered[filtered["date"] >= start_dt]
    if end_dt is not None:
        filtered = filtered[filtered["date"] <= end_dt]
    return filtered, start_dt, end_dt


def _build_tx_df(txs):
    df = pd.DataFrame(txs).copy()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["gain_loss", "short_term_gain_loss", "long_term_gain_loss", "usd_value"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "taxable_event" not in df.columns:
        df["taxable_event"] = False
    return df.sort_values("date").reset_index(drop=True)


def _with_tax_columns(df, ordinary_rate, long_term_rate):
    if df.empty:
        return df
    df = df.copy()
    df["tax_due"] = (
        df["short_term_gain_loss"].clip(lower=0) * ordinary_rate
        + df["long_term_gain_loss"].clip(lower=0) * long_term_rate
    )
    return df


def _tax_summary(df, other_income, ordinary_rate, long_term_rate):
    if df.empty:
        other_income_tax_due = max(other_income, 0.0) * ordinary_rate
        return {
            "capital_gain_total": 0.0,
            "short_term_total": 0.0,
            "long_term_total": 0.0,
            "net_short_term_taxable": 0.0,
            "net_long_term_taxable": 0.0,
            "capital_loss_offset_used": 0.0,
            "capital_gains_tax_due": 0.0,
            "other_income_tax_due": other_income_tax_due,
            "total_tax_due": other_income_tax_due,
        }
    short_term_total = float(df["short_term_gain_loss"].sum())
    long_term_total = float(df["long_term_gain_loss"].sum())
    capital_gain_total = float(df["gain_loss"].sum())

    # Net short-term and long-term buckets against each other first so losses
    # reduce taxable gains for the year.
    net_short = short_term_total
    net_long = long_term_total
    if net_short > 0 and net_long < 0:
        offset = min(net_short, abs(net_long))
        net_short -= offset
        net_long += offset
    elif net_long > 0 and net_short < 0:
        offset = min(net_long, abs(net_short))
        net_long -= offset
        net_short += offset

    capital_gains_tax_due = max(net_short, 0.0) * ordinary_rate + max(net_long, 0.0) * long_term_rate

    # If capital remains net negative, allow up to $3,000 ordinary-income offset.
    net_capital_total = net_short + net_long
    capital_loss_offset_used = min(abs(net_capital_total), 3000.0) if net_capital_total < 0 else 0.0
    taxable_other_income = max(other_income - capital_loss_offset_used, 0.0)
    other_income_tax_due = taxable_other_income * ordinary_rate

    return {
        "capital_gain_total": capital_gain_total,
        "short_term_total": short_term_total,
        "long_term_total": long_term_total,
        "net_short_term_taxable": net_short,
        "net_long_term_taxable": net_long,
        "capital_loss_offset_used": capital_loss_offset_used,
        "capital_gains_tax_due": capital_gains_tax_due,
        "other_income_tax_due": other_income_tax_due,
        "total_tax_due": capital_gains_tax_due + other_income_tax_due,
    }


TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crypto Tax Ledger</title>
    <style>
        :root {
            --bg: #f4f7fb;
            --surface: #ffffff;
            --text: #172133;
            --muted: #5c667a;
            --border: #d8dfeb;
            --accent: #0f6bcf;
            --gain: #0f8f4c;
            --loss: #c53b32;
        }
        body {
            margin: 0;
            background: linear-gradient(180deg, #f4f7fb 0%, #edf2f9 100%);
            color: var(--text);
            font-family: "Avenir Next", "Segoe UI", sans-serif;
        }
        .container {
            max-width: 1350px;
            margin: 22px auto;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 20px;
        }
        h1 {
            margin: 0 0 16px 0;
            font-size: 1.8rem;
        }
        .controls {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
            gap: 10px;
            margin-bottom: 16px;
        }
        .controls label {
            font-size: 0.82rem;
            color: var(--muted);
            display: block;
            margin-bottom: 4px;
        }
        .controls input, .controls select {
            width: 100%;
            box-sizing: border-box;
            padding: 8px 10px;
            border-radius: 8px;
            border: 1px solid var(--border);
            font-size: 0.9rem;
        }
        .controls button {
            margin-top: 22px;
            padding: 10px 12px;
            border-radius: 8px;
            border: none;
            background: var(--accent);
            color: #fff;
            font-weight: 600;
            cursor: pointer;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 12px;
            margin: 10px 0 18px 0;
        }
        .summary-card {
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 10px 12px;
            background: #fbfcff;
        }
        .summary-card h3 {
            margin: 0 0 8px 0;
            font-size: 1rem;
        }
        .summary-card p {
            margin: 3px 0;
            font-size: 0.88rem;
        }
        .gain { color: var(--gain); font-weight: 600; }
        .loss { color: var(--loss); font-weight: 600; }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.84rem;
        }
        th, td {
            border-bottom: 1px solid var(--border);
            padding: 8px;
            text-align: left;
            vertical-align: top;
        }
        th {
            background: #f0f4fb;
            color: #2f3c52;
            position: sticky;
            top: 0;
        }
        .table-wrap {
            overflow-x: auto;
            border: 1px solid var(--border);
            border-radius: 10px;
        }
        .total-row {
            font-weight: 700;
            background: #f8fbff;
        }
        .meta-note {
            color: var(--muted);
            font-size: 0.85rem;
            margin: 4px 0 10px 0;
        }
        .nowrap {
            white-space: nowrap;
        }
        .input-percent {
            display: flex;
            align-items: center;
        }
        .input-percent input[type="number"] {
            flex: 1 1 auto;
            margin-right: 4px;
        }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Crypto Tax Ledger</h1>
        <form method="get" class="controls">
            <div>
                <label>Timeframe</label>
                <select name="timeframe">
                    <option value="all" {{ "selected" if timeframe == "all" else "" }}>All Time</option>
                    <option value="ytd" {{ "selected" if timeframe == "ytd" else "" }}>Year To Date</option>
                    <option value="last_30d" {{ "selected" if timeframe == "last_30d" else "" }}>Last 30 Days</option>
                    <option value="last_90d" {{ "selected" if timeframe == "last_90d" else "" }}>Last 90 Days</option>
                    <option value="custom" {{ "selected" if timeframe == "custom" else "" }}>Custom</option>
                </select>
            </div>
            {% if timeframe != 'all' %}
            <div>
                <label>Start Date (custom)</label>
                <input type="date" name="start_date" value="{{ start_date_input }}">
            </div>
            <div>
                <label>End Date (custom)</label>
                <input type="date" name="end_date" value="{{ end_date_input }}">
            </div>
            {% endif %}
            <div>
                <label class="nowrap">Ordinary Income Tax</label>
                <span class="input-percent">
                    <input type="number" step="0.01" min="0" name="ordinary_income_rate" value="{{ ordinary_income_rate_pct }}">
                    <span>%</span>
                </span>
            </div>
            <div>
                <label class="nowrap">Long-Term Capital Gains Tax</label>
                <span class="input-percent">
                    <input type="number" step="0.01" min="0" name="long_term_rate" value="{{ long_term_rate_pct }}">
                    <span>%</span>
                </span>
            </div>
            <div>
                <label>Other Income (USD)</label>
                <input type="number" step="0.01" name="other_income" value="{{ other_income }}">
            </div>
            <div>
                <button type="submit">Apply</button>
            </div>
        </form>

        <p class="meta-note">
            Showing {{ rows|length }} transactions in selected timeframe.
            Short-term gains taxed at {{ ordinary_income_rate_pct }}%.
            Long-term gains taxed at {{ long_term_rate_pct }}%.
        </p>

        <div class="summary-grid">
            <div class="summary-card">
                <h3>FIFO Tax Summary</h3>
                <p>Capital Gain Total: <span class="{{ 'gain' if fifo_summary.capital_gain_total > 0 else 'loss' if fifo_summary.capital_gain_total < 0 else '' }}">{{ fifo_summary.capital_gain_total|currency }}</span></p>
                <p>Short-Term Gain/Loss: <span class="{{ 'gain' if fifo_summary.short_term_total > 0 else 'loss' if fifo_summary.short_term_total < 0 else '' }}">{{ fifo_summary.short_term_total|currency }}</span></p>
                <p>Long-Term Gain/Loss: <span class="{{ 'gain' if fifo_summary.long_term_total > 0 else 'loss' if fifo_summary.long_term_total < 0 else '' }}">{{ fifo_summary.long_term_total|currency }}</span></p>
                <p>Net Taxable Short-Term: <span class="{{ 'gain' if fifo_summary.net_short_term_taxable > 0 else 'loss' if fifo_summary.net_short_term_taxable < 0 else '' }}">{{ fifo_summary.net_short_term_taxable|currency }}</span></p>
                <p>Net Taxable Long-Term: <span class="{{ 'gain' if fifo_summary.net_long_term_taxable > 0 else 'loss' if fifo_summary.net_long_term_taxable < 0 else '' }}">{{ fifo_summary.net_long_term_taxable|currency }}</span></p>
                <p>Capital Loss Offset Used: {{ fifo_summary.capital_loss_offset_used|currency }}</p>
                <p>Capital Gains Tax: {{ fifo_summary.capital_gains_tax_due|currency }}</p>
                <p>Other Income Tax: {{ fifo_summary.other_income_tax_due|currency }}</p>
                <p><strong>Total Estimated Tax Due: {{ fifo_summary.total_tax_due|currency }}</strong></p>
            </div>
            <div class="summary-card">
                <h3>LIFO Tax Summary</h3>
                <p>Capital Gain Total: <span class="{{ 'gain' if lifo_summary.capital_gain_total > 0 else 'loss' if lifo_summary.capital_gain_total < 0 else '' }}">{{ lifo_summary.capital_gain_total|currency }}</span></p>
                <p>Short-Term Gain/Loss: <span class="{{ 'gain' if lifo_summary.short_term_total > 0 else 'loss' if lifo_summary.short_term_total < 0 else '' }}">{{ lifo_summary.short_term_total|currency }}</span></p>
                <p>Long-Term Gain/Loss: <span class="{{ 'gain' if lifo_summary.long_term_total > 0 else 'loss' if lifo_summary.long_term_total < 0 else '' }}">{{ lifo_summary.long_term_total|currency }}</span></p>
                <p>Net Taxable Short-Term: <span class="{{ 'gain' if lifo_summary.net_short_term_taxable > 0 else 'loss' if lifo_summary.net_short_term_taxable < 0 else '' }}">{{ lifo_summary.net_short_term_taxable|currency }}</span></p>
                <p>Net Taxable Long-Term: <span class="{{ 'gain' if lifo_summary.net_long_term_taxable > 0 else 'loss' if lifo_summary.net_long_term_taxable < 0 else '' }}">{{ lifo_summary.net_long_term_taxable|currency }}</span></p>
                <p>Capital Loss Offset Used: {{ lifo_summary.capital_loss_offset_used|currency }}</p>
                <p>Capital Gains Tax: {{ lifo_summary.capital_gains_tax_due|currency }}</p>
                <p>Other Income Tax: {{ lifo_summary.other_income_tax_due|currency }}</p>
                <p><strong>Total Estimated Tax Due: {{ lifo_summary.total_tax_due|currency }}</strong></p>
            </div>
            <div class="summary-card">
                <h3>HIFO Tax Summary</h3>
                <p>Capital Gain Total: <span class="{{ 'gain' if hifo_summary.capital_gain_total > 0 else 'loss' if hifo_summary.capital_gain_total < 0 else '' }}">{{ hifo_summary.capital_gain_total|currency }}</span></p>
                <p>Short-Term Gain/Loss: <span class="{{ 'gain' if hifo_summary.short_term_total > 0 else 'loss' if hifo_summary.short_term_total < 0 else '' }}">{{ hifo_summary.short_term_total|currency }}</span></p>
                <p>Long-Term Gain/Loss: <span class="{{ 'gain' if hifo_summary.long_term_total > 0 else 'loss' if hifo_summary.long_term_total < 0 else '' }}">{{ hifo_summary.long_term_total|currency }}</span></p>
                <p>Net Taxable Short-Term: <span class="{{ 'gain' if hifo_summary.net_short_term_taxable > 0 else 'loss' if hifo_summary.net_short_term_taxable < 0 else '' }}">{{ hifo_summary.net_short_term_taxable|currency }}</span></p>
                <p>Net Taxable Long-Term: <span class="{{ 'gain' if hifo_summary.net_long_term_taxable > 0 else 'loss' if hifo_summary.net_long_term_taxable < 0 else '' }}">{{ hifo_summary.net_long_term_taxable|currency }}</span></p>
                <p>Capital Loss Offset Used: {{ hifo_summary.capital_loss_offset_used|currency }}</p>
                <p>Capital Gains Tax: {{ hifo_summary.capital_gains_tax_due|currency }}</p>
                <p>Other Income Tax: {{ hifo_summary.other_income_tax_due|currency }}</p>
                <p><strong>Total Estimated Tax Due: {{ hifo_summary.total_tax_due|currency }}</strong></p>
            </div>
        </div>

        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Type</th>
                        <th>Token</th>
                        <th>Amount</th>
                        <th>USD Value</th>
                        <th>Wallet Amount After Tx</th>
                        <th>Wallet Value After Tx</th>
                        <th>Gain/Loss (FIFO)</th>
                        <th>Tax Due (FIFO)</th>
                        <th>Gain/Loss (LIFO)</th>
                        <th>Tax Due (LIFO)</th>
                        <th>Gain/Loss (HIFO)</th>
                        <th>Tax Due (HIFO)</th>
                    </tr>
                </thead>
                <tbody>
                {% for fifo, lifo, hifo in rows %}
                    <tr>
                        <td>{{ fifo['date'] }}</td>
                        <td>{{ fifo['type'] }}</td>
                        <td>{{ fifo['token'] }}</td>
                        <td>{{ fifo['amount'] }}</td>
                        <td>{{ fifo['usd_value']|currency }}</td>
                        <td>{{ fifo['wallet_amount_after_display']|wallet_breaks|safe }}</td>
                        <td>{{ fifo['wallet_value_after_display']|wallet_breaks|safe }}</td>
                        <td class="{{ 'gain' if fifo['gain_loss'] > 0 else 'loss' if fifo['gain_loss'] < 0 else '' }}">{{ fifo['gain_loss']|currency }}</td>
                        <td>{{ fifo['tax_due']|currency }}</td>
                        <td class="{{ 'gain' if lifo['gain_loss'] > 0 else 'loss' if lifo['gain_loss'] < 0 else '' }}">{{ lifo['gain_loss']|currency }}</td>
                        <td>{{ lifo['tax_due']|currency }}</td>
                        <td class="{{ 'gain' if hifo['gain_loss'] > 0 else 'loss' if hifo['gain_loss'] < 0 else '' }}">{{ hifo['gain_loss']|currency }}</td>
                        <td>{{ hifo['tax_due']|currency }}</td>
                    </tr>
                {% endfor %}
                    <tr class="total-row">
                        <td colspan="7">Filtered Total Gain/Loss</td>
                        <td class="{{ 'gain' if fifo_total_gain_loss > 0 else 'loss' if fifo_total_gain_loss < 0 else '' }}">{{ fifo_total_gain_loss|currency }}</td>
                        <td>{{ fifo_total_tax_due|currency }}</td>
                        <td class="{{ 'gain' if lifo_total_gain_loss > 0 else 'loss' if lifo_total_gain_loss < 0 else '' }}">{{ lifo_total_gain_loss|currency }}</td>
                        <td>{{ lifo_total_tax_due|currency }}</td>
                        <td class="{{ 'gain' if hifo_total_gain_loss > 0 else 'loss' if hifo_total_gain_loss < 0 else '' }}">{{ hifo_total_gain_loss|currency }}</td>
                        <td>{{ hifo_total_tax_due|currency }}</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""

USER_WALLET = "0x9bbe5840e8915b652a5fb44a8237b08d44cb12c5"
swaps_df, deposits_df, sends_df = parse_eth_transactions("real_data.csv", USER_WALLET)

swaps_df = swaps_df.copy()
if not swaps_df.empty:
    swaps_df["type"] = "swap"
deposits_df = deposits_df.copy()
if not deposits_df.empty:
    deposits_df["type"] = "deposit"
sends_df = sends_df.copy()
if not sends_df.empty:
    sends_df["type"] = "send"

if not swaps_df.empty and "date" in swaps_df.columns:
    swaps_df["date"] = pd.to_datetime(swaps_df["date"])
if not deposits_df.empty and "date" in deposits_df.columns:
    deposits_df["date"] = pd.to_datetime(deposits_df["date"])
if not sends_df.empty and "date" in sends_df.columns:
    sends_df["date"] = pd.to_datetime(sends_df["date"])


@app.route("/")
def index():
    timeframe = request.args.get("timeframe", "all")
    start_date_input = request.args.get("start_date", "")
    end_date_input = request.args.get("end_date", "")
    custom_start = _parse_date_arg("start_date")
    custom_end = _parse_date_arg("end_date")

    ordinary_income_rate_pct = _parse_float_arg("ordinary_income_rate", 24.0)
    long_term_rate_pct = _parse_float_arg("long_term_rate", 15.0)
    other_income = _parse_float_arg("other_income", 0.0)

    ordinary_income_rate = ordinary_income_rate_pct / 100.0
    long_term_rate = long_term_rate_pct / 100.0

    _, fifo_txs = calculate_fifo_ledger_and_transactions(swaps_df, deposits_df, sends_df)
    _, lifo_txs = calculate_lifo_ledger_and_transactions(swaps_df, deposits_df, sends_df)
    _, hifo_txs = calculate_hifo_ledger_and_transactions(swaps_df, deposits_df, sends_df)

    fifo_df = _build_tx_df(fifo_txs)
    lifo_df = _build_tx_df(lifo_txs)
    hifo_df = _build_tx_df(hifo_txs)

    fifo_df, _, _ = _apply_timeframe(fifo_df, timeframe, custom_start, custom_end)
    lifo_df, _, _ = _apply_timeframe(lifo_df, timeframe, custom_start, custom_end)
    hifo_df, _, _ = _apply_timeframe(hifo_df, timeframe, custom_start, custom_end)

    fifo_df = _with_tax_columns(fifo_df, ordinary_income_rate, long_term_rate)
    lifo_df = _with_tax_columns(lifo_df, ordinary_income_rate, long_term_rate)
    hifo_df = _with_tax_columns(hifo_df, ordinary_income_rate, long_term_rate)

    fifo_summary = _tax_summary(fifo_df, other_income, ordinary_income_rate, long_term_rate)
    lifo_summary = _tax_summary(lifo_df, other_income, ordinary_income_rate, long_term_rate)
    hifo_summary = _tax_summary(hifo_df, other_income, ordinary_income_rate, long_term_rate)

    fifo_records = fifo_df.to_dict(orient="records")
    lifo_records = lifo_df.to_dict(orient="records")
    hifo_records = hifo_df.to_dict(orient="records")
    rows = list(zip(fifo_records, lifo_records, hifo_records))

    fifo_total_gain_loss = float(fifo_df["gain_loss"].sum()) if not fifo_df.empty else 0.0
    lifo_total_gain_loss = float(lifo_df["gain_loss"].sum()) if not lifo_df.empty else 0.0
    hifo_total_gain_loss = float(hifo_df["gain_loss"].sum()) if not hifo_df.empty else 0.0
    fifo_total_tax_due = fifo_summary["total_tax_due"]
    lifo_total_tax_due = lifo_summary["total_tax_due"]
    hifo_total_tax_due = hifo_summary["total_tax_due"]

    return render_template_string(
        TEMPLATE,
        rows=rows,
        timeframe=timeframe,
        start_date_input=start_date_input,
        end_date_input=end_date_input,
        ordinary_income_rate_pct=ordinary_income_rate_pct,
        long_term_rate_pct=long_term_rate_pct,
        other_income=other_income,
        fifo_summary=fifo_summary,
        lifo_summary=lifo_summary,
        hifo_summary=hifo_summary,
        fifo_total_gain_loss=fifo_total_gain_loss,
        lifo_total_gain_loss=lifo_total_gain_loss,
        hifo_total_gain_loss=hifo_total_gain_loss,
        fifo_total_tax_due=fifo_total_tax_due,
        lifo_total_tax_due=lifo_total_tax_due,
        hifo_total_tax_due=hifo_total_tax_due,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5001)
