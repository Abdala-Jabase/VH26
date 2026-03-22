#!/Users/abdalajabase/Documents/repos/VH26/.venv/bin/python3
from split_types import parse_eth_transactions, cumulative_token_balances_from_raw_csv
from hifo import fifo_gains_from_deposits_and_swaps, lots_to_dataframe


if __name__ == "__main__":

    USER_WALLET = "0x9bbe5840e8915b652a5fb44a8237b08d44cb12c5"
    OPENING_BALANCES = {
        "ETH": 2.3075734928158906,
    }
    swaps_df, deposits_df, sends_df = parse_eth_transactions("ethplorer.csv", USER_WALLET)

    realized_df, remaining_lots, steps_df = fifo_gains_from_deposits_and_swaps(
        deposits_df,
        swaps_df,
        include_steps=True,
    )
    remaining_lots_df = lots_to_dataframe(remaining_lots)

    realized_df.to_csv("realized_fifo_gains.csv", index=False)
    remaining_lots_df.to_csv("remaining_fifo_lots.csv", index=False)
    steps_df.to_csv("fifo_steps.csv", index=False)

    print(f"Processed {len(realized_df)} swaps.")
    print(f"Total realized gain/loss USD: {realized_df['realized_gain_loss_usd'].sum():.2f}")
    print(f"Generated {len(steps_df)} step rows in fifo_steps.csv")

    preview_cols = [
        "date",
        "txHash",
        "step_type",
        "coin",
        "amount",
        "usd_value",
        "cost_basis_usd",
        "realized_gain_loss_usd",
    ]
    available_cols = [c for c in preview_cols if c in steps_df.columns]
    print("\nStep preview:")
    print(steps_df[available_cols].head(25).to_string(index=False))

    cumulative_df = cumulative_token_balances_from_raw_csv(
        "ethplorer.csv",
        USER_WALLET,
        write_csv=True,
        opening_balances=OPENING_BALANCES,
    )
    print(f"\nGenerated {len(cumulative_df)} rows in raw_cumulative_balances.csv")

    eth_cumulative = (
        cumulative_df[cumulative_df["tokenSymbol"] == "ETH"]
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
        .copy()
    )
    eth_preview_cols = [
        "date",
        "txHash",
        "direction",
        "delta_amount",
        "running_amount",
        "running_amount_adjusted",
        "delta_usd_value",
        "running_usd_value",
    ]
    print("\nRaw ETH cumulative preview (first 15 by date):")
    print(eth_cumulative[eth_preview_cols].to_string(index=False))

    print("\nRaw coverage diagnostics:")
    print(f"Date range in CSV: {cumulative_df['date'].min()} -> {cumulative_df['date'].max()}")
    token_min = (
        cumulative_df.groupby("tokenSymbol", as_index=False)[["running_amount", "running_amount_adjusted"]]
        .min()
        .rename(
            columns={
                "running_amount": "min_running_amount_raw",
                "running_amount_adjusted": "min_running_amount_adjusted",
            }
        )
        .sort_values("min_running_amount_adjusted")
    )
    negatives = token_min[token_min["min_running_amount_adjusted"] < 0]
    if negatives.empty:
        print("No negative running balances found after opening-balance adjustment.")
    else:
        print("Tokens still negative after opening-balance adjustment:")
        print(negatives.to_string(index=False))
