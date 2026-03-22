import pandas as pd
from collections import defaultdict, deque


def concat_and_sort_by_date(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Concatenate three DataFrames and return them sorted by a date column."""
    combined = pd.concat([df1, df2, df3], ignore_index=True)
    combined[date_col] = pd.to_datetime(combined[date_col], errors="coerce")
    return combined.sort_values(by=date_col, kind="stable").reset_index(drop=True)


def fifo_gains_from_deposits_and_swaps(
    deposits_df: pd.DataFrame,
    swaps_df: pd.DataFrame,
    include_steps: bool = False,
) -> tuple[pd.DataFrame, dict[str, deque]] | tuple[pd.DataFrame, dict[str, deque], pd.DataFrame]:
    """
    Process deposits and swaps in chronological order using FIFO lots.

    Returns:
    1) Realized gain/loss rows for each swap.
    2) Remaining inventory as per-coin FIFO queues.
    3) Optional step-by-step trace DataFrame when include_steps=True.
    """
    deposits = deposits_df.copy()
    swaps = swaps_df.copy()

    deposits["date"] = pd.to_datetime(deposits["date"], errors="coerce")
    swaps["date"] = pd.to_datetime(swaps["date"], errors="coerce")

    deposits["value"] = pd.to_numeric(deposits["value"], errors="coerce")
    deposits["usdValue"] = pd.to_numeric(deposits["usdValue"], errors="coerce")
    swaps["sent_amount"] = pd.to_numeric(swaps["sent_amount"], errors="coerce")
    swaps["received_amount"] = pd.to_numeric(swaps["received_amount"], errors="coerce")
    swaps["usd_value_at_time"] = pd.to_numeric(swaps["usd_value_at_time"], errors="coerce")

    deposits_norm = deposits[
        ["date", "txHash", "tokenSymbol", "value", "usdValue"]
    ].rename(
        columns={
            "tokenSymbol": "coin",
            "value": "amount",
            "usdValue": "usd_value",
        }
    )
    deposits_norm["tx_type"] = "deposit"

    swaps_norm = swaps[
        [
            "date",
            "txHash",
            "sent_token",
            "sent_amount",
            "received_token",
            "received_amount",
            "usd_value_at_time",
        ]
    ].copy()
    swaps_norm["tx_type"] = "swap"

    all_txs = pd.concat([deposits_norm, swaps_norm], ignore_index=True, sort=False)
    all_txs = all_txs.sort_values("date", kind="stable").reset_index(drop=True)

    lots: dict[str, deque] = defaultdict(deque)
    realized_rows = []
    step_rows = []

    for _, row in all_txs.iterrows():
        tx_type = row["tx_type"]

        if tx_type == "deposit":
            coin = row["coin"]
            amount = float(row["amount"])
            usd_value = float(row["usd_value"])
            if amount <= 0:
                continue

            lots[coin].append(
                {
                    "date": row["date"],
                    "txHash": row["txHash"],
                    "amount": amount,
                    "usd_cost": usd_value,
                    "unit_cost": usd_value / amount if amount else 0.0,
                }
            )

            if include_steps:
                step_rows.append(
                    {
                        "date": row["date"],
                        "txHash": row["txHash"],
                        "step_type": "deposit_add",
                        "coin": coin,
                        "amount": amount,
                        "usd_value": usd_value,
                        "unit_price": usd_value / amount if amount else 0.0,
                        "lot_date": row["date"],
                        "lot_txHash": row["txHash"],
                        "queue_lots_after": len(lots[coin]),
                        "queue_amount_after": sum(lot["amount"] for lot in lots[coin]),
                    }
                )
            continue

        sent_coin = row["sent_token"]
        sent_amount = float(row["sent_amount"])
        recv_coin = row["received_token"]
        recv_amount = float(row["received_amount"])
        proceeds_usd = float(row["usd_value_at_time"])

        if sent_amount <= 0:
            continue

        remaining_to_dispose = sent_amount
        total_cost_basis = 0.0

        while remaining_to_dispose > 0:
            if not lots[sent_coin]:
                raise ValueError(
                    f"Insufficient {sent_coin} lots for tx {row['txHash']} on {row['date']}. "
                    f"Needed {sent_amount}, missing {remaining_to_dispose}."
                )

            oldest_lot = lots[sent_coin][0]
            lot_amount = float(oldest_lot["amount"])
            lot_unit_cost = float(oldest_lot["unit_cost"])

            consumed = min(lot_amount, remaining_to_dispose)
            total_cost_basis += consumed * lot_unit_cost

            if include_steps:
                step_rows.append(
                    {
                        "date": row["date"],
                        "txHash": row["txHash"],
                        "step_type": "swap_dispose_chunk",
                        "coin": sent_coin,
                        "amount": consumed,
                        "usd_value": consumed * lot_unit_cost,
                        "unit_price": lot_unit_cost,
                        "lot_date": oldest_lot["date"],
                        "lot_txHash": oldest_lot["txHash"],
                        "queue_lots_after": len(lots[sent_coin]),
                        "queue_amount_after": sum(lot["amount"] for lot in lots[sent_coin]) - consumed,
                    }
                )

            oldest_lot["amount"] = lot_amount - consumed
            oldest_lot["usd_cost"] = oldest_lot["amount"] * lot_unit_cost
            remaining_to_dispose -= consumed

            if oldest_lot["amount"] <= 1e-18:
                lots[sent_coin].popleft()

        realized_rows.append(
            {
                "date": row["date"],
                "txHash": row["txHash"],
                "sent_coin": sent_coin,
                "sent_amount": sent_amount,
                "proceeds_usd": proceeds_usd,
                "cost_basis_usd": total_cost_basis,
                "realized_gain_loss_usd": proceeds_usd - total_cost_basis,
                "received_coin": recv_coin,
                "received_amount": recv_amount,
            }
        )

        if include_steps:
            step_rows.append(
                {
                    "date": row["date"],
                    "txHash": row["txHash"],
                    "step_type": "swap_summary",
                    "coin": sent_coin,
                    "amount": sent_amount,
                    "usd_value": proceeds_usd,
                    "unit_price": proceeds_usd / sent_amount if sent_amount else 0.0,
                    "lot_date": pd.NaT,
                    "lot_txHash": None,
                    "queue_lots_after": len(lots[sent_coin]),
                    "queue_amount_after": sum(lot["amount"] for lot in lots[sent_coin]),
                    "cost_basis_usd": total_cost_basis,
                    "realized_gain_loss_usd": proceeds_usd - total_cost_basis,
                    "received_coin": recv_coin,
                    "received_amount": recv_amount,
                }
            )

        if recv_amount > 0:
            lots[recv_coin].append(
                {
                    "date": row["date"],
                    "txHash": row["txHash"],
                    "amount": recv_amount,
                    "usd_cost": proceeds_usd,
                    "unit_cost": proceeds_usd / recv_amount,
                }
            )

            if include_steps:
                step_rows.append(
                    {
                        "date": row["date"],
                        "txHash": row["txHash"],
                        "step_type": "swap_receive_add",
                        "coin": recv_coin,
                        "amount": recv_amount,
                        "usd_value": proceeds_usd,
                        "unit_price": proceeds_usd / recv_amount,
                        "lot_date": row["date"],
                        "lot_txHash": row["txHash"],
                        "queue_lots_after": len(lots[recv_coin]),
                        "queue_amount_after": sum(lot["amount"] for lot in lots[recv_coin]),
                    }
                )

    realized_df = pd.DataFrame(realized_rows)
    if include_steps:
        steps_df = pd.DataFrame(step_rows).sort_values("date", kind="stable").reset_index(drop=True)
        return realized_df, lots, steps_df
    return realized_df, lots


def lots_to_dataframe(lots: dict[str, deque]) -> pd.DataFrame:
    """Flatten per-coin lot queues into a DataFrame."""
    rows = []
    for coin, queue in lots.items():
        for lot in queue:
            rows.append(
                {
                    "coin": coin,
                    "date": lot["date"],
                    "txHash": lot["txHash"],
                    "amount": lot["amount"],
                    "usd_cost": lot["usd_cost"],
                    "unit_cost": lot["unit_cost"],
                }
            )
    return pd.DataFrame(rows)
