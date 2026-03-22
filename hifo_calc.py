# Placeholder for HIFO (Highest-In-First-Out) calculation logic
# Implement your HIFO algorithm here, following the same interface as FIFO
import pandas as pd
from collections import defaultdict, deque


def _token_totals(lots, token):
    amount = sum(lot.get('amount', 0.0) for lot in lots[token])
    usd_value = sum(lot.get('usd_value', 0.0) for lot in lots[token])
    return amount, usd_value


def _holding_days(disposal_date, acquisition_date):
    try:
        return int((disposal_date - acquisition_date).days)
    except Exception:
        return 0


def calculate_hifo_ledger_and_transactions(swaps_df, deposits_df, sends_df):
    # HIFO implementation: Highest-In-First-Out
    # For each outgoing transaction, consume the highest cost basis lots first
    all_txs = pd.concat([swaps_df, deposits_df, sends_df], ignore_index=True, sort=False)
    if 'date' in all_txs.columns:
        all_txs = all_txs.sort_values('date')

    # Build lots by token
    lots = defaultdict(list)  # token -> list of lots (dicts with amount, usd_value, date)
    ledger = defaultdict(float)
    txs_with_gain_loss = []

    for _, tx in all_txs.iterrows():
        tx_type = tx.get('type')
        # Use correct column names for each type
        if tx_type == 'deposit':
            token = tx.get('tokenSymbol')
            amount = tx.get('value')
            usd_value = tx.get('usdValue')
        elif tx_type == 'send':
            token = tx.get('tokenSymbol')
            amount = tx.get('value')
            usd_value = tx.get('usdValue')
        elif tx_type == 'swap':
            token = None  # not used for swap
            amount = None
            usd_value = None
        else:
            token = None
            amount = None
            usd_value = None
        date = tx.get('date')
        gain_loss = 0.0
        short_term_gain_loss = 0.0
        long_term_gain_loss = 0.0

        # Handle None values for amount and usd_value
        amount = float(amount) if amount is not None else 0.0
        usd_value = float(usd_value) if usd_value is not None else 0.0

        if tx_type == 'deposit':
            if amount > 0:
                lots[token].append({'amount': amount, 'usd_value': usd_value, 'date': date, 'unit_cost': usd_value / amount if amount else 0})
                ledger[token] += amount
        elif tx_type == 'send':
            if amount > 0:
                remaining = amount
                outgoing_value = usd_value
                lots[token].sort(key=lambda l: l['unit_cost'], reverse=True)
                while remaining > 0 and lots[token]:
                    lot = lots[token][0]
                    lot_amt = lot['amount']
                    consume = min(remaining, lot_amt)
                    cost_basis = lot['unit_cost'] * consume
                    proceeds = (outgoing_value / amount) * consume if amount else 0
                    gain_part = proceeds - cost_basis
                    gain_loss += gain_part
                    held_days = _holding_days(date, lot.get('date'))
                    if held_days >= 365:
                        long_term_gain_loss += gain_part
                    else:
                        short_term_gain_loss += gain_part
                    lot['amount'] -= consume
                    remaining -= consume
                    if lot['amount'] <= 1e-10:
                        lots[token].pop(0)
                    ledger[token] -= consume
        elif tx_type == 'swap':
            sent_token = tx.get('sent_token')
            sent_amount = tx.get('sent_amount')
            received_token = tx.get('received_token')
            received_amount = tx.get('received_amount')
            usd_value_at_time = tx.get('usd_value_at_time')
            # Handle None values for swap fields
            sent_amount = float(sent_amount) if sent_amount is not None else 0.0
            received_amount = float(received_amount) if received_amount is not None else 0.0
            usd_value_at_time = float(usd_value_at_time) if usd_value_at_time is not None else 0.0
            # Remove sent_token (HIFO)
            if sent_token and sent_amount > 0:
                remaining = sent_amount
                lots[sent_token].sort(key=lambda l: l['unit_cost'], reverse=True)
                while remaining > 0 and lots[sent_token]:
                    lot = lots[sent_token][0]
                    lot_amt = lot['amount']
                    consume = min(remaining, lot_amt)
                    cost_basis = lot['unit_cost'] * consume
                    proceeds = (usd_value_at_time / sent_amount) * consume if sent_amount else 0
                    gain_part = proceeds - cost_basis
                    gain_loss += gain_part
                    held_days = _holding_days(date, lot.get('date'))
                    if held_days >= 365:
                        long_term_gain_loss += gain_part
                    else:
                        short_term_gain_loss += gain_part
                    lot['amount'] -= consume
                    remaining -= consume
                    if lot['amount'] <= 1e-10:
                        lots[sent_token].pop(0)
                    ledger[sent_token] -= consume
            # Add received_token
            if received_token and received_amount > 0:
                lots[received_token].append({'amount': received_amount, 'usd_value': usd_value_at_time, 'date': date, 'unit_cost': usd_value_at_time / received_amount if received_amount else 0})
                ledger[received_token] += received_amount
        # Record transaction with gain/loss
        # For display, use the same keys as FIFO/LIFO output
        if tx_type == 'deposit' or tx_type == 'send':
            bal_amt, bal_usd = _token_totals(lots, token)
            txs_with_gain_loss.append({
                'date': date,
                'type': tx_type,
                'token': token,
                'amount': amount,
                'usd_value': usd_value,
                'gain_loss': gain_loss,
                'short_term_gain_loss': short_term_gain_loss,
                'long_term_gain_loss': long_term_gain_loss,
                'taxable_event': tx_type in {'send'},
                'wallet_amount_after_display': f"{token}: {bal_amt:,.8f}",
                'wallet_value_after_display': f"{token}: ${bal_usd:,.2f}",
            })
        elif tx_type == 'swap':
            # For swaps, display sent_token, sent_amount, usd_value_at_time
            sent_bal_amt, sent_bal_usd = _token_totals(lots, sent_token)
            recv_bal_amt, recv_bal_usd = _token_totals(lots, received_token)
            txs_with_gain_loss.append({
                'date': date,
                'type': tx_type,
                'token': sent_token,
                'amount': sent_amount,
                'usd_value': usd_value_at_time,
                'gain_loss': gain_loss,
                'short_term_gain_loss': short_term_gain_loss,
                'long_term_gain_loss': long_term_gain_loss,
                'taxable_event': True,
                'wallet_amount_after_display': (
                    f"{sent_token}: {sent_bal_amt:,.8f} | {received_token}: {recv_bal_amt:,.8f}"
                ),
                'wallet_value_after_display': (
                    f"{sent_token}: ${sent_bal_usd:,.2f} | {received_token}: ${recv_bal_usd:,.2f}"
                ),
            })
    return ledger, txs_with_gain_loss
