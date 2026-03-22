# Placeholder for LIFO (Last-In-First-Out) calculation logic
# Implement your LIFO algorithm here, following the same interface as FIFO
import pandas as pd
from collections import defaultdict, deque


def _token_totals(ledger, token):
    amount = sum(lot.get('amount', 0.0) for lot in ledger[token])
    usd_value = sum(lot.get('usd_value', 0.0) for lot in ledger[token])
    return amount, usd_value


def _holding_days(disposal_date, acquisition_date):
    try:
        return int((disposal_date - acquisition_date).days)
    except Exception:
        return 0


def calculate_lifo_ledger_and_transactions(swaps_df, deposits_df, sends_df):
    # LIFO implementation
    swaps_df = swaps_df.copy()
    if not swaps_df.empty:
        swaps_df['type'] = 'swap'
    deposits_df = deposits_df.copy()
    if not deposits_df.empty:
        deposits_df['type'] = 'deposit'
    sends_df = sends_df.copy()
    if not sends_df.empty:
        sends_df['type'] = 'send'
    if not swaps_df.empty and 'date' in swaps_df.columns:
        swaps_df['date'] = pd.to_datetime(swaps_df['date'])
    if not deposits_df.empty and 'date' in deposits_df.columns:
        deposits_df['date'] = pd.to_datetime(deposits_df['date'])
    if not sends_df.empty and 'date' in sends_df.columns:
        sends_df['date'] = pd.to_datetime(sends_df['date'])
    for df in [deposits_df, sends_df]:
        for col in ['sent_token', 'sent_amount', 'received_token', 'received_amount', 'usd_value_at_time']:
            if col not in df.columns:
                df[col] = None
    for df in [swaps_df]:
        for col in deposits_df.columns:
            if col not in df.columns:
                df[col] = None
        for col in sends_df.columns:
            if col not in df.columns:
                df[col] = None
    all_txs = pd.concat([swaps_df, deposits_df, sends_df], ignore_index=True, sort=False)
    if 'date' in all_txs.columns:
        all_txs = all_txs.sort_values('date')
    ledger = defaultdict(deque)
    txs = []
    for _, row in all_txs.iterrows():
        tx_type = row['type']
        date = row['date']
        gain_loss = 0.0
        if tx_type == 'deposit':
            token = row['tokenSymbol']
            amount = row['value']
            usd_value = row['usdValue']
            ledger[token].append({'amount': amount, 'usd_value': usd_value, 'date': date})
            bal_amt, bal_usd = _token_totals(ledger, token)
            txs.append({
                'date': date,
                'type': tx_type,
                'token': token,
                'amount': amount,
                'usd_value': usd_value,
                'gain_loss': 0.0,
                'short_term_gain_loss': 0.0,
                'long_term_gain_loss': 0.0,
                'taxable_event': False,
                'wallet_amount_after_display': f"{token}: {bal_amt:,.8f}",
                'wallet_value_after_display': f"{token}: ${bal_usd:,.2f}",
            })
        elif tx_type == 'send':
            token = row['tokenSymbol']
            amount = row['value']
            consumed_usd = 0.0
            lots_consumed = []
            amt_needed = amount
            while amt_needed > 0 and ledger[token]:
                lot = ledger[token][-1]  # LIFO: use last lot
                lot_amount = lot['amount']
                lot_usd = lot['usd_value']
                if lot_amount <= amt_needed:
                    consumed_usd += lot_usd
                    lots_consumed.append({'amount': lot_amount, 'usd_value': lot_usd})
                    amt_needed -= lot_amount
                    ledger[token].pop()
                else:
                    ratio = amt_needed / lot_amount
                    usd_part = lot_usd * ratio
                    lots_consumed.append({'amount': amt_needed, 'usd_value': usd_part})
                    consumed_usd += usd_part
                    lot['amount'] -= amt_needed
                    lot['usd_value'] -= usd_part
                    amt_needed = 0
            short_term_gain_loss = 0.0
            long_term_gain_loss = 0.0
            for lot in lots_consumed:
                lot_amount = lot['amount']
                if amount:
                    proceeds_part = (row['usdValue'] or 0.0) * (lot_amount / amount)
                else:
                    proceeds_part = 0.0
                gain_part = proceeds_part - lot['usd_value']
                held_days = _holding_days(date, lot.get('date'))
                if held_days >= 365:
                    long_term_gain_loss += gain_part
                else:
                    short_term_gain_loss += gain_part
            gain_loss = row['usdValue'] - consumed_usd if row['usdValue'] is not None else 0.0
            remainder = gain_loss - (short_term_gain_loss + long_term_gain_loss)
            if abs(remainder) > 1e-9:
                short_term_gain_loss += remainder
            bal_amt, bal_usd = _token_totals(ledger, token)
            txs.append({
                'date': date,
                'type': tx_type,
                'token': token,
                'amount': amount,
                'usd_value': row['usdValue'],
                'gain_loss': gain_loss,
                'short_term_gain_loss': short_term_gain_loss,
                'long_term_gain_loss': long_term_gain_loss,
                'taxable_event': True,
                'wallet_amount_after_display': f"{token}: {bal_amt:,.8f}",
                'wallet_value_after_display': f"{token}: ${bal_usd:,.2f}",
            })
        elif tx_type == 'swap':
            sent_token = row['sent_token']
            sent_amount = row['sent_amount']
            received_token = row['received_token']
            received_amount = row['received_amount']
            sent_usd = 0.0
            lots_consumed = []
            amt_needed = sent_amount
            while amt_needed > 0 and ledger[sent_token]:
                lot = ledger[sent_token][-1]  # LIFO: use last lot
                lot_amount = lot['amount']
                lot_usd = lot['usd_value']
                if lot_amount <= amt_needed:
                    sent_usd += lot_usd
                    lots_consumed.append({'amount': lot_amount, 'usd_value': lot_usd})
                    amt_needed -= lot_amount
                    ledger[sent_token].pop()
                else:
                    ratio = amt_needed / lot_amount
                    usd_part = lot_usd * ratio
                    lots_consumed.append({'amount': amt_needed, 'usd_value': usd_part})
                    sent_usd += usd_part
                    lot['amount'] -= amt_needed
                    lot['usd_value'] -= usd_part
                    amt_needed = 0
            short_term_gain_loss = 0.0
            long_term_gain_loss = 0.0
            proceeds_total = row['usd_value_at_time'] if row['usd_value_at_time'] is not None else 0.0
            for lot in lots_consumed:
                lot_amount = lot['amount']
                if sent_amount:
                    proceeds_part = proceeds_total * (lot_amount / sent_amount)
                else:
                    proceeds_part = 0.0
                gain_part = proceeds_part - lot['usd_value']
                held_days = _holding_days(date, lot.get('date'))
                if held_days >= 365:
                    long_term_gain_loss += gain_part
                else:
                    short_term_gain_loss += gain_part
            total_sent = sum(lot['amount'] for lot in lots_consumed)
            for lot in lots_consumed:
                portion = lot['amount'] / total_sent if total_sent > 0 else 0
                received_amt = received_amount * portion
                ledger[received_token].append({'amount': received_amt, 'usd_value': lot['usd_value'], 'date': date})
            gain_loss = row['usd_value_at_time'] - sent_usd if row['usd_value_at_time'] is not None else 0.0
            remainder = gain_loss - (short_term_gain_loss + long_term_gain_loss)
            if abs(remainder) > 1e-9:
                short_term_gain_loss += remainder
            sent_bal_amt, sent_bal_usd = _token_totals(ledger, sent_token)
            recv_bal_amt, recv_bal_usd = _token_totals(ledger, received_token)
            txs.append({
                'date': date,
                'type': tx_type,
                'token': f"{sent_token}\n{received_token}",
                'amount': f"{sent_amount}\n{received_amount}",
                'usd_value': row['usd_value_at_time'],
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
    return ledger, txs
