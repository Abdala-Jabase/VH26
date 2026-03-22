import pandas as pd


def _clean_numeric(val):
    if isinstance(val, str):
        return float(val.replace(',', '.'))
    return val


def parse_eth_transactions(file_path, user_address, write_csv=False):
    # 1. Load the CSV (using ';' as separator based on file structure)
    df = pd.read_csv(file_path, sep=';')

    # 2. Clean numeric columns (convert '123,45' to 123.45)
    cols_to_clean = ['value', 'usdValue', 'usdPrice']
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric)

    # 3. Identify Transaction Types by grouping by Hash
    grouped = df.groupby('txHash')
    
    swaps_list = []
    deposits_list = []
    sends_list = []

    user_address = user_address.lower()

    for tx_hash, group in grouped:
        is_from_user = group['fromAddress'].str.lower() == user_address
        is_to_user = group['toAddress'].str.lower() == user_address
        
        out_rows = group[is_from_user]
        in_rows = group[is_to_user]

        # Classification Logic
        if not out_rows.empty and not in_rows.empty:
            # Swap: Sent something and received something in one hash
            swaps_list.append({
                'date': group['date'].iloc[0],
                'txHash': tx_hash,
                'sent_token': ", ".join(out_rows['tokenSymbol']),
                'sent_amount': out_rows['value'].sum(),
                'received_token': ", ".join(in_rows['tokenSymbol']),
                'received_amount': in_rows['value'].sum(),
                'usd_value_at_time': out_rows['usdValue'].sum()
            })
        elif not in_rows.empty:
            # Deposit: User only received
            deposits_list.append(in_rows)
        elif not out_rows.empty:
            # Send: User only sent
            sends_list.append(out_rows)

    # 4. Create DataFrames and Save
    swaps_df = pd.DataFrame(swaps_list)
    deposits_df = pd.concat(deposits_list) if deposits_list else pd.DataFrame()
    sends_df = pd.concat(sends_list) if sends_list else pd.DataFrame()

    if write_csv:
        swaps_df.to_csv('swaps.csv', index=False)
        deposits_df.to_csv('deposits.csv', index=False)
        sends_df.to_csv('sends.csv', index=False)

    print(f"Success! Found {len(swaps_df)} swaps, {len(deposits_df)} deposits, and {len(sends_df)} sends.")

    return (swaps_df, deposits_df, sends_df)


def cumulative_token_balances_from_raw_csv(
    file_path,
    user_address,
    write_csv=False,
    opening_balances=None,
):
    """
    Build running token balances directly from the original ethplorer export.

    Direction is determined relative to `user_address`:
    - incoming transfer -> positive delta
    - outgoing transfer -> negative delta

    Returns one row per raw transfer line with running totals per token.

    opening_balances:
    Optional dict like {"ETH": 2.5, "USDC": 1000}. These are added to
    running totals so partial-history exports can be reconciled.
    """
    df = pd.read_csv(file_path, sep=';')
    user_address = user_address.lower()

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    for col in ['value', 'usdValue', 'usdPrice']:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric)

    from_is_user = df['fromAddress'].str.lower() == user_address
    to_is_user = df['toAddress'].str.lower() == user_address

    relevant = df[from_is_user | to_is_user].copy()
    if relevant.empty:
        return relevant

    relevant['direction'] = 'internal'
    relevant.loc[to_is_user & ~from_is_user, 'direction'] = 'in'
    relevant.loc[from_is_user & ~to_is_user, 'direction'] = 'out'

    relevant['delta_amount'] = 0.0
    relevant.loc[relevant['direction'] == 'in', 'delta_amount'] = relevant['value']
    relevant.loc[relevant['direction'] == 'out', 'delta_amount'] = -relevant['value']

    relevant['delta_usd_value'] = 0.0
    relevant.loc[relevant['direction'] == 'in', 'delta_usd_value'] = relevant['usdValue']
    relevant.loc[relevant['direction'] == 'out', 'delta_usd_value'] = -relevant['usdValue']

    relevant = relevant.sort_values(['date', 'txHash'], kind='stable').reset_index(drop=True)
    relevant['running_amount'] = relevant.groupby('tokenSymbol')['delta_amount'].cumsum()
    relevant['running_usd_value'] = relevant.groupby('tokenSymbol')['delta_usd_value'].cumsum()

    opening_balances = opening_balances or {}
    relevant['opening_amount'] = relevant['tokenSymbol'].map(opening_balances).fillna(0.0)
    relevant['running_amount_adjusted'] = relevant['running_amount'] + relevant['opening_amount']

    output_cols = [
        'date',
        'txHash',
        'tokenSymbol',
        'direction',
        'value',
        'usdValue',
        'delta_amount',
        'delta_usd_value',
        'running_amount',
        'opening_amount',
        'running_amount_adjusted',
        'running_usd_value',
    ]
    cumulative_df = relevant[output_cols]

    if write_csv:
        cumulative_df.to_csv('ethplorer.csv', index=False)

    return cumulative_df
