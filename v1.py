import pandas as pd

def zigzag(df, depth, deviation, backstep, pip_size):
    i = depth

    zigzag_buffer = pd.Series(0*df['Close'], name='ZigZag')
    high_buffer = pd.Series(0*df['Close'])
    low_buffer = pd.Series(0*df['Close'])

    curlow = 0
    curhigh = 0
    lasthigh = 0
    lastlow = 0

    whatlookfor = 0

    lows = pd.Series(df['Low'].rolling(depth).min())
    highs = pd.Series(df['High'].rolling(depth).max())

    while i + 1 <= df.index[-1]:
        extremum = lows[i]
        if extremum == lastlow:
            extremum = 0
        else:
            lastlow = extremum
            if df.at[i, 'Low']-extremum > deviation*pip_size:
                extremum = 0
            else:
                for back in range(1, backstep + 1):
                    pos = i-back
                    if low_buffer[pos] != 0 and low_buffer[pos] > extremum:
                        low_buffer[pos] = 0

        if df.at[i, 'Low'] == extremum:
            low_buffer[i] = extremum
        else:
            low_buffer[i] = 0

        extremum = highs[i]
        if extremum == lasthigh:
            extremum = 0
        else:
            lasthigh = extremum
            if extremum - df.at[i, 'High'] > deviation*pip_size:
                extremum = 0
            else:
                for back in range(1, backstep + 1):
                    pos = i - back
                    if high_buffer[pos] != 0 and high_buffer[pos] < extremum:
                        high_buffer[pos] = 0

        if df.at[i, 'High'] == extremum:
            high_buffer[i] = extremum
        else:
            high_buffer[i] = 0

        i = i + 1

    lastlow = 0
    lasthigh = 0

    i = depth

    while i + 1 <= df.index[-1]:
        if whatlookfor == 0:
            if lastlow == 0 and lasthigh == 0:
                if high_buffer[i] != 0:
                    lasthigh = df.at[i, 'High']
                    lasthighpos = i
                    whatlookfor = -1
                    zigzag_buffer[i] = lasthigh
                if low_buffer[i] != 0:
                    lastlow = df.at[i, 'Low']
                    lastlowpos = i
                    whatlookfor = 1
                    zigzag_buffer[i] = lastlow
        elif whatlookfor == 1:
            if low_buffer[i] != 0 and low_buffer[i] < lastlow and high_buffer[i] == 0:
                zigzag_buffer[lastlowpos] = 0
                lastlowpos = i
                lastlow = low_buffer[i]
                zigzag_buffer[i] = lastlow
            if high_buffer[i] != 0 and low_buffer[i] == 0:
                lasthigh = high_buffer[i]
                lasthighpos = i
                zigzag_buffer[i] = lasthigh
                whatlookfor = -1
        elif whatlookfor == -1:
            if high_buffer[i] != 0 and high_buffer[i] > lasthigh and low_buffer[i] == 0:
                zigzag_buffer[lasthighpos] = 0
                lasthighpos = i
                lasthigh = high_buffer[i]
                zigzag_buffer[i] = lasthigh
            if low_buffer[i] != 0 and high_buffer[i] == 0:
                lastlow = low_buffer[i]
                lastlowpos = i
                zigzag_buffer[i] = lastlow
                whatlookfor = 1

        i = i + 1

    df = df.join(zigzag_buffer)
    df['Sign'] = df.apply(lambda row: 'Peak' if row['ZigZag'] == row['High'] else ('Trough' if row['ZigZag'] == row['Low'] else None), axis=1)
    return df


df = pd.read_csv('XAUUSD_Candlestick_15_M_BID_22.08.2024-30.09.2024.csv')
df['datetime'] = pd.to_datetime(df['Gmt time'], dayfirst=True)
df = df.drop(['Gmt time'], axis=1)



current_sign = None
current_trough = {
    'datetime': 0,
    'price': 0,
    'bars_passed': 0
}
current_peak = {
    'datetime': 0,
    'price': 0,
    'bars_passed': 0
}
buy_stop_order = {}
sell_stop_order = {}
orders_to_activate = []
p = 0
q = 1
for i in range(1,len(df)+1):
    df2 = zigzag(df.iloc[p:q].reset_index(drop=True), depth=12, deviation=5, backstep=7, pip_size=0.01) # run zigzag function on selected slice of dataframe
    if len(df2[df2['Sign'].notna()]) > 0:
        current_sign = df2[df2['Sign'].notna()]['Sign'].tolist()[-1] # save the last sign made, will be 'Peak' or 'Trough'
        current_sign_datetime = df2[df2['Sign'].notna()]['datetime'].tolist()[-1] # save when that last sign was made
        current_sign_price = df2[df2['Sign'].notna()]['ZigZag'].tolist()[-1] # save the price at what that sign was
        if current_sign == 'Trough':
            if (current_trough['datetime'] != current_sign_datetime) & (current_trough['price'] != current_sign_price): # troughs can move with every new bar that arrives, so if there is a new trough save it with the above variables alongside one bar being passed (since the zigzag function can only recognise a sign if at least one bar has been made after it)
                current_trough['datetime'] = current_sign_datetime
                current_trough['price'] = current_sign_price
                current_trough['bars_passed'] = 1
            else: # if trough stays the same as new bars arrive, the datetime and price stay the same while bars passed increases by 1 every time
                current_trough['datetime'] = current_sign_datetime
                current_trough['price'] = current_sign_price
                current_trough['bars_passed'] += 1
            if (len(df2[df2['Sign'] == 'Peak']) > 0):
                if (current_peak['datetime'] != df2[df2['Sign'] == 'Peak']['datetime'].tolist()[-1]) & (current_peak['price'] != df2[df2['Sign'] == 'Peak']['ZigZag'].tolist()[-1]): # double checks that the current_peak is actually the real peak from the data, if not then reset the current_peak since the current sign is a 'Trough'
                    current_peak = {
                        'datetime': 0,
                        'price': 0,
                        'bars_passed': 0
                    }
            if current_peak['datetime'] != 0: 
                current_peak['bars_passed'] += 1 # current peaks can also exist alongside a the current sign being a 'Trough' before they're executed as a buy stop, so we need to also account for bars passing until they're confirmed and executed
        if current_sign == 'Peak':
            if (current_peak['datetime'] != current_sign_datetime) & (current_trough['price'] != current_sign_price): # peaks can move with every new bar that arrives, so if there is a new peak save it with the above variables alongside one bar being passed (since the zigzag function can only recognise a sign if at least one bar has been made after it)
                current_peak['datetime'] = current_sign_datetime
                current_peak['price'] = current_sign_price
                current_peak['bars_passed'] = 1
            else: # if peak stays the same as new bars arrive, the datetime and price stay the same while bars passed increases by 1 every time
                current_peak['datetime'] = current_sign_datetime
                current_peak['price'] = current_sign_price
                current_peak['bars_passed'] += 1
            if (len(df2[df2['Sign'] == 'Trough']) > 0):
                if (current_trough['datetime'] != df2[df2['Sign'] == 'Trough']['datetime'].tolist()[-1]) & (current_trough['price'] != df2[df2['Sign'] == 'Trough']['ZigZag'].tolist()[-1]): # double checks that the current_trough is actually the real trough from the data, if not then reset the current_trough since the current sign is a 'Peak'
                    current_trough = {
                        'datetime': 0,
                        'price': 0,
                        'bars_passed': 0
                    }
            if current_trough['datetime'] != 0:
                current_trough['bars_passed'] += 1 # current troughs can also exist alongside a the current sign being a 'Peak' before they're executed as a sell stop, so we need to also account for bars passing until they're confirmed and executed
        if df2['datetime'].tolist()[-1].minute == 0: # if time its the hour mark
            if current_trough['bars_passed'] >= 6: # if at least 6 bars pass
                if f"sell_{current_trough['price']}" not in [f"{i['order']}_{i['price']}" for i in orders_to_activate]: # making sure that we haven't placed this sell stop already in our orders
                    if df2['Low'].tolist()[-1] < current_trough['price']: # if the newest bar's Low is lower than the current_trough, don't make the order and instead start looking for either a Peak or Trough by resetting the dataframe from this current timestamp
                        p = df2.index[-1]
                    if df2['Low'].tolist()[-1] > current_trough['price']: # if the newest bar's Low is higher than the current_trough, we can place the sell stop finally
                        print(f"Placing sell stop order at price {current_trough['price']} at {df2['datetime'].tolist()[-1]}")
                        sell_stop_order['order'] = 'sell'
                        sell_stop_order['price'] = current_trough['price']
                        sell_stop_order['datetime'] = df2['datetime'].tolist()[-1]
                        orders_to_activate.append(sell_stop_order)
                        current_trough = {
                            'datetime': 0,
                            'price': 0,
                            'bars_passed': 0
                        }

            if current_peak['bars_passed'] >= 6: # if at least 6 bars pass
                if f"buy_{current_peak['price']}" not in [f"{i['order']}_{i['price']}" for i in orders_to_activate]: # making sure that we haven't placed this buy stop already in our orders
                    if df2['High'].tolist()[-1] > current_peak['price']: # if the newest bar's High is higher than the current_peak, don't make the order and instead start looking for either a Peak or Trough by resetting the dataframe from this current timestamp
                        p = df2.index[-1]
                    if df2['High'].tolist()[-1] < current_peak['price']: # if the newest bar's High is lower than the current_peak, we can place the buy stop finally
                        print(f"Placing buy stop order at price {current_peak['price']} at {df2['datetime'].tolist()[-1]}")
                        buy_stop_order['order'] = 'buy'
                        buy_stop_order['price'] = current_peak['price']
                        buy_stop_order['datetime'] = df2['datetime'].tolist()[-1]
                        orders_to_activate.append(buy_stop_order)
                        current_peak = {
                            'datetime': 0,
                            'price': 0,
                            'bars_passed': 0
                        }

    q += 1 # getting the new bar in
