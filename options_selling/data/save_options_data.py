from datetime import datetime

def save_chain(conn , chain, ticker):
    '''
    Saves the option chain into the database under the table options_strikes
    '''
    rows = []

    #rows is a list of tuples
    for expiry in chain:
        for strike in chain[expiry]["P"]:
            rows.append((expiry , ticker, "P" , strike))
        for strike in chain[expiry]["C"]:
            rows.append((expiry ,ticker, "C" , strike))

    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO options_strikes (\"expiry\", \"stock_underlying\", \"option_right\", \"strike\") VALUES " + ",".join(cursor.mogrify(
            "(%s,%s,%s, %s)", x).decode("utf-8") for x in rows) +
            " ON CONFLICT (\"expiry\", \"stock_underlying\", \"option_right\", \"strike\") DO NOTHING")
        conn.commit()


def save_options_prices(conn , price_chain , expiry , ticker):
    '''
    Saves the option prices for the current day into the database under the table options_prices
    '''
    curr_date = datetime.today().date()

    rows = []

    for strike in price_chain:
        for right in ["P" , "C"]:
            last_price = None
            bid_price = None
            ask_price = None

            if "last_price" in price_chain[strike][right]:
                last_price = price_chain[strike][right]["last_price"]

            if "bid_price" in price_chain[strike][right]:
                bid_price = price_chain[strike][right]["bid_price"]
            
            if "ask_price" in price_chain[strike][right]:
                ask_price = price_chain[strike][right]["ask_price"]
            
            rows.append( (curr_date, ticker, expiry, right, strike , last_price, bid_price, ask_price))
    
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO options_prices (\"curr_date\", \"stock_underlying\", \"expiry\", \"option_right\", \"strike\", \"last_price\" , \"bid_price\", \"ask_price\") VALUES " + ",".join(cursor.mogrify(
            "(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in rows) +
            " ON CONFLICT (\"curr_date\", \"stock_underlying\", \"expiry\", \"option_right\", \"strike\") DO NOTHING")
        conn.commit()


def save_underlying_price(conn, price, ticker, today = datetime.today().strftime('%Y-%m-%d')):

    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO stock_prices (\"curr_date\", \"stock\" , \"price\") VALUES (\'"+ today + "\', \'" +ticker + "\', " + str(price) + ") ON CONFLICT (\"curr_date\", \"stock\") DO NOTHING")
    conn.commit()


def save_vix_historical(conn, vix_historical):
    '''
    Updates the historical vix data with yesterdays data
    '''
    rows = []
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO vix (\"curr_date\", \"open\", \"close\", \"high\", \"low\") VALUES " + ",".join(cursor.mogrify(
            "(%s,%s,%s,%s,%s)", x).decode("utf-8") for x in rows) +
            " ON CONFLICT (\"curr_date\", \"stock_underlying\", \"expiry\", \"option_right\", \"strike\") DO NOTHING")
    conn.commit()
