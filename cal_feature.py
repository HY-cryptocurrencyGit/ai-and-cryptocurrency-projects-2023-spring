import pandas as pd
import numpy as np
import time

def get_mid_price(bid,ask):
        return (bid.iloc[0,0] + ask.iloc[0,0])/2

def get_mid_price_wt(bid,ask):
    return (bid['price'].mean() + ask['price'].mean())/2

def get_mid_price_mkt(bid,ask):
    return (bid.iloc[0,0]*ask.iloc[0,1] + ask.iloc[0,0]*bid.iloc[0,1]) \
                              / (bid.iloc[0,1] + ask.iloc[0,1])

def get_book_imbalance(bid ,ask, ratio, mid_price):
    bidQty, askQty = (bid.iloc[:,1]**ratio).sum(), (ask.iloc[:,1]**ratio).sum()
    bidPx, askPx = (bid['price']*bid.iloc[:,1]**ratio).sum(), (ask['price']*ask.iloc[:,1]**ratio).sum()
    return ((askQty*bidPx/ bidQty + bidQty*askPx/askQty)/(bidQty + askQty) - mid_price)

def get_cumulative(past_mid_price, sec = 10):
    try:
        idx = np.where(past_mid_price == 0)[0][0]
    except:
        idx = len(past_mid_price)
    if idx <= sec:
        return past_mid_price[:idx].sum() / idx
    else:
        return past_mid_price[int(idx-sec):idx].sum() / sec

date = ['2023-05-14-bithumb-BTC','2023-05-15-bithumb-BTC',\
        '2023-05-16-bithumb-BTC']
for d in date:
    df = pd.read_csv('/home/user/다운로드/'+d+'-book.csv')
    # trdbook = pd.read_csv('/home/user/다운로드/2023-05-10-upbit-BTC-trade.csv')
    feature = pd.DataFrame()

    tic = time.time()
    tmp_df = pd.DataFrame(df.groupby('type'))
    bid_df = pd.DataFrame(tmp_df.iloc[0,1].groupby('timestamp'))
    ask_df = pd.DataFrame(pd.DataFrame(tmp_df.iloc[1,1].groupby('timestamp')))
    feature['timestamp'] = bid_df.iloc[:,0]
    toc = time.time()
    print('데이터 전처리 시간:', toc-tic)




    mid_price = np.zeros(len(bid_df))
    mid_price_wt = np.zeros(len(bid_df))
    mid_price_mkt = np.zeros(len(bid_df))

    cumulative_mid_price = np.zeros(len(bid_df))
    cumulative_mid_price_wt = np.zeros(len(bid_df))
    cumulative_mid_price_mkt = np.zeros(len(bid_df))

    book_imbalance1 = np.zeros(len(bid_df))
    book_imbalance2 = np.zeros(len(bid_df))
    book_imbalance3 = np.zeros(len(bid_df))

    ratio1 = 0.05
    ratio2 = 0.1
    ratio3 = 0.2

    tic = time.time()
    for i in range(len(bid_df)):
        
        bid_tmp = bid_df.iloc[i,1].sort_values('price',ascending =False)
        ask_tmp = ask_df.iloc[i,1].sort_values('price',ascending = True)
        
        mid_price[i] = get_mid_price(bid_tmp, ask_tmp)
        mid_price_wt[i] = get_mid_price_wt(bid_tmp,ask_tmp)
        mid_price_mkt[i] = get_mid_price_mkt(bid_tmp, ask_tmp)
        
        cumulative_mid_price[i] = get_cumulative(mid_price, 10)
        cumulative_mid_price_wt[i] = get_cumulative(mid_price_wt, 10)
        cumulative_mid_price_mkt[i] = get_cumulative(mid_price_mkt, 10)

        book_imbalance1[i] = get_book_imbalance(bid_tmp, ask_tmp, ratio1, \
                                                mid_price[i])
        book_imbalance2[i] = get_book_imbalance(bid_tmp, ask_tmp, ratio2, \
                                                mid_price[i])
        book_imbalance3[i] = get_book_imbalance(bid_tmp, ask_tmp, ratio3, \
                                                mid_price[i])
        if i%10000 == 0: print(i)
    toc = time.time()
    print('feature 계산 시간 및 날짜', d, toc - tic)
    feature['midprice'] = mid_price
    feature['midprice_wt'] = mid_price_wt
    feature['midprice_mkt'] = mid_price_mkt

    feature['cumulative_midprice'] = cumulative_mid_price
    feature['cumulative_midprice_wt'] = cumulative_mid_price_wt
    feature['cumulative_midprice_mkt'] = cumulative_mid_price_mkt

    feature['book_imbalance_0.05-5-1'] = book_imbalance1
    feature['book_imbalance_0.1-5-1'] = book_imbalance2
    feature['book_imbalance_0.2-5-1'] = book_imbalance3

    feature.to_csv('/home/user/다운로드/'+d+'-feature.csv')
    
