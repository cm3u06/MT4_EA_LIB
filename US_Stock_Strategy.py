# %%
from EACommunicator_API import EACommunicator_API
import sys
#
############################################
# ALL API
############################################
#
#
#    def Disconnect(self):
#    def Connect(self,
#                server: str = 'localhost',
#                port: int = 5555) -> bool:
#    def Check_connection(self) -> bool:
#    def IsConnected(self) -> bool:
#    def Get_instrument_info(self,
#                            instrument: str = 'EURUSD') -> dict:
#    def Update_instruments(self, selected=True) -> bool:
#    def Get_instruments(self, selected=True) ->list:
#    def Get_last_x_ticks_from_now(self,
#                                  instrument: str = 'EURUSD',
#                                  nbrofticks: int = 2000) -> np.array:
#    def Get_last_x_bars_from_now(self,
#                                 instrument: str = 'EURUSD',
#                                 timeframe: str = 'D1',
#                                 nbrofbars: int = 1000) -> np.array:
#    def Get_all_orders(self) -> pd.DataFrame:
#    def readCsv(self, inputCsvString):
#    def Get_all_open_positions(self) -> pd.DataFrame:
#    def Get_all_closed_positions(self) -> pd.DataFrame:
#    def Open_order(self,
#                   instrument: str = '',
#                   ordertype: str = 'buy',
#                   volume: float = 0.01,
#                   openprice: float = 0.0,
#                   slippage: int = 5,
#                   magicnumber: int = 0,
#                   stoploss: float = 0.0,
#                   takeprofit: float = 0.0,
#                   comment: str = '',
#                   market: bool = False
#                   ) -> int:
#    def Close_position_by_ticket(self,
#                                 ticket: int = 0) -> bool:
#    def Close_position_partial_by_ticket(self,
#                                         ticket: int = 0,
#                                         volume_to_close: float = 0.01) -> bool:
#    def Delete_order_by_ticket(self,
#                               ticket: int = 0) -> bool:
#    def Set_sl_and_tp_for_position(self,
#                                   ticket: int = 0,
#                                   stoploss: float = 0.0,
#                                   takeprofit: float = 0.0) -> bool:
#    def Set_sl_and_tp_for_order(self,
#                                ticket: int = 0,
#                                stoploss: float = 0.0,
#                                takeprofit: float = 0.0) -> bool:
#    def Change_settings_for_pending_order(self,
#                                ticket: int = 0,
#                                price: float = -1.0,
#                                stoploss: float = -1.0,
#                                takeprofit: float = -1.0) -> bool:
#    def Get_last_tick_info(self, symbol):
#    def send_command(self,
#                     command: TradingCommands, arguments: str = ''):
#    def get_timeframe_value(self,
#                            timeframe: str = 'M1') -> int:

# %%
mt = EACommunicator_API()
#mt.Connect(server='host.docker.internal')
mt.Connect(server='localhost', port=5555)

# %%

Symbols = mt.Get_instruments(False)

# %%
stocks=[]
for sid in mt.Symbols:
    if '#' in sid and '#ETF' not in sid and '#HK' not in sid:
        tick = mt.Get_last_tick_info(sid)
        #if tick['ask'] <= 100 and tick['ask'] > 50:
        stocks.append(sid)

# %%
#stocks = [
# '#AAPL','#ADBE', '#AMZN', '#BABA', '#COST', '#CPNG', '#EBAY', '#GOOG', '#IBM', '#INTC', '#JNJ', '#KO', 
# '#MMM', '#MO', '#MSFT',  '#NFLX', '#NKE',  '#NVDA',  '#PG',  '#QCOM',  '#SONY',  '#TSLA', '#TSM',  '#UBER',  '#WMT',  ]

#stocks = ['#AIG', '#BABA', '#BBY', '#BK', '#BMY', '#BNPP', '#C', '#CL', '#CVS', '#DAI',
# '#DANO', '#DOCU', '#DUK', '#GILD', '#KO', '#L', '#LVS', '#MET', '#MMM', '#MRNA',
# '#MS', '#NRG', '#OMC', '#PCOR', '#PM', '#PYPL', '#SBUX', '#SCHW', '#SHAK', '#SHOP',
# '#SONY', '#SQ', '#TSN', '#TTEF', '#TWLO', '#UBER', '#W', '#WFC', '#ZM']

#stocks = ['#C']


EU_shares = ['#ADS', '#AIR', '#AIRP', '#ALV', '#AXAF', '#BAS', '#BAYN', '#BEI',
             '#BMW', '#BNPP', '#BOUY', '#CBK', '#DAI', '#DANO', '#DB1', '#DBK', '#DPW', '#DTE',
             '#EON', '#FME', '#HRMS', '#IFX', '#LHA', '#LIN', '#LVMH', '#MICP', '#MUV2', '#OREP',
             '#RWE', '#SAP', '#SIE', '#TTEF', '#VOW']

for eu_sid in EU_shares:
    if eu_sid in stocks:
        stocks.remove(eu_sid)

stocks

# %% [markdown]
# ## Strategy : Stock Hit BBands LB

# %%
import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 100)

def Get_ohlcv(instrument='EURUSD', timeframe='D', nbrofbars=20):
    df = pd.DataFrame(mt.Get_last_x_bars_from_now(instrument, timeframe,nbrofbars))
    df = df.iloc[:, 1:].set_index('Time')
    df.index = pd.to_datetime(df.index)
    df.columns = [c.lower() for c in df.columns]
    return df

# %%
from functools import reduce
def recursive_reduce(cond_dict, conditions):
    def reduce_op(a,b):
        #print(f'OP = {a} {list(cond_dict.keys())[0]} {b}')
        if list(cond_dict.keys())[0]=='|':
            return a|b
        elif list(cond_dict.keys())[0]=='&':
            return a&b
        else:
            return False
    conds = []
    for cond in list(cond_dict.values())[0]:
        if type(cond) == dict:
            conds.append(recursive_reduce(cond, conditions))
        else:
            if cond in conditions:
                conds.append(conditions[cond])
            else:
                print(cond + ' is not in conditions!')
            
    if list(cond_dict.keys())[0]=='~':
        return ~conds[0]
    else:
        return reduce(reduce_op, conds)


# %%
from talib import abstract
from functools import reduce
from tqdm import tqdm
import sys
import datetime as dt

def signal_gen_Strategy__STOCK_HIT_BBANDS_LB(stocks, 
                                             nbrofbars=20,
                                             kd_window=5,
                                             bb_window=5,
                                             bb_near_ratio=0.1,
                                             rr_ratio=1.8,
                                             min_price=10,
                                             now_srv=None,
                                             debug=False):
    __fname__ = '[' + sys._getframe().f_code.co_name + ']'
    if debug:
        print(f'{__fname__}')

    signal = {}
    
    #for sid in stocks:
    for sid in tqdm(stocks):
        info = mt.Get_instrument_info(sid)
        tick = mt.Get_last_tick_info(sid)
        df = Get_ohlcv(instrument=sid, timeframe='D', nbrofbars=nbrofbars)
        KD = abstract.STOCH(df,fastk_period=9)
        BB = abstract.BBANDS(df, 21, 2.1, 2.1)
        

        def KD_bull_divergence(window=13,D_crs1_ub=20, D_crs2_lb=17,debug=False):
            # 2 crs up in window
            KD_diff = (KD['slowk']-KD['slowd'])
            KD_crs_up_pos = ((KD_diff > 0).astype(int).diff()>0)
            KD_crs_up_2 = KD_crs_up_pos.rolling(window).sum()>=2

            # crs up point bounding
            KD_crs_up_pos1 = (KD['slowd'] < D_crs1_ub) & KD_crs_up_pos
            KD_crs_up_pos2 = (KD['slowd'] > D_crs2_lb) & KD_crs_up_pos
            KD_crs_up_pos_order_id = pd.Series(np.nan, index=KD.index)
            KD_crs_up_pos_order_id[KD_crs_up_pos1] =  1
            KD_crs_up_pos_order_id[KD_crs_up_pos2] =  2
            KD_crs_up_pos_in_order = (KD_crs_up_pos_order_id.ffill() == 2) \
            & ((KD_crs_up_pos_order_id==1).rolling(window).sum() > 0) \
            & ((KD_crs_up_pos_order_id==2).rolling(window).sum() > 0)

            # crs up point D increase
            KD_crs_up_pos_d_inc = (KD['slowd'].where(KD_crs_up_pos1 | KD_crs_up_pos2).ffill())
            KD_crs_up_pos_d_inc_until = pd.Series(np.nan, index=KD.index)
            KD_crs_up_pos_d_inc_until[KD_crs_up_pos_d_inc.diff()>0] = True
            KD_crs_up_pos_d_inc_until = KD_crs_up_pos_d_inc_until.ffill()
            
            # crs up point low decrease 
            low_win5 = df['low'].rolling(5).min()
            KD_crs_up_pos_low_not_inc = (low_win5.where(KD_crs_up_pos1 | KD_crs_up_pos2).ffill())
            KD_crs_up_pos_low_not_inc_until = pd.Series(np.nan, index=KD.index)
            KD_crs_up_pos_low_not_inc_until[KD_crs_up_pos_low_not_inc.diff()<=0] = True
            KD_crs_up_pos_low_not_inc_until = KD_crs_up_pos_low_not_inc_until.ffill()

            if debug:
                print('KD_crs_up_pos')
                print(KD_crs_up_pos)
                print('KD_crs_up_2')
                print(KD_crs_up_2)
                print('KD_crs_up_pos1')
                print(KD_crs_up_pos1)
                print('KD_crs_up_pos2')
                print(KD_crs_up_pos2)
                print('KD_crs_up_pos_order_id')
                print(KD_crs_up_pos_order_id)
                print('KD_crs_up_pos_in_order')
                print(KD_crs_up_pos_in_order)
                print('KD_crs_up_pos_d_inc')
                print(KD_crs_up_pos_d_inc)
                print('KD_crs_up_pos_d_inc_until')
                print(KD_crs_up_pos_d_inc_until)
                print('KD_crs_up_pos_low_not_inc')
                print(KD_crs_up_pos_low_not_inc)
                print('KD_crs_up_pos_low_not_inc_until')
                print(KD_crs_up_pos_low_not_inc_until)

            return KD_crs_up_2 & KD_crs_up_pos_in_order & KD_crs_up_pos_d_inc_until & KD_crs_up_pos_low_not_inc_until
    
        conditions = {
            'KD_up_crs_D_lte20_win' : ( \
                (KD['slowk'] <= KD['slowd']).shift() \
                & (KD['slowk'] > KD['slowd']) \
                & (KD['slowd'] <= 20) \
                & ((df['low'] <= BB['lowerband']) \
                | ((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband']) <= 0.5 )) \
            ).rolling(kd_window).sum() > 0,
            'KD_crs3_win10' : \
            ((((KD['slowk'] - KD['slowd']) >= 0).diff() != 0).rolling(10).sum() > 3) \
            & ((KD['slowd'] <= 20).rolling(10).sum() > 7),
            'KD_diff_pos' : (KD['slowk'] - KD['slowd']) >= 0,
            'KD_diff3' : (KD['slowk'] - KD['slowd']) >= 3,
            'KD_diff_inc' : (KD['slowk']-KD['slowd']).diff() > 0,
            'KD_diff_inc_win2' : ((KD['slowk']-KD['slowd']).diff() > 0).rolling(2).sum() >= 2,
            'KD_diff_lt1_win4_shift1' : ((KD['slowk']-KD['slowd']).abs() < 1.0).rolling(4).sum().shift() == 4,
            'KD_K_slope_pos' : KD['slowk'] > KD['slowk'].shift(),
            'KD_D_slope_pos' : KD['slowd'] > KD['slowd'].shift(),
            'KD_D_slope_pos3' : (KD['slowd'] > KD['slowd'].shift()).rolling(3).sum() == 3,
            'KD_D_slope_neg_win4_shift1' : (KD['slowd'] <= KD['slowd'].shift()).rolling(4).sum().shift() == 4,
            'KD_D_lt20_win4_shift1' : (KD['slowd'] < 20).rolling(4).sum().shift() == 4,
            'KD_D_inc6_win2' : (KD['slowd'] - KD['slowd'].shift(2)) >= 6,
            'KD_K_inc6' : (KD['slowk'] - KD['slowk'].shift()) >= 6,
            'KD_K_slope_gte5' : (KD['slowk'] - KD['slowk'].shift()) >= 5,
            'KD_K_slope_gte10' : (KD['slowk'] - KD['slowk'].shift()) >= 10,
            'KD_bull_divergence' : KD_bull_divergence(window=13,D_crs1_ub=20, D_crs2_lb=17,debug=debug),
            'KD_saturate_gt6' : KD['slowd'].rolling(6).max() < 20,
            'BB_nearLB_win' : ( \
                (df['low'] <= BB['lowerband']) \
                | ((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband']) <= bb_near_ratio ) \
            ).rolling(bb_window).sum() > 0,
            'BB_nearLB' : \
                (df['low'] <= BB['lowerband']) \
                | ((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband']) <= 0.3 ),
            'BB_RiskReward' : ( ((BB['upperband']-tick['ask'])/ (tick['ask']-df['low'].rolling(5).min()+tick['spread']*info['point']) ) >= rr_ratio ),
            'BB_nearLB_win13' : ( \
                (df['low'] <= BB['lowerband']) \
                | ((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband']) <= bb_near_ratio ) \
            ).rolling(13).sum() > 0,
            'close_min' : (df['close'] > min_price),
            'close_gt_close1' : (df['close'] > df['close'].shift() ),
            'close_gt_high1' : (df['close'] > df['high'].shift() ),
            'close_gt_close_open_avg1' : (df['close'] > ((df['close']+df['open'])/2).shift() ),
            'close_gt_high_low_avg1' : (df['close'] > ((df['high']+df['low'])/2).shift() ),
            'up_candle1' : (df['close'].shift() > df['open'].shift() ),
            'up_candle' : (df['close'] > df['open'] ),
            'now_srv_gr1500': now_srv.time() >= dt.time(15,0,0),
        }

        cond_dict = \
        {'|' : [
            {'&' : [
                {'|': [
                    {'&':['KD_up_crs_D_lte20_win',
                          {'|':[
                              {'&': [{'|':['KD_D_lt20_win4_shift1','KD_diff_lt1_win4_shift1']}, 'KD_diff_pos','KD_D_inc6_win2']},
                              #{'&': [{'~': [{'|':['KD_saturate_gt6','KD_crs3_win10']}]},{'|':['KD_diff3','KD_K_inc6']}, 'KD_K_slope_pos','KD_D_slope_pos']}
                              {'&': [{'~': [{'|':['KD_D_lt20_win4_shift1','KD_diff_lt1_win4_shift1']}]},'KD_diff_pos','KD_K_slope_pos']}
                          ]}
                         ]},
                ]},
                'BB_RiskReward',
                'BB_nearLB_win',
                'close_min',
                {'|': [
                    {'&':['up_candle1','close_gt_high_low_avg1']},
                    {'&':[ {'~':[ 'up_candle1']}, {'|':['close_gt_high_low_avg1', 'up_candle']}  ]}
                ]}
            ]},
        ]}

        if debug:
            KD['diff'] = KD['slowk'] - KD['slowd']
            KD['diff>0'] = (KD['diff']>0).astype(int)
            KD['(diff>0).diff'] = KD['diff>0'].diff()
            KD['(diff>0).diff>0'] = (KD['(diff>0).diff']>0)
            print(f'price\n{df}')
            print(f'KD\n{KD}')
            print(f'BB\n{BB}')
            print(f'{__fname__}\n{conditions["BB_RiskReward"]}')
            #print(f'{__fname__}  RR_ratio = ({BB["middleband"].iloc[-1]}-{df["close"].iloc[-1]})/({df["close"].iloc[-1]}-{df["low"].iloc[-5:].min()}) = {(BB["middleband"].iloc[-1]-df["close"].iloc[-1])/(df["close"].iloc[-1]-df["low"].iloc[-5:].min())}')
            print(f'RR_ratio = ',
                  f'({BB["upperband"].iloc[-1]:.5f}-{tick["ask"]:.5f})/({tick["ask"]:.5f}-{df["low"].iloc[-5:].min()}+{tick["spread"]*info["point"]:.5f}) = ',
                  (BB["upperband"].iloc[-1]-tick["ask"])/(tick["ask"]-df["low"].iloc[-5:].min()+tick['spread']*info['point']))
            for k,v in conditions.items():
                print(f'{k} : {v.iloc[-1]}')
        
        signal[sid] = recursive_reduce(cond_dict, conditions)
        #signal[sid] =  reduce(lambda a,b:a&b, [conditions[c] for c in cond_sum[0]])]
    
    signal_df = pd.DataFrame(signal)

    return signal_df

# %%


# %%


# %%
from datetime import datetime


def open_trade_Strategy__STOCK_HIT_BBANDS_LB(
    stocks,
    money = 1000,
    timeframe='D',
    nbrofbars=10,
    lotsize_limit=0.005,
    drop_vio_lotsize=False,
    comment = 'STOCK_HIT_BBANDS_LB',
    debug=False):

    __fname__ = '[' + sys._getframe().f_code.co_name + ']'
    money_each_order = money * lotsize_limit
    
    ticket = {}
    for sid in stocks:
        df = Get_ohlcv(instrument=sid, timeframe=timeframe, nbrofbars=nbrofbars)
        print(f'{__fname__} {sid} ==============')
        info = mt.Get_instrument_info(sid)
        tick = mt.Get_last_tick_info(sid)
        ask = tick['ask']
        lowest = df['low'].iloc[-5:].min()
        risk = max(ask - lowest + info['point']*tick['spread'] , info['stop_level']*info['point'], 1)
        stoploss = ask - risk
        #lotsize = max(money_each_order / risk //info['lot_step'] *info['lot_step'] , info['min_lotsize'])
        lotsize = money_each_order / risk //info['lot_step'] *info['lot_step']
        if debug:
            print(f'{__fname__} info = {info}')
            print(f'{__fname__} tick = {tick}')
            print(f'{__fname__} ask = {ask}, lowest = {lowest}')
        print(f'{__fname__} lotsize = {lotsize:.2f}, openprice = {ask}, stoploss = {stoploss}')
        if drop_vio_lotsize:
            print(f'lotsize is too small to open trade !')
            return
        else:
            lotsize = max(info['min_lotsize'], lotsize)
            
        ticket[sid] =  mt.Open_order(
            instrument = sid,
            ordertype = 'buy',
            volume = lotsize,
            openprice = ask,
            slippage = 3,
            stoploss= stoploss,
            takeprofit= 0,
            comment = comment)
            
        print(f'=================================')
        print()

# %%
from datetime import datetime


def close_trade_Strategy__STOCK_HIT_BBANDS_UB(
    timeframe='D',
    nbrofbars=30,
    kd_window=5,
    bb_window=5,
    bb_near_ratio=0.1,
    TrackStopLoss=None,
    debug=False):

    __fname__ = '[' + sys._getframe().f_code.co_name + ']'
    print(f'{__fname__}')
    open_position = mt.Get_all_open_positions()

    for sid in open_position['symbol'].drop_duplicates():
        info = mt.Get_instrument_info(sid)
        tick = mt.Get_last_tick_info(sid)
        df = Get_ohlcv(instrument=sid, timeframe=timeframe, nbrofbars=nbrofbars)
        KD = abstract.STOCH(df,fastk_period=9)
        BB = abstract.BBANDS(df, 21, 2.1, 2.1)
        
    
        conditions = {
           'KD' : ( \
               (KD['slowk'] >= KD['slowd']).shift() \
             & (KD['slowk'] < KD['slowd']) \
             & (KD['slowd'] >= 80) \
           ).rolling(kd_window).sum()>0,
           'BB_NearUB' :  ( \
               (BB['upperband'] <= df['high']) \
             | ((BB['upperband'] - df['high'])/(BB['upperband']-BB['middleband']) <= bb_near_ratio )
            ).rolling(bb_window).sum() > 0,
           'Close' : (df['close'] < df['close'].shift() ),
            'SMA_up4': (BB['middleband'].diff()>0).rolling(4).sum() == 4,
        }

        cond_dict = {'&':[
            'KD',
            'BB_NearUB',
            'Close',
        ]}
        
        sell = recursive_reduce(cond_dict, conditions)
        
        if debug:
            print(f'{df}')
            print(f'{KD}')
            print(f'{BB}')
            print(f'{pd.DataFrame(conditions)}')
            print(f'{sell.iloc[-1]}')
    
        
        for ticket in open_position[open_position['symbol']==sid]['ticket']:
            openprice = open_position[open_position['ticket']==ticket]['openprice'].iloc[0]
            stoploss = open_position[open_position['ticket']==ticket]['stoploss'].iloc[0]
            print(f'ticket = {ticket}, open = {openprice}, SL= {stoploss}')
            if sell.iloc[-1]==True or stoploss >= openprice:
                if TrackStopLoss is not None:
                    newstoploss = 0
                    if TrackStopLoss == 1: # nearest 3 bar
                        newstoploss=max(df['low'].iloc[-3:].min(),openprice)
                    elif TrackStopLoss == 2:
                        hl_max = df[['high','low']].max(axis=1)
                        hl_min = df[['high','low']].min(axis=1)
                        hl_ovlp = (hl_min < hl_max.shift()) & (hl_max > hl_min.shift())
                        hl_ovlp.iloc[-1] = True
                        hl_ovlp_grp = (~hl_ovlp).cumsum()
                        #hl_grp_max = hl_max.iloc[:-1].groupby(by=hl_ovlp_grp.iloc[:-1]).max()

                        oc_max = df[['open','close']].max(axis=1)
                        oc_min = df[['open','close']].min(axis=1)
                        oc_ovlp = (oc_min < oc_max.shift()) & (oc_max > oc_min.shift())
                        oc_ovlp.iloc[-1] = False
                        oc_ovlp_grp = (~oc_ovlp).cumsum()
                        #oc_grp_max = oc_max.iloc[:-1].groupby(by=oc_ovlp_grp.iloc[:-1]).max()

                        if debug:
                            print('hl_ovlp_grp')
                            print(hl_ovlp_grp)
                            print('oc_ovlp_grp')
                            print(oc_ovlp_grp)
                            print('~hl_ovlp_shift1')
                            print((~hl_ovlp).shift(-1))
                            print('~oc_ovlp_shift1')
                            print((~oc_ovlp).shift(-1))

                        if hl_ovlp_grp.iloc[-10:-1].nunique() == 1:
                            if oc_ovlp_grp.nunique() == 1:
                                newstoploss = df['low'].iloc[-3:].min()
                            else:
                                #newstoploss = oc_grp_max.iloc[-2]
                                newstoploss = df['low'].loc[(~oc_ovlp).shift(-1).fillna(False)].iloc[-1]
                        else:
                            if hl_ovlp_grp.nunique() == 1:
                                newstoploss = df['low'].iloc[-3:].min()
                            else:
                                #newstoploss = hl_grp_max.iloc[-2]
                                newstoploss = df['low'].loc[(~hl_ovlp).shift(-1).fillna(False)].iloc[-1]
                        newstoploss=max(newstoploss-tick['spread']*info['point'],openprice)
                    elif TrackStopLoss == 3:
                        if ~((BB['middleband'].iloc[-4:].diff() > 0).all() and KD['slowk'] > 20):
                            newstoploss=max(df['low'].iloc[-3:].min(),openprice)
                            print('********************')
                            print('move stoploss')
                            print('********************')
                        
                    print(f'{newstoploss}')
                    if mt.Set_sl_and_tp_for_position(ticket, newstoploss):
                        print(f'{__fname__} Move SL@{newstoploss} {sid} : {ticket} DONE !')
                    else:
                        print(f'{__fname__} Move SL@{newstoploss} {sid} : {ticket} FAIL !')
                else:
                    if mt.Close_position_by_ticket(ticket):
                        print(f'{__fname__} Close position {sid} : {ticket} DONE !')
                    else:
                        print(f'{__fname__} Close position {sid} : {ticket} FAIL !')


# %%
import datetime as dt

def RemoveOpenPositionEver(symbol_selected: list, strategy_name: str, now_srv: dt=None, debug: bool=False):
    open_position = mt.Get_all_open_positions()
    close_position = mt.Get_all_closed_positions()
    reduced_symbol = symbol_selected[:]
    print(f'[RemoveOpenPositionEver:ALL_SRC_SYMBOL] => {symbol_selected}')
    for symbol in symbol_selected:
            
        if ((open_position['symbol']==symbol) & (open_position['comment'].str.startswith(strategy_name))).any():
            reduced_symbol.remove(symbol)
            if debug:
                print(f'[RemoveOpenPositionEver:OPEN] => {symbol}, {strategy_name}')
                print(open_position)
                print(open_position['symbol']==symbol) 
                print(open_position['comment'].str.startswith(strategy_name))
                print(((open_position['symbol']==symbol) & (open_position['comment'].str.startswith(strategy_name))))
                print(((open_position['symbol']==symbol) & (open_position['comment'].str.startswith(strategy_name))).any())
        elif now_srv is not None:
            today_srv = now_srv.strftime('%Y.%m.%d')
            if debug:
                print(f'[RemoveOpenPositionEver:CLOSE] => {symbol}, {strategy_name}, {today_srv}')
                print(close_position)
                print(close_position['symbol']==symbol) 
                print(close_position['comment'].str.startswith(strategy_name))
                #print(close_position['opentime']==today_srv)
                print(close_position['closetime']==today_srv)
            if ((close_position['symbol']==symbol) & (close_position['comment'].str.startswith(strategy_name)) & (close_position['opentime']==today_srv)).any() :
                reduced_symbol.remove(symbol)

    print(f'[RemoveOpenPositionEver:REDUCED_SYMBOL] => {reduced_symbol}')
    return reduced_symbol

# %%

def Strategy__STOCK_BY_BBANDS_AND_KD(now_srv=None):
    __fname__ = '[' + sys._getframe().f_code.co_name + ']'
    comment='STOCK_HIT_BBANDS_LB'
    
    print(f'{__fname__} Refresh all Stocks ...')
    mt.RefreshRates(instruments=stocks,
                    timeframes=['D'], nbrofbars=30, period=10)
    
    print(f'{__fname__} Close trade ...')
    close_trade_Strategy__STOCK_HIT_BBANDS_UB(
        timeframe='D',
        nbrofbars=30,
        kd_window=5,
        bb_window=5,
        bb_near_ratio=0.1,
        TrackStopLoss=2,
        debug=True)

    
    print(f'{__fname__} Generating trade signals ...')
    signal_df = signal_gen_Strategy__STOCK_HIT_BBANDS_LB(
        stocks=stocks, 
        nbrofbars=30,
        kd_window=5,
        bb_window=5,
        bb_near_ratio=0.15,
        rr_ratio=2.0,
        min_price=10,
        now_srv=now_srv,
        debug=False)
    sid_selected = list(signal_df.columns[signal_df.iloc[-1].fillna(False)])
    
    print(f'{__fname__} Remove opend position from selected stocks ...')
    sid_selected_2 = RemoveOpenPositionEver(sid_selected, strategy_name = comment, now_srv=now_srv, debug=False)
    
    print(f'{__fname__} open trade ...')
    open_trade_Strategy__STOCK_HIT_BBANDS_LB(
        stocks = sid_selected_2,
        money = 10000,
        lotsize_limit=0.005,
        drop_vio_lotsize=False,
        timeframe='D',
        nbrofbars=10,
        debug=False)
    
    return sid_selected, sid_selected_2

# %%
import datetime as dt
import pytz
from dateutil.tz import tzlocal
from threading import Thread, Event
import time


def srv_tz_dst_compensate(src_dt):
    if src_dt.dst()==dt.timedelta(hours=0):
        return src_dt
    else:
        return src_dt-src_dt.dst()

class Strategy_Scheduler:

    def __init__(self, \
                 func,
                 trade_time_list: list = [dt.time(hour=9,minute=30,second=0),dt.time(hour=16,minute=0,second=0)], \
                 tz_srv : dt.timezone = dt.timezone(dt.timedelta(hours=+2)), \
                 tz_mrk : dt.timezone = pytz.timezone('America/New_York'), \
                 idle_second : int = 60*30,
                ):
        self.func            = func
        self.trade_time_list = trade_time_list
        self.tz_srv          = tz_srv
        self.tz_mrk          = tz_mrk
        self.idle_second     = idle_second
        self.trade_interval  = [0] * (len(trade_time_list)-1)
        self.ev = Event()

    def start(self, idle_second=None, end_date_mrk=None, debug=False):
        test_mode     = mt.CheckTestMode(with_break=True)
        if idle_second is not None:
            self.idle_second = idle_second

        def EXEC_LOOP(ev: Event):
            exec_times    = 0
            while not ev.is_set():
                now_srv        = dt.datetime.fromtimestamp(timestamp=mt.GetServerTime(),tz=pytz.utc).replace(tzinfo=self.tz_srv)
                now_mrk        = srv_tz_dst_compensate(now_srv.astimezone(self.tz_mrk))
                now_lca        = now_mrk.astimezone(tzlocal())
                trade_dt_list = [now_mrk.replace(hour=t.hour, minute=t.minute, second=t.second) for t in self.trade_time_list]
                for i in range(len(trade_dt_list)-1):
                    if  (now_mrk.weekday() < 5) \
                        and (now_mrk >= trade_dt_list[i]) \
                        and (now_mrk <  trade_dt_list[i+1]) :
                        if not self.trade_interval[i]:
                            exec_times=exec_times+1
                            print(f"\nStrategy[{self.func.__name__}] Scheduled")
                            print(f"@LocalTime ={now_lca.isoformat(timespec='seconds')}")
                            print(f"@ServerTime={now_srv.isoformat(timespec='seconds')}")
                            print(f"@MarketTime={now_mrk.isoformat(timespec='seconds')}")
                            print(f"exec_times = {exec_times}")
                            kwargs = {'now_srv': now_srv}
                            s1, s2 = self.func(**kwargs)
                            #if debug:
                            #    print(f'[DEBUG]:STRATEGY__STOCK_HIT_BBANDS_LB => selected = {s1}')
                            #    print(f'[DEBUG]:remove opend order => selected = {s2}')
                            self.trade_interval[i] = True
                    else:
                        self.trade_interval[i] = False
    
                print(f"Tick Finished @ServerTime {now_srv.isoformat(timespec='seconds')}")
                mt.Break()

                # early stop; used for MT4 Testing ending
                if end_date_mrk != None and \
                   now_mrk >= end_date_mrk.replace(tzinfo=self.tz_mrk):
                    print("End of session !")
                    self.ev.set()

                cnt = 0
                while cnt < self.idle_second and not ev.is_set():
                    cnt=cnt+1
                    time.sleep(1)

        self.ev.clear()
        self.schedule_thread = Thread(target=EXEC_LOOP, args=(self.ev, ))
        self.schedule_thread.start()

    def stop(self):
        self.ev.set()
        self.schedule_thread.join()
        self.ev.clear()


# %%
scheduler = Strategy_Scheduler(func=Strategy__STOCK_BY_BBANDS_AND_KD, \
                               trade_time_list= [
                                   dt.time(hour=9,minute=30,second=0),
                                   dt.time(hour=10,minute=0,second=0),
                                   dt.time(hour=11,minute=0,second=0),
                                   dt.time(hour=12,minute=0,second=0),
                                   dt.time(hour=13,minute=0,second=0),
                                   dt.time(hour=14,minute=0,second=0),
                                   dt.time(hour=15,minute=0,second=0),
                                   dt.time(hour=16,minute=0,second=0)],
                               idle_second = 0
                              )

# %%
scheduler.stop()

# %%
#scheduler.start(debug=True, end_date_mrk=dt.datetime.strptime('20220808','%Y%m%d'))
scheduler.start(debug=True, end_date_mrk=dt.datetime.strptime('20240308','%Y%m%d'))

# %%
scheduler.start(debug=True, end_date_mrk=dt.datetime.now(tz=tzlocal()).astimezone(pytz.timezone('America/New_York')).replace(hour=15,minute=0,second=0))

# %%
mt.Break()

# %% [markdown]
# ## Backtest

# %%
import datetime as dt
import pytz
from dateutil.tz import tzlocal
timestamp = mt.GetServerTime()

print(dt.datetime.fromtimestamp(timestamp))
print(dt.datetime.fromtimestamp(timestamp, tz=tzlocal()))
print(dt.datetime.fromtimestamp(timestamp, tz=pytz.utc))
print(dt.datetime.fromtimestamp(timestamp, tz=dt.timezone(dt.timedelta(hours=2))))
print(dt.datetime.utcfromtimestamp(timestamp).replace(tzinfo=dt.timezone(dt.timedelta(hours=2))).astimezone(pytz.timezone('America/New_York')))


# %%
def srv_tz_dst_compensate(src_dt):
    if src_dt.dst()==dt.timedelta(hours=0):
        return src_dt
    else:
        return src_dt-src_dt.dst()

# %%


srv_tz_dst_compensate(dt.datetime.utcfromtimestamp(timestamp).replace(tzinfo=dt.timezone(dt.timedelta(hours=2))).astimezone(pytz.timezone('America/New_York')))

# %%


# %%


# %%


# %%
import datetime as dt

dt.datetime(2024,1,1,15,0,1,22).time()

# %%
import pandas as pd
import numpy as np
s=pd.Series(range(20))

# %%
(s<8).shift(-1).fillna(False)

# %%



