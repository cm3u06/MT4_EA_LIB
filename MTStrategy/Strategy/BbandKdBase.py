
from talib import abstract
from functools import reduce
from tqdm import tqdm
import datetime as dt
from MTStrategy.EACommunicator_API import *
from MTStrategy.utils import *
from .Strategy import Strategy
from . import DEBUG

class BbandKdBase(Strategy):

    def __init__(self, mt : EACommunicator_API, symbols:list, **kwargs):
        super().__init__(mt, symbols, **kwargs)

        # default parameter
        parms = {
            'entry.nbrofbars': 30,
            'entry.bb_near_ratio': 0.15,
            'entry.bb_window': 5,
            'entry.rr_ratio':2.0,
            'entry.min_price':10,
            'open_trade.money':10000,
            'open_trade.lotsize_limit':0.005,
            'open_trade.timeframe':'D',
            'open_trade.drop_vio_lotsize':False,
            'exit.timeframe':'D',
            'exit.nbrofbars':30,
            'exit.kd_window':5,
            'exit.bb_window':5,
            'exit.bb_near_ratio':0.15,
            'close_trade.SL_mode': 2,
            'close_trade.timeframe':'D',
            'close_trade.nbrofbars':30,
        }

        self.kwargs = parms | self.kwargs # kwargs will overwrite parms

        print(f'[{self.__name__}:INFO] parameters ')
        for k,v in self.kwargs.items():
            print(f'{k:<35} = {v:>20}')


    def entry_signal(self):
        __fname__ = f'{self.__name__}:entry_signal'

        signal = {}
        #if DEBUG['BbandKdBase.entry_signal']:
        print(f'[{__fname__}:DEBUG] {self.symbols}')
        
        #for sid in stocks:
        for sid in tqdm(self.symbols):
            #if DEBUG['BbandKdBase.entry_signal']:
            print(f'[{__fname__}:DEBUG] start')
            info = self.mt.Get_instrument_info(sid)
            #if DEBUG['BbandKdBase.entry_signal']:
            print(f'[{__fname__}:DEBUG] info = ', info)
            tick = self.mt.Get_last_tick_info(sid)
            #if DEBUG['BbandKdBase.entry_signal']:
            print(f'[{__fname__}:DEBUG] tick = ', tick)
            df = self.mt.Get_ohlcv(instrument=sid, timeframe='D', nbrofbars=self.kwargs['entry.nbrofbars'])
            #if DEBUG['BbandKdBase.entry_signal']:
            print(f'[{__fname__}:DEBUG] price = ', df)
            KD = abstract.STOCH(df,fastk_period=9)
            BB = abstract.BBANDS(df, 21, 2.1, 2.1)
            

            conditions = {
                'KD_up_crs_D_lte20_win10' : ( \
                    (KD['slowk'] <= KD['slowd']).shift() \
                    & (KD['slowk'] > KD['slowd']) \
                    & (KD['slowd'] <= 20) \
                    & (((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband'])).shift() <= 0.15 )) \
                .rolling(10).sum() > 0,
                'KD_crs3_win10' : \
                ((((KD['slowk'] - KD['slowd']) >= 0).diff() != 0).rolling(10).sum() > 3) \
                & ((KD['slowd'] <= 20).rolling(10).sum() > 7),
                'KD_diff_pos' : (KD['slowk'] - KD['slowd']) >= 0,
                'KD_diff3' : (KD['slowk'] - KD['slowd']) >= 3,
                'KD_diff_inc' : (KD['slowk']-KD['slowd']).diff() > 0,
                'KD_diff_inc_win2' : ((KD['slowk']-KD['slowd']).diff() > 0).rolling(2).sum() >= 2,
                'KD_diff_lt3_win4_shift1' : ((KD['slowk']-KD['slowd']).abs() < 3.0).rolling(4).sum().shift() == 4,
                'KD_K_slope_pos' : KD['slowk'] > KD['slowk'].shift(),
                'KD_D_slope_pos' : KD['slowd'] > KD['slowd'].shift(),
                'KD_D_slope_pos3' : (KD['slowd'] > KD['slowd'].shift()).rolling(3).sum() == 3,
                'KD_D_slope_neg_win4_shift1' : (KD['slowd'] <= KD['slowd'].shift()).rolling(4).sum().shift() == 4,
                'KD_D_lt20_win4_shift1' : (KD['slowd'] < 20).rolling(4).sum().shift() == 4,
                'KD_D_inc6_win2' : (KD['slowd'] - KD['slowd'].shift(2)) >= 6,
                'KD_K_inc4' : (KD['slowk'] - KD['slowk'].shift()) >= 4,
                'KD_K_slope_gte5' : (KD['slowk'] - KD['slowk'].shift()) >= 5,
                'KD_K_slope_gte10' : (KD['slowk'] - KD['slowk'].shift()) >= 10,
                'KD_saturate_gt6' : KD['slowd'].rolling(6).max() < 20,
                'BB_nearLB_win' : ( \
                    ((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband']) <= self.kwargs['entry.bb_near_ratio'] ) \
                ).rolling(self.kwargs['entry.bb_window']).sum() > 0,
                'BB_nearLB' : ((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband']) <= 0.7 ),
                'BB_RiskReward' : ( ((BB['upperband']-tick['ask'])/ (tick['ask']-df['low'].rolling(5).min()+tick['spread']*info['point']) ) >= self.kwargs['entry.rr_ratio'] ),
                'BB_nearLB_win13' : ( \
                    (df['low'] <= BB['lowerband']) \
                    | ((df['low'] - BB['lowerband'])/(BB['middleband']-BB['lowerband']) <= self.kwargs['entry.bb_near_ratio'] ) \
                ).rolling(13).sum() > 0,
                'close_min' : (df['close'] > self.kwargs['entry.min_price']),
                'close_gt_close1' : (df['close'] > df['close'].shift() ),
                'close_gt_high1' : (df['close'] > df['high'].shift() ),
                'close_gt_close_open_avg1' : (df['close'] > ((df['close']+df['open'])/2).shift() ),
                'close_gt_high_low_avg1' : (df['close'] > ((df['high']+df['low'])/2).shift() ),
                'up_candle1' : (df['close'].shift() > df['open'].shift() ),
                'up_candle' : (df['close'] > df['open'] ),
                'now_srv_gt1500': self.now_srv.time() >= dt.time(15,0,0),
            }

            cond_dict = \
            {'&' : [
                    {'&':['KD_up_crs_D_lte20_win10',
                        {'|':[
                            {'&': [{'|':['KD_D_lt20_win4_shift1','KD_diff_lt3_win4_shift1']}, 'KD_diff_pos','KD_D_inc6_win2','KD_diff_inc']},
                            #{'&': [{'~': [{'|':['KD_saturate_gt6''KD_crs3_win10']}]},{'|':['KD_diff3','KD_K_inc6']}, 'KD_K_slope_pos','KD_D_slope_pos']}
                            {'&': [
                                {'~': [{'|':['KD_D_lt20_win4_shift1','KD_diff_lt3_win4_shift1']}]},
                                'KD_diff_pos',
                                {'|':[
                                    {'&':['KD_K_slope_pos', 'KD_D_slope_pos']},
                                    'KD_K_inc4'
                                ]}
                            ]}
                        ]}
                    ]},
                    'BB_RiskReward',
                    'BB_nearLB',
                    'close_min',
                    {'|': [
                        {'&':['up_candle1','close_gt_high_low_avg1']},
                        {'&':[ {'~':[ 'up_candle1']}, {'|':['close_gt_high_low_avg1', 'up_candle']}, 'now_srv_gt1500'  ]}
                    ]}
            ]}

            
            signal[sid] = recursive_reduce(cond_dict, conditions)
        
        signal_df = pd.DataFrame(signal)

        entry_symbols_0 = list(signal_df.columns[signal_df.iloc[-1]])

        entry_symbols_1 = self.filter(entry_symbols_0)

        return entry_symbols_1


    def filter(self, symbols: list):
        __fname__ = f'{self.__name__}:filter'

        open_position = self.mt.Get_all_open_positions()
        close_position = self.mt.Get_all_closed_positions()
        filter_symbols = symbols[:]
        print(f'[{__fname__}:symbols] => {symbols}')
        for symbol in symbols:
                
            if ((open_position['symbol']==symbol) & (open_position['comment'].str.startswith(self.__name__))).any():
                filter_symbols.remove(symbol)
            elif self.now_srv is not None:
                today_srv = self.now_srv.strftime('%Y.%m.%d')
                if ((close_position['symbol']==symbol) & (close_position['comment'].str.startswith(self.__name__)) & (close_position['opentime']==today_srv)).any() :
                    filter_symbols.remove(symbol)

        print(f'[{__fname__}:filter_symbols] => {filter_symbols}')
        return filter_symbols


    def open_trade(self, entry_symbols:list):
        __fname__ = f'{self.__name__}:open_trade'

        money_each_order = self.kwargs['open_trade.money'] * self.kwargs['open_trade.lotsize_limit']

        if DEBUG['BbandKdBase.open_trade']:
            print(f'[{__fname__}:DEBUG] {entry_symbols}')
        
        ticket = {}
        for sid in entry_symbols:
            if DEBUG['BbandKdBase.open_trade']:
                print(f'[{__fname__}:DEBUG] {sid}')
            df = self.mt.Get_ohlcv(instrument=sid, timeframe=self.kwargs['open_trade.timeframe'], nbrofbars=20)
            if DEBUG['BbandKdBase.open_trade']:
                print(f'[{__fname__}:DEBUG] price = {df}')
            info = self.mt.Get_instrument_info(sid)
            if DEBUG['BbandKdBase.open_trade']:
                print(f'[{__fname__}:DEBUG] info = {info}')
            tick = self.mt.Get_last_tick_info(sid)
            if DEBUG['BbandKdBase.open_trade']:
                print(f'[{__fname__}:DEBUG] tick = {tick}')
            ask = tick['ask']
            lowest = df['low'].iloc[-5:].min()
            risk = max(ask - lowest + info['point']*tick['spread'] , info['stop_level']*info['point'], 1)
            stoploss = ask - risk
            #lotsize = max(money_each_order / risk //info['lot_step'] *info['lot_step'] , info['min_lotsize'])
            lotsize = money_each_order / risk //info['lot_step'] *info['lot_step']
            print(f'[{__fname__}:INFO] {sid} ==============')
            if DEBUG['BbandKdBase.open_trade']:
                print(f'[{__fname__}:DEBUG] ask = {ask}, lowest = {lowest}')
            print(f'[{__fname__}:INFO] lotsize = {lotsize:.2f}, openprice = {ask}, stoploss = {stoploss}\n')
            if self.kwargs['open_trade.drop_vio_lotsize']:
                print(f'[{__fname__}:WARNING] lotsize is too small to open trade !')
                return
            else:
                lotsize = max(info['min_lotsize'], lotsize)
                
            ticket[sid] =  self.mt.Open_order(
                instrument = sid,
                ordertype = 'buy',
                volume = lotsize,
                openprice = ask,
                slippage = 3,
                stoploss= stoploss,
                takeprofit= 0,
                comment = self.__name__)
                

    def exit_signal(self):
        __fname__ = f'{self.__name__}:exit_signal'

        open_position = self.mt.Get_all_open_positions()
        exit_symbols = []

        for sid in open_position['symbol'].drop_duplicates():
            df = self.mt.Get_ohlcv(instrument=sid,
                                   timeframe=self.kwargs['exit.timeframe'],
                                   nbrofbars=self.kwargs['exit.nbrofbars'])
            KD = abstract.STOCH(df,fastk_period=9)
            BB = abstract.BBANDS(df, 21, 2.1, 2.1)
            
        
            conditions = {
            'KD' : ( \
                (KD['slowk'] >= KD['slowd']).shift() \
                & (KD['slowk'] < KD['slowd']) \
                & (KD['slowd'] >= 80) \
            ).rolling(self.kwargs['exit.kd_window']).sum()>0,
            'BB_NearUB' :  ( \
                (BB['upperband'] <= df['high']) \
                | ((BB['upperband'] - df['high'])/(BB['upperband']-BB['middleband']) <= self.kwargs['exit.bb_near_ratio'] )
                ).rolling(self.kwargs['exit.bb_window']).sum() > 0,
            'Close' : (df['close'] < df['close'].shift() ),
                'SMA_up4': (BB['middleband'].diff()>0).rolling(4).sum() == 4,
            }

            cond_dict = {'&':[
                'KD',
                'BB_NearUB',
                'Close',
            ]}
            
            sell = recursive_reduce(cond_dict, conditions)

            if sell.iloc[-1]:
                exit_symbols.append(sid)

        return exit_symbols
            
    
    def close_trade(self, exit_symbols:list):
        __fname__ = f'{self.__name__}:close_trade'

        open_position = self.mt.Get_all_open_positions()
            
        for id, order in open_position.iterrows():
            ticket = order['ticket']
            sid    = order['symbol']
            openprice = order['openprice']
            stoploss = order['stoploss']
            comment = order['comment']
            print(f'[{__fname__}:INFO] ticket = {ticket}, IN = {openprice}, SL= {stoploss}')

            if comment == self.__name__ and \
            (sid in exit_symbols or stoploss >= openprice):

                info = self.mt.Get_instrument_info(sid)
                tick = self.mt.Get_last_tick_info(sid)
                df = self.mt.Get_ohlcv(instrument=sid,
                                    timeframe=self.kwargs['close_trade.timeframe'],
                                    nbrofbars=self.kwargs['close_trade.nbrofbars'])

                if self.kwargs.get('close_trade.SL_mode'):
                    newstoploss = 0



                    if self.kwargs['close_trade.SL_mode'] == 1: # nearest 3 bar
                        newstoploss=df['low'].iloc[-3:].min()
                    elif self.kwargs['close_trade.SL_mode'] == 2:
                        hl_max = df[['high','low']].max(axis=1)
                        hl_min = df[['high','low']].min(axis=1)
                        hl_ovlp = (hl_min < hl_max.shift()) & (hl_max > hl_min.shift())
                        hl_ovlp_grp = (~hl_ovlp).cumsum()
                        hl_1st_grp_min = df['low'].loc[hl_ovlp_grp==hl_ovlp_grp.iloc[-2]].min()
                        hl_grp_sta_m1 = (~hl_ovlp).iloc[:-1].shift(-1).fillna(False)
                        if hl_grp_sta_m1.any():
                            hl_2nd_grp_last_min = df['low'].iloc[:-1].loc[hl_grp_sta_m1].iloc[-1]
                        else:
                            hl_2nd_grp_last_min = hl_1st_grp_min
                        if hl_ovlp_grp.iloc[-2] >= 2:
                            hl_2nd_grp_min = df['low'].loc[hl_ovlp_grp==hl_ovlp_grp.iloc[-2]-1].min()
                        else:
                            hl_2nd_grp_min = hl_1st_grp_min
                        #hl_grp_max = hl_max.iloc[:-1].groupby(by=hl_ovlp_grp.iloc[:-1]).max()

                        oc_max = df[['open','close']].max(axis=1)
                        oc_min = df[['open','close']].min(axis=1)
                        oc_ovlp = (oc_min < oc_max.shift()) & (oc_max > oc_min.shift())
                        oc_ovlp_grp = (~oc_ovlp).cumsum()
                        oc_1st_grp_min = df['low'].loc[oc_ovlp_grp==oc_ovlp_grp.iloc[-2]].min()
                        oc_grp_sta_m1 = (~oc_ovlp).iloc[:-1].shift(-1).fillna(False)
                        if oc_grp_sta_m1.any():
                            oc_2nd_grp_last_min = df['low'].iloc[:-1].loc[oc_grp_sta_m1].iloc[-1]
                        else:
                            oc_2nd_grp_last_min = oc_1st_grp_min
                        if oc_ovlp_grp.iloc[-2] >= 2:
                            oc_2nd_grp_min = df['low'].loc[oc_ovlp_grp==oc_ovlp_grp.iloc[-2]-1].min()
                        else:
                            oc_2nd_grp_min = oc_1st_grp_min

                        #oc_grp_max = oc_max.iloc[:-1].groupby(by=oc_ovlp_grp.iloc[:-1]).max()

                        if hl_ovlp_grp.iloc[-15:-1].nunique() == 1:
                        #if False:
                            #if oc_ovlp_grp.nunique() == 1:
                            #    newstoploss = df['low'].iloc[-3:].min()
                            #else:
                                #newstoploss = oc_grp_max.iloc[-2]
                            #newstoploss = min(oc_1st_grp_min, oc_2nd_grp_last_min)

                            newstoploss = hl_min.iloc[-15:-1].min()
                            #if oc_ovlp_grp.iloc[-15:-1].nunique() == 1:
                            #    newstoploss = oc_min.iloc[-15:-1].min()
                            #elif oc_2nd_grp_min < oc_1st_grp_min:
                            #    newstoploss = min(oc_1st_grp_min, oc_2nd_grp_min)
                            #else:
                            #    newstoploss = stoploss
                                
                            #print(oc_ovlp_grp)
                        else:
                            #if hl_ovlp_grp.nunique() == 1:
                            #    newstoploss = df['low'].iloc[-3:].min()
                            #else:
                                #newstoploss = hl_grp_max.iloc[-2]
                            #newstoploss = min(hl_1st_grp_min, hl_2nd_grp_last_min)

                            if hl_ovlp_grp.iloc[-15:-1].nunique() == 1:
                                newstoploss = hl_min.iloc[-15:-1].min()
                            elif hl_2nd_grp_min < hl_1st_grp_min:
                                newstoploss = min(hl_1st_grp_min, hl_2nd_grp_min)
                            else:
                                newstoploss = stoploss





                    newstoploss=max(openprice, stoploss, newstoploss-tick['spread']*info['point'])
                        
                    if self.mt.Set_sl_and_tp_for_position(ticket, newstoploss):
                        print(f'[{__fname__}:INFO] Move SL@{newstoploss} {sid} : {ticket} DONE !')
                    else:
                        print(f'[{__fname__}:INFO] Move SL@{newstoploss} {sid} : {ticket} FAIL !')




                else:
                    if self.mt.Close_position_by_ticket(ticket):
                        print(f'[{__fname__}:INFO] Close position {sid} : {ticket} DONE !')
                    else:
                        print(f'[{__fname__}:INFO] Close position {sid} : {ticket} FAIL !')


    def run(self, now_srv):
        self.now_srv = now_srv
        super().run()



