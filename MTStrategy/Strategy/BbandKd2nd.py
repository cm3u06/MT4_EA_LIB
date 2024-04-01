
from talib import abstract
from functools import reduce
from tqdm import tqdm
import datetime as dt
from MTStrategy.EACommunicator_API import *
from MTStrategy.utils import *
from .Strategy import Strategy

class BbandKd2nd(Strategy):

    def __init__(self, mt : EACommunicator_API, symbols:list, **kwargs):

        # default parameter
        parms = {
            'entry.nbrofbars':30,
            'entry.rr_ratio':1.0,
            'exit.bb_near_ratio' : 0,
            'open_trade.money':10000,
            'open_trade.lotsize_limit':0.005,
            'open_trade.timeframe':'D',
            'open_trade.drop_vio_lotsize':True,
            'open_trade.nbrofbars':30,
            'exit.timeframe':'D',
            'exit.nbrofbars':30,
            'exit.bb_near_ratio':0,
            'close_trade.timeframe':'D',
            'close_trade.nbrofbars':30,
            'close_trade.SL_mode':0,
            'comment':[self.__class__.__name__]
        }

        kwargs = parms | kwargs # kwargs will overwrite parms

        super().__init__(mt, symbols, **kwargs)


            


    def entry_signal(self):
        __fname__ = f'{self.__name__}:entry_signal'

        signal = {}
        
        #for sid in stocks:
        for sid in tqdm(self.symbols):
            MESSAGE(__fname__, f'start {sid}', MESS_VERBOSITY.DEBUG)
            info = self.mt.Get_instrument_info(sid)
            MESSAGE(__fname__, f'info = {info}', MESS_VERBOSITY.DEBUG)
            tick = self.mt.Get_last_tick_info(sid)
            MESSAGE(__fname__, f'tick = {tick}', MESS_VERBOSITY.DEBUG)
            df = self.mt.Get_ohlcv(instrument=sid, timeframe='D', nbrofbars=self.kwargs['entry.nbrofbars'])
            MESSAGE(__fname__, f'price = \n{df}', MESS_VERBOSITY.DEBUG)
            #df_h4 = self.mt.Get_ohlcv(instrument=sid, timeframe='H4', nbrofbars=self.kwargs['entry.nbrofbars'])
            #MESSAGE(__fname__, f'price_h4 = \n{df_h4}', MESS_VERBOSITY.DEBUG)
            KD = abstract.STOCH(df,fastk_period=9)
            BB = abstract.BBANDS(df, 21, 2.1, 2.1)
            

            conditions = {
                'breakout_middle_sft1' : \
                    (df['close'].shift() > BB['middleband'].shift()) 
                    & (df['open'].shift() <= BB['middleband'].shift())
                    & (df['close'] > BB['middleband'])
                    & (df['open'] > BB['middleband']) ,
                'BB_RiskReward' : ( ((BB['upperband']-tick['ask'])/ (tick['ask']-find_high_pre_low(df)+tick['spread']*info['point']) ) >= self.kwargs['entry.rr_ratio'] ),
            }

            cond_dict = \
            {'&' : ['breakout_middle_sft1','BB_RiskReward' ]}

            
            signal[sid] = recursive_reduce(cond_dict, conditions)
        
        signal_df = pd.DataFrame(signal)
        
        MESSAGE(__fname__, f'signal_df = {signal_df.tail(5)}', MESS_VERBOSITY.DEBUG)
        for k,v in conditions.items():
            if type(v)==pd.DataFrame:
                MESSAGE(__fname__, f'{k}', MESS_VERBOSITY.DEBUG)
                MESSAGE(__fname__, f'{v.iloc[-1]}', MESS_VERBOSITY.DEBUG)

        entry_symbols_0 = list(signal_df.columns[signal_df.iloc[-1]])

        entry_symbols_1 = self.filter(entry_symbols_0)

        return entry_symbols_1


    def filter(self, symbols: list):
        __fname__ = f'{self.__name__}:filter'
        
        func_check_comment = {item : lambda x, item=item: x.startswith(item) for item in self.kwargs['comment'] }
        base_order_names = ['BbandKdBase', 'STOCK_HIT_BBANDS_LB']
        check_base_order_exist = {item : lambda x, item=item: x.startswith(item) for item in base_order_names }

        open_position = self.mt.Get_all_open_positions()
        MESSAGE(__fname__, f'open_position = {open_position}', MESS_VERBOSITY.DEBUG)
        close_position = self.mt.Get_all_closed_positions()
        MESSAGE(__fname__, f'close_position = {close_position}', MESS_VERBOSITY.DEBUG)
        filter_symbols = symbols[:]
        MESSAGE(__fname__, f'symbols = {symbols}', MESS_VERBOSITY.INFO)
        for symbol in symbols:
  
            # if no base order
            if ~((open_position['symbol']==symbol) &
               (open_position['comment'].fillna('').transform(check_base_order_exist).any(axis=1))).any():
                filter_symbols.remove(symbol)
                MESSAGE(__fname__, f"{open_position['comment'].fillna('').transform(check_base_order_exist)}", MESS_VERBOSITY.DEBUG)

            # if 2nd order exist
            elif ((open_position['symbol']==symbol) & 
                (open_position['comment'].fillna('').transform(func_check_comment).any(axis=1))
                ).any():
                filter_symbols.remove(symbol)
                MESSAGE(__fname__, f"{open_position['comment'].fillna('').transform(func_check_comment)}", MESS_VERBOSITY.DEBUG)

            # if 2nd order has exist today ever
            elif self.now_srv is not None:
                today_srv = self.now_srv.strftime('%Y.%m.%d')
                if ((close_position['symbol']==symbol) & 
                    (close_position['comment'].fillna('').transform(func_check_comment).any(axis=1)) &
                    (close_position['opentime']==today_srv)
                    ).any() :
                    filter_symbols.remove(symbol)
                    MESSAGE(__fname__, f"{close_position['comment'].fillna('').transform(func_check_comment)}", MESS_VERBOSITY.DEBUG)

        MESSAGE(__fname__, f'filter_symbols = {filter_symbols}', MESS_VERBOSITY.INFO)
        return filter_symbols


    def open_trade(self, entry_symbols:list):
        __fname__ = f'{self.__name__}:open_trade'

        money_each_order = self.kwargs['open_trade.money'] * self.kwargs['open_trade.lotsize_limit']

        MESSAGE(__fname__, f'entry_symbols = {entry_symbols}', MESS_VERBOSITY.DEBUG)
        
        ticket = {}
        for sid in entry_symbols:
            MESSAGE(__fname__, f'{sid} ==============', MESS_VERBOSITY.INFO)

            df = self.mt.Get_ohlcv(instrument=sid, timeframe=self.kwargs['open_trade.timeframe'], nbrofbars=self.kwargs['open_trade.nbrofbars'])
            MESSAGE(__fname__, f'price = \n{df}', MESS_VERBOSITY.DEBUG)
            info = self.mt.Get_instrument_info(sid)
            MESSAGE(__fname__, f'info = {info}', MESS_VERBOSITY.DEBUG)
            tick = self.mt.Get_last_tick_info(sid)
            MESSAGE(__fname__, f'tick = {tick}', MESS_VERBOSITY.DEBUG)

            ask = tick['ask']
            lowest = find_high_pre_low(df)
            MESSAGE(__fname__, f'ask = {ask}, lowest = {lowest}', MESS_VERBOSITY.DEBUG)

            risk = max(ask - lowest + info['point']*tick['spread'] , info['stop_level']*info['point'], 1)
            stoploss = ask - risk
            #lotsize = max(money_each_order / risk //info['lot_step'] *info['lot_step'] , info['min_lotsize'])
            lotsize = money_each_order / risk //info['lot_step'] *info['lot_step']
            MESSAGE(__fname__, f'lotsize = {lotsize:.2f}, openprice = {ask}, stoploss = {stoploss}\n', MESS_VERBOSITY.INFO)

            if self.kwargs['open_trade.drop_vio_lotsize'] and lotsize < info['min_lotsize']:
                MESSAGE(__fname__, f'lotsize is too small to open trade !', MESS_VERBOSITY.WARNING)
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
            'BB_NearUB' : ((BB['upperband'] - df['high'])/(BB['upperband']-BB['middleband']) <= self.kwargs['exit.bb_near_ratio'] ),
            'Close' : (df['close'] > df['open'] )
            }

            cond_dict = {'&':[
                'BB_NearUB',
            ]}
            
            sell = recursive_reduce(cond_dict, conditions)

            if sell.iloc[-1]:
                exit_symbols.append(sid)

        return exit_symbols
            
    
    def close_trade(self, exit_symbols:list):
        __fname__ = f'{self.__name__}:close_trade'


        open_position = self.mt.Get_all_open_positions()
        open_position['comment'] = open_position['comment'].fillna('')
            
        for id, order in open_position.iterrows():
            ticket = order['ticket']
            sid    = order['symbol']
            openprice = order['openprice']
            stoploss = order['stoploss']
            comment = order['comment'] if order['comment'] else ''
            MESSAGE(__fname__, f'ticket = {ticket}, symbol = {sid}, IN = {openprice}, SL= {stoploss}, comment = {comment}', MESS_VERBOSITY.INFO)

            if comment.startswith(tuple(self.kwargs['comment'])) and \
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

                        if hl_ovlp_grp.iloc[-10:-1].nunique() == 1:
                            newstoploss = hl_min.iloc[-10:-1].min()
                        elif hl_2nd_grp_min < hl_1st_grp_min:
                            newstoploss = min(hl_1st_grp_min, hl_2nd_grp_min)
                        else:
                            newstoploss = stoploss





                    newstoploss=max(openprice, stoploss, newstoploss-tick['spread']*info['point'])
                        
                    if self.mt.Set_sl_and_tp_for_position(ticket, newstoploss):
                        MESSAGE(__fname__, f'Move SL@{newstoploss} {sid} : {ticket} DONE !', MESS_VERBOSITY.INFO)
                    else:
                        MESSAGE(__fname__, f'Move SL@{newstoploss} {sid} : {ticket} FAIL !', MESS_VERBOSITY.INFO)




                else:
                    if self.mt.Close_position_by_ticket(ticket):
                        MESSAGE(__fname__, f'Close position {sid} : {ticket} DONE !', MESS_VERBOSITY.INFO)
                    else:
                        MESSAGE(__fname__, f'Close position {sid} : {ticket} FAIL !', MESS_VERBOSITY.INFO)


    def run(self, **kwargs):
        self.now_srv = kwargs['now_srv']
        self.now_mrk = kwargs['now_mrk']
        super().run()



