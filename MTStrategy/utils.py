from functools import reduce
import pandas as pd

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



def true_range(df):
    df['C-H'] = (df['close']-df['high'].shift()).abs()
    df['C-L'] = (df['close']-df['low'].shift()).abs()
    df['H-L'] = (df['high']-df['low'].shift()).abs()
    return df.loc[:,['C-H','C-L','H-L']].agg('max',axis=1)


def find_high_pre_low(df):
    __fname__ = 'find_high_pre_low'

    segment = (df['close']>df['close'].shift()).diff().abs().cumsum()
    segment_high = df.groupby(by=segment)['high'].max()
    segment_low  = df.groupby(by=segment)['low'].min()
    segment_dir  = (df['close']>df['close'].shift()).groupby(by=segment).any()
    segment_df = pd.DataFrame({'dir': segment_dir,
                               'max': segment_high,
                               'min': segment_low})
    MESSAGE(__fname__, f'segment = \n{segment_df}', MESS_VERBOSITY.DEBUG)
    
    if segment_dir.iloc[-1] and len(segment)>=2:
        return segment_low.iloc[-2]
    elif len(segment)>=3:
        return segment_low.iloc[-3]
    else:
        return segment_low.iloc[-1]




# servirity filter
import enum
class MESS_VERBOSITY(enum.IntEnum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    FATAL = 4
    NONE = -1
    

VERBOSITY_LEVEL = MESS_VERBOSITY.DEBUG


# scope filter
DEBUG_ENTRY = {
    'Strategy_Scheduler:EXEC_LOOP':True,
    'BbandKdBase:entry_signal' : False,
    'BbandKdBase:filter' : False,
    'BbandKdBase:exit_signal' : False,
    'BbandKdBase:close_trade' : False,
    'BbandKdBase:open_trade' : True,
    'BbandKd2nd:entry_signal' : True,
    'BbandKd2nd:filter' : True,
    'BbandKd2nd:exit_signal' : False,
    'BbandKd2nd:close_trade' : False,
    'BbandKd2nd:open_trade' : False,
    'find_high_pre_low' : True,
}


    
def MESSAGE(dbg_entry: str, message: str, verbosity: MESS_VERBOSITY):
    if dbg_entry in DEBUG_ENTRY and DEBUG_ENTRY[dbg_entry] and verbosity >= VERBOSITY_LEVEL:
        print(f'[{dbg_entry}:{verbosity.name}] ' + message)
    