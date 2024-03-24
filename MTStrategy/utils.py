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



def true_range(df):
    df['C-H'] = (df['close']-df['high'].shift()).abs()
    df['C-L'] = (df['close']-df['low'].shift()).abs()
    df['H-L'] = (df['high']-df['low'].shift()).abs()
    return df.loc[:,['C-H','C-L','H-L']].agg('max',axis=1)