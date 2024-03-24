import pandas as pd
from MTStrategy.EACommunicator_API import EACommunicator_API

class Strategy:

    def __init__(self, mt : EACommunicator_API, symbols:list, **kwargs):
        self.__name__ = self.__class__.__name__
        print(f'[{self.__name__}:INFO] initialzation')
        if mt:
            self.mt = mt
        else:
            print(f'[{self.__class__.__name__}:ERROR] mt is not exists !')

        
        self.symbols = symbols
        if self.symbols:
            print(f'[{self.__class__.__name__}:INFO] symbol = {symbols}')
        else:
            print(f'[{self.__class__.__name__}:ERROR] No symbol !')

        self.kwargs = kwargs
        if self.kwargs:
            print(f'[{self.__class__.__name__}:INFO] parameters:')
            for k,v in self.kwargs.items():
                print(f'{k:20} = {v.__str__():20}')
        

    def entry_signal(self):
        return [False] * len(self.symbols)

    def exit_signal(self):
        return [False] * len(self.symbols)

    def open_trade(self, entry_symbols):
        pass

    def close_trade(self, exit_symbols):
        pass

    def run(self, **kwargs):
        print(f'[{self.__name__}:INFO] phase(run({kwargs})')
        print(f'[{self.__name__}:INFO] phase(exit_signal)')
        exit_symbols = self.exit_signal()
        print(f'[{self.__name__}:INFO] phase(close_trade)')
        self.close_trade(exit_symbols)
        print(f'[{self.__name__}:INFO] phase(entry_signal)')
        entry_symbols = self.entry_signal()
        print(f'[{self.__name__}:INFO] phase(open_trade)')
        self.open_trade(entry_symbols)
        print(f'[{self.__name__}:INFO] phase(done)')