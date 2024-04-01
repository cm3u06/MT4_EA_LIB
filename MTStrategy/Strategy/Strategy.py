import pandas as pd
from MTStrategy.EACommunicator_API import EACommunicator_API
from MTStrategy.utils import *

class Strategy:

    def __init__(self, mt : EACommunicator_API, symbols:list, **kwargs):
        self.__name__ = self.__class__.__name__
        MESSAGE(self.__name__, 'initialzation', MESS_VERBOSITY.INFO)
        if mt:
            self.mt = mt
        else:
            MESSAGE(self.__name__, 'mt is not exists !', MESS_VERBOSITY.ERROR)

        
        self.symbols = symbols
        if self.symbols:
            MESSAGE(self.__name__, f'symbol = {symbols}', MESS_VERBOSITY.INFO)
        else:
            MESSAGE(self.__name__, 'No symbol !', MESS_VERBOSITY.ERROR)

        self.kwargs = kwargs
        if self.kwargs:
            MESSAGE(self.__name__, f'parameters:', MESS_VERBOSITY.INFO)
            for k,v in self.kwargs.items():
                MESSAGE(self.__name__, f'{k:20} = {v.__str__():20}', MESS_VERBOSITY.INFO)
        

    def entry_signal(self):
        return [False] * len(self.symbols)

    def exit_signal(self):
        return [False] * len(self.symbols)

    def open_trade(self, entry_symbols):
        pass

    def close_trade(self, exit_symbols):
        pass

    def run(self, **kwargs):
        MESSAGE(self.__name__, f'phase(run({kwargs})', MESS_VERBOSITY.INFO)
        MESSAGE(self.__name__, f'phase(exit_signal)', MESS_VERBOSITY.INFO)
        exit_symbols = self.exit_signal()
        MESSAGE(self.__name__, f'phase(close_trade)', MESS_VERBOSITY.INFO)
        self.close_trade(exit_symbols)
        MESSAGE(self.__name__, f'phase(entry_signal)', MESS_VERBOSITY.INFO)
        entry_symbols = self.entry_signal()
        MESSAGE(self.__name__, f'phase(open_trade)', MESS_VERBOSITY.INFO)
        self.open_trade(entry_symbols)
        MESSAGE(self.__name__, f'phase(done)', MESS_VERBOSITY.INFO)