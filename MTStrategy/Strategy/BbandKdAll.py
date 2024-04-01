
from talib import abstract
from functools import reduce
from tqdm import tqdm
import datetime as dt
from MTStrategy.EACommunicator_API import *
from MTStrategy.utils import *
from .Strategy import *
from .BbandKdBase import *
from .BbandKd2nd import *

class BbandKdAll(Strategy):

    def __init__(self, mt : EACommunicator_API, symbols:list, **kwargs):
        super().__init__(mt, symbols, **kwargs)
        self.strategys = [
            BbandKdBase(mt, symbols),
            BbandKd2nd(mt, symbols)
        ]

    def run(self, **kwargs):
        for stg in self.strategys:
            stg.run(**kwargs)



