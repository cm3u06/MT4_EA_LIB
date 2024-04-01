from MTStrategy.EACommunicator_API import EACommunicator_API
from MTStrategy.Strategy.BbandKdAll import BbandKdAll
from MTStrategy.Strategy.BbandKdBase import BbandKdBase
from MTStrategy.schedule import Strategy_Scheduler
import datetime as dt
import sys
import re


port = 5555
for i,arg in enumerate(sys.argv):
    print(f'argv[{i}]={arg}')
if len(sys.argv) >= 2:
    re_match_port = re.match(sys.argv[1],'--port=(\d+)')
    if re_match_port:
        port = re_match_port.group(1)
print(f'port={port}')

mt = EACommunicator_API()
mt.Connect(server='localhost',port=port)
mt.Get_instruments(False)

EU_shares = ['#ADS', '#AIR', '#AIRP', '#ALV', '#AXAF', '#BAS', '#BAYN', '#BEI',
             '#BMW', '#BNPP', '#BOUY', '#CBK', '#DAI', '#DANO', '#DB1', '#DBK', '#DPW', '#DTE',
             '#EON', '#FME', '#HRMS', '#IFX', '#LHA', '#LIN', '#LVMH', '#MICP', '#MUV2', '#OREP',
             '#RWE', '#SAP', '#SIE', '#TTEF', '#VOW']
Symbols=[]
for sid in mt.Symbols:
    if '#' in sid and '#ETF' not in sid and '#HK' not in sid and sid not in EU_shares:
        tick = mt.Get_last_tick_info(sid)
        #if tick['ask'] <= 100 and tick['ask'] > 50:
        Symbols.append(sid)



strategy = BbandKdBase(mt, Symbols)

scheduler = Strategy_Scheduler(strategy=strategy,
                               mt = mt,
                               trade_time_list= [
                                   dt.time(hour=9,minute=30,second=0),
                                   dt.time(hour=10,minute=30,second=0),
                                   dt.time(hour=11,minute=30,second=0),
                                   dt.time(hour=12,minute=30,second=0),
                                   dt.time(hour=13,minute=30,second=0),
                                   dt.time(hour=14,minute=30,second=0),
                                   dt.time(hour=15,minute=30,second=0),
                                   dt.time(hour=16,minute=0,second=0)],
                               idle_second = 0
                              )



import pytz
scheduler.start(end_date_mrk=dt.datetime.now().astimezone(pytz.timezone('America/New_York')).replace(hour=15,minute=50,second=0))