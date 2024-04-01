
import datetime as dt
import pytz
from dateutil.tz import tzlocal
from threading import Thread, Event
import time
from MTStrategy import EACommunicator_API
from MTStrategy.Strategy import Strategy
from MTStrategy.utils import *


def srv_tz_dst_compensate(src_dt):
    if src_dt.dst()==dt.timedelta(hours=0):
        return src_dt
    else:
        return src_dt-src_dt.dst()

class Strategy_Scheduler:

    def __init__(self, \
                 strategy : Strategy,
                 mt : EACommunicator_API = None,
                 trade_time_list: list = [dt.time(hour=9,minute=30,second=0),dt.time(hour=16,minute=0,second=0)],
                 tz_srv : dt.timezone = dt.timezone(dt.timedelta(hours=+2)),
                 tz_mrk : dt.timezone = pytz.timezone('America/New_York'),
                 idle_second : int = 60*30,
                ):
        self.strategy        = strategy
        self.trade_time_list = trade_time_list
        self.tz_srv          = tz_srv
        self.tz_mrk          = tz_mrk
        self.idle_second     = idle_second
        self.trade_interval  = [0] * (len(trade_time_list)-1)
        self.ev = Event()
        self.mt = mt

    def start(self, idle_second=None, end_date_mrk=None):
        test_mode     = self.mt.CheckTestMode(with_break=True)
        if idle_second is not None:
            self.idle_second = idle_second

        def EXEC_LOOP(ev: Event):
            __fname__ = self.__class__.__name__+':EXEC_LOOP'
            exec_times    = 0
            while not ev.is_set():
                now_srv        = dt.datetime.fromtimestamp(timestamp=self.mt.GetServerTime(),tz=pytz.utc).replace(tzinfo=self.tz_srv)
                now_mrk        = srv_tz_dst_compensate(now_srv.astimezone(self.tz_mrk))
                now_lca        = now_mrk.astimezone(tzlocal())
                trade_dt_list = [now_mrk.replace(hour=t.hour, minute=t.minute, second=t.second) for t in self.trade_time_list]
                for i in range(len(trade_dt_list)-1):
                    if  (now_mrk.weekday() < 5) \
                        and (now_mrk >= trade_dt_list[i]) \
                        and (now_mrk <  trade_dt_list[i+1]) :
                        if not self.trade_interval[i]:
                            exec_times=exec_times+1
                            MESSAGE(__fname__, f"\nStrategy[{self.strategy.__class__.__name__}] Scheduled" , MESS_VERBOSITY.INFO)
                            MESSAGE(__fname__, f"@LocalTime ={now_lca.isoformat(timespec='seconds')}", MESS_VERBOSITY.INFO)
                            MESSAGE(__fname__, f"@ServerTime={now_srv.isoformat(timespec='seconds')}", MESS_VERBOSITY.INFO)
                            MESSAGE(__fname__, f"@MarketTime={now_mrk.isoformat(timespec='seconds')}", MESS_VERBOSITY.INFO)
                            MESSAGE(__fname__, f"exec_times = {exec_times}", MESS_VERBOSITY.INFO)
                            kwargs = {
                                'now_srv': now_srv,
                                'now_mrk': now_mrk,
                                'now_lca': now_lca,
                                }
                            self.strategy.run(**kwargs)
                            self.trade_interval[i] = True
                    else:
                        self.trade_interval[i] = False
    
                MESSAGE(__fname__, f"Tick Finished @ServerTime {now_srv.isoformat(timespec='seconds')}", MESS_VERBOSITY.NONE)
                self.mt.Break()

                # early stop; used for MT4 Testing ending
                if end_date_mrk != None and \
                   now_mrk >= end_date_mrk.replace(tzinfo=self.tz_mrk):
                    MESSAGE(__fname__, f"End of session !", MESS_VERBOSITY.NONE)
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
