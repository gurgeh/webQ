import time
import logging
import socket

from apscheduler.scheduler import Scheduler

from comline import ComLine
from qlient import Qlient

"""
restart worqer (for example, run the program with a restarter script, which restarts if sys.exit(42))
update worqer

--
server web ui
"""

STAT_BUCKET = 'minion_stat_%s'

class Worqer(ComLine):
    def __init__(self, server, pw, name=None):
        self.the_type = self.__class__.__name__
        if name is None:
            name = socket.gethostname()
        self.name = name
        self.server = server
        self.pw = pw

        self.logs = []
        self.thrownlog = 0
        self.last_sc_upload = time.time()

        self.the_logger = logging.getLogger(self.name)
        self.the_logger.setLevel(logging.INFO)

        self.queues = []
        self.overlord = self.make_lord('commands_%s_%s' % (self.the_type, self.name))

        self.config = {}
        config = self.get_config()
        self.config_update(config)
        self.config = config
        
        self.stats = self.get_stats()
        self.stats['started'] = time.mktime(time.gmtime())

        logconf = self.config.get('logging', {})
        logconf['filename'] = './%s_%s.log' % (self.the_type, self.name)
        logging.basicConfig(**logconf)
        self.the_logger = logging.getLogger(self.name)
        self.the_logger.setLevel(self.config.get('loglevel', logging.INFO))
        self.log('info', 'I live to serve!')
        
        self.sched = Scheduler()
        self.sched.start()

        self.sched.add_interval_job(self.upload, seconds=self.config.get('check_interval', 60))

        ComLine.__init__(self)

    def make_lord(self, queue, keep=True):
        q = Qlient(self.server, queue, self.name, self.pw)
        if keep:
            self.queues.append(q)
        return q
        
    def pre_upload(self):
        pass

    def post_upload(self):
        pass

    def config_update(self, newconfig):
        pass
    
    def upload(self):
        self.pre_upload()
        
        if self.logs:
            self.overlord.log('%s_%s' % (self.the_type, self.name), self.logs, self.thrownlog)
            self.logs = []
            self.thrownlog = 0

        while True:
            com = self.overlord.get(1)
            if com:
                com = com[0]
                getattr(self, com['fun'])(*com.get('args', []), **com.get('kwargs', {}))
                self.overlord.clean(com['_id'])
            else:
                break

        now = time.time()
        if self.config.get('sc_interval', 5) * 60 > now - self.last_sc_upload:
            self.last_sc_upload = now
            self.put_stats()
            self.log('info', 'stats %s' % self.stats)
            config = self.get_config()
            if self.config != config:
                self.log('info', 'New config! %s' % newconfig)
                self.config_update(config)
                self.config = config                

        self.post_upload()

    def quit(self):
        self.alive = False
        for q in self.queues:
            q.alive = False

    def forcequit(self):
        sys.exit()

    def log(self, level, mess):
        if len(self.logs) > self.config.get('max_log', 100):
            self.thrownlog += 1
            return
        now = time.mktime(time.gmtime())
        self.logs.append((now, level, mess))
        getattr(self.the_logger, level)(mess)

    def get_config(self):
        config = self.overlord.dbget('minion_config', '_basic', {})
        config.update(self.overlord.dbget('minion_config', self.the_type, {}))
        config.update(self.overlord.dbget('minion_config', '%s/%s' % (self.the_type, self.name), {}))
        return config

    def incr_stat(self, stat, nr=1):
        if stat not in self.stats:
            self.stats[stat] = nr
        else:
            self.stats[stat] += nr

    def set_stat(self, stat, val):
        self.stats[stat] = val

    def get_stats(self):
        return self.overlord.dbget(STAT_BUCKET % self.the_type, 'latest_%s' % self.name, {})

    def put_stats(self):
        statbucket = STAT_BUCKET % self.the_type
        key = time.strftime('%y%m%d %H:%M:%S', time.gmtime())

        self.stats['_id'] = 'latest_%s' % self.name
        self.overlord.dbput(statbucket, self.stats)
        self.stats['_id'] = key
        self.overlord.dbput(statbucket, self.stats)
