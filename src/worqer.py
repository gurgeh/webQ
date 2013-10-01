import time
import logging

import riak

from apscheduler.scheduler import Scheduler

from qlient import Qlient

"""
server web ui

command line args, also commands
kill old process when starting new one

work with config
"""

STAT_BUCKET = 'minion_stat_%s'

class Worqer:
    def __init__(self, name, server, pw):
        self.the_type = self.__class__.__name__
        self.name = name

        self.overlord = Qlient(server, 'commands_%s_%s' % (self.the_type, self.name), name, pw)
        
        self.config = self.get_config()
        self.stats = self.get_stats()
        self.stats['started'] = time.mktime(time.gmtime())

        self.logs = []
        self.thrownlog = 0
        self.upload_time = 0
        self.live = True
        logging.basicConfig(**self.config.get('logging', {}))
        self.log('info', 'I live to serve!')

        self.sched = Scheduler()
        self.sched.start()

        self.sched.add_interval_job(self.upload, seconds=self.config.get('upload_interval', 60))

    def pre_upload(self):
        pass

    def post_upload(self):
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
                getattr(self, com['fun'])(*com['args'], **com['kwargs'])
            else:
                break
        
        self.upload_time += 1
        if self.upload_time % self.config.get('stat_skip', 5) == 0:
            self.put_stats()

        self.post_upload()

    def quit(self):
        self.live = False

    def forcequit(self):
        sys.exit()

    def log(self, level, mess):
        if len(self.logs) > self.config.get('max_log', 100):
            self.thrownlog += 1
            return
        now = time.mktime(time.gmtime())
        self.logs.append((now, level, mess))
        getattr(logging, level)(mess)

    def get_config(self):
        config = self.overlord.dbget('minion_config', '_basic', {})
        config.update(self.overlord.dbget('minion_config', self.the_type, {}))
        config.update(self.overlord.dbget('minion_config', '%s/%s' % (self.the_type, self.name), {}))
        return config

    def incr_stat(self, stat, nr):
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
