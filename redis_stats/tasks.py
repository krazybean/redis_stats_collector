import config
import pymongo
from celery import Celery, Task
from kombu import Queue, Exchange

app = Celery('tasks', broker=config.RABBITMQ_BROKER)
app.conf.CELERY_DEFAULT_QUEUE = config.STATS_QUEUE
app.conf.CELERY_QUEUES = (
    Queue(name=config.STATS_QUEUE, exchange=Exchange(config.STATS_QUEUE), delivery_mode=1, durable=False),
)


class StatTask(Task):
    abstract = True
    _stats_db_connection = None

    @property
    def stats_db_connection(self):
        """Returns an authenticated connection to the stats database"""
        if not self._stats_db_connection:
            #stats_connection = pymongo.MongoClient(config.DB_HOSTS)
            stats_db_connection = stats_connection[config.DB_NAME]
            stats_db_connection.authenticate(config.DB_USER, config.DB_PASS)
            self._stats_db_connection = stats_db_connection

        return self._stats_db_connection


class MinuteStatTask(StatTask):
    abstract = True
    _minute_collection = None

    @property
    def minute_collection(self):
        """Returns an authenticated connection to the minute statistics collection"""
        if not self._minute_collection:
            self._minute_collection = self.stats_db_connection['minute']

        return self._minute_collection

@app.task(base=MinuteStatTask)
def add_minute_stat(minute_document):
    add_minute_stat.minute_collection.insert(minute_document)


class MetricNamesStatTask(StatTask):
    abstract = True
    _metric_names_collection = None

    @property
    def metric_names_collection(self):
        """Returns an authenticated connection to the minute statistics collection"""
        if not self._metric_names_collection:
            self._metric_names_collection = self.stats_db_connection['metric_names']

        return self._metric_names_collection

@app.task(base=MetricNamesStatTask)
def update_metric_names(update_predicate, update_action):
    update_metric_names.metric_names_collection.update(update_predicate, update_action, upsert=True, multi=True)


class HourStatTask(StatTask):
    abstract = True
    _hour_collection = None

    @property
    def hour_collection(self):
        """Returns an authenticated connection to the minute statistics collection"""
        if not self._hour_collection:
            self._hour_collection = self.stats_db_connection['hour']

        return self._hour_collection

@app.task(base=HourStatTask)
def update_hour_stat(update_predicate, update_action):
    update_hour_stat.hour_collection.update(update_predicate, update_action, upsert=True, multi=True)


class DayStatTask(StatTask):
    abstract = True
    _day_collection = None

    @property
    def day_collection(self):
        """Returns an authenticated connection to the minute statistics collection"""
        if not self._day_collection:
            self._day_collection = self.stats_db_connection['day']

        return self._day_collection

@app.task(base=DayStatTask)
def update_day_stat(update_predicate, update_action):
    update_day_stat.day_collection.update(update_predicate, update_action, upsert=True, multi=True)
