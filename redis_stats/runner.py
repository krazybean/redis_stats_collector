import parser
import logging
import time
import config
from logging.handlers import RotatingFileHandler

# Logging section
LOG_LEVEL = config.LOG_LEVEL
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
file_handler = RotatingFileHandler(config.LOG_FILE,
                                   maxBytes=config.LOG_MAX_SIZE,
                                   backupCount=config.LOG_RETENTION)
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

logger = logging.getLogger('redisstats.runner')
logger.setLevel(LOG_LEVEL)

LOG_LEVEL = logging.INFO
INTERVAL = 60

while True:
    try:
        p = parser.RedisParser()
        result = p.main_parse()
        print "marker ----"
        time.sleep(INTERVAL)
    except Exception as e:
        logger.error("Outer Error: {0}".format(e))
