import logging
from logging.handlers import RotatingFileHandler
import json
import sys

def setup_logging(logfile: str, error_log: str):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # main rotating log
    fh = RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=7, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    # errors log (json lines)
    eh = RotatingFileHandler(error_log, maxBytes=20*1024*1024, backupCount=10, encoding='utf-8')
    eh.setLevel(logging.ERROR)
    # write JSON lines for errors
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            obj = {
                'ts': self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                'level': record.levelname,
                'name': record.name,
                'msg': record.getMessage()
            }
            if record.exc_info:
                obj['exc'] = self.formatException(record.exc_info)
            return json.dumps(obj, ensure_ascii=False)
    eh.setFormatter(JsonFormatter())
    logger.addHandler(eh)
