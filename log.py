import sys
from logging import getLogger, LoggerAdapter, StreamHandler, Formatter, DEBUG, INFO, ERROR, WARNING
from logging.handlers import SysLogHandler
from functools import wraps

cuemsFormatter = Formatter('[%(asctime)s][%(levelname)s] \tFormitGo (PID: %(process)d)-(%(threadName)-9s)-(%(name)s:%(funcName)s:%(caller)s)> %(message)s')

def main_logger(with_syslog = True, with_stdout = True):
    """
    Create a root logger with a custom formatter.
    """
    logger = getLogger(__name__)
    logger.setLevel(DEBUG)

    if with_stdout:
        sh = StreamHandler(sys.stdout)
        sh.setFormatter(cuemsFormatter)
        logger.addHandler(sh)
    
    if with_syslog:
        syslog_handler = SysLogHandler(
            address = '/dev/log', facility = 'local0'
        )
        syslog_handler.setFormatter(cuemsFormatter)
        logger.addHandler(syslog_handler)

    logger_adapter = LoggerAdapter(logger, {"caller": ''})
    return logger_adapter

class Logger:
    logger = main_logger()

    @staticmethod
    def log(level, message, extra=None):
        Logger.logger.log(level, message, stacklevel = 4, extra = extra)
    
    @staticmethod
    def debug(message, **kwargs):
        Logger.log(DEBUG, message, **kwargs)
    
    @staticmethod
    def info(message, **kwargs):
        Logger.log(INFO, message, **kwargs)

    @staticmethod
    def error(message, **kwargs):
        Logger.log(ERROR, message, **kwargs)

    @staticmethod
    def warning(message, **kwargs):
        Logger.log(WARNING, message, **kwargs)

def logged(func):
    """
    A decorator function to log information about function calls and their results.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """
        The wrapper function that logs function calls and their results.
        """
        d = {"caller": func.__name__}
        Logger.logger = getLogger(func.__module__)
        Logger.info(f"Call recieved", extra = d)
        Logger.debug(f"Using args: {args} and kwargs: {kwargs}", extra = d)
        try:
            result = func(*args, **kwargs)
            Logger.debug(f"Finished with result: {result}", extra = d)
        except Warning as w:
            Logger.warning(f"Warning occurred: {w}", extra = d)
            return result
        except Exception as e:
            Logger.error(f"Error occurred: {e}", extra = d)
            raise
        
        else:
            return result

    return wrapper
