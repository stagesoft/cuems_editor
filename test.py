
from CuemsWsServer import CuemsWsServer
import time
import logging




server = CuemsWsServer()
logging.info('start server')
server.start(9092)
time.sleep(20)
logging.info('stoping server')
server.stop()
logging.info('server cleanly stoped')