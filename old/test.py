
from CuemsWsServer import CuemsWsServer
import time
import logging




server = CuemsWsServer()
logging.info('start server')
server.start(9092)
