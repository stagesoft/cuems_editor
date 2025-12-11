class CuemsWsServerError(Exception):
    pass
class FileIntegrityError(CuemsWsServerError):
    pass
class NonExistentItemError(CuemsWsServerError):
    pass
class NotTimeCodeError(CuemsWsServerError):
    pass
class EngineError(CuemsWsServerError):
    pass