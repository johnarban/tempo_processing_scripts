
import logging

def setup_logging(debug: bool = False, name = __name__) -> logging.Logger:
    """
    Set up logging configuration.
    """
    logger = logging.getLogger(name)
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)
    
    format = logging.Formatter(f" {name} %(asctime)s - %(levelname)s - %(message)s - %(funcName)s", '%H:%M')
    ch = logging.StreamHandler()
    ch.setFormatter(format)
    # logging.basicConfig(level=level)
    if not logger.hasHandlers():
        logger.addHandler(ch)
    
    return logger

def set_log_level(debug: bool = False) -> None:
    """
    Set the log level for the logger.
    """
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    level = logging.DEBUG if debug else logging.INFO
    for logger in loggers:
        logger.setLevel(level)
        # for handler in logger.handlers:
        #     handler.setLevel(level)