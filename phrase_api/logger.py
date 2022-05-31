"""Logger definitions."""
import logging
import os

LOGGER_LEVEL = {
    None: logging.NOTSET,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

logging.root.setLevel(LOGGER_LEVEL[os.getenv("LOG_LEVEL")])


class LoggerSetup:
    """Class serving several loggers"""

    def __init__(self, name, level="debug") -> None:
        self.level = level
        self.name = name

    def get_minimal(self):
        """Simple logger"""
        # Creating handler
        handler = logging.StreamHandler()

        # Create formatters and add it to handlers
        s_format = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(s_format)
        handler.setLevel(LOGGER_LEVEL[self.level])

        return self.create_logger(name=self.name, handler=handler)

    def get_detailed(self):
        """Getting detailed logger.
        Usually used for debug & error levels."""
        # Create handler
        handler = logging.StreamHandler()

        # Create formatters and add it to handlers
        s_format = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(filename)s - %(lineno)d - %(message)s")
        handler.setFormatter(s_format)
        handler.setLevel(LOGGER_LEVEL[self.level])

        return self.create_logger(name=self.name, handler=handler)

    def file_log(self, file_path):
        """Logging into a file"""
        # Create handler
        f_handler = logging.FileHandler(file_path)

        # Create formatters and add it to handlers
        f_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        f_handler.setLevel(LOGGER_LEVEL[self.level])

        return self.create_logger(name=self.name, handler=f_handler)

    def create_logger(self, name, handler):
        """Creating logger from name and created handler"""
        logger = logging.getLogger(name)
        logger.addHandler(handler)
        return logger


def get_logger(name):
    """Getting logger."""
    # Configuring Logger
    logging.root.setLevel(LOGGER_LEVEL[os.getenv("LOG_LEVEL")])
    logger = logging.getLogger(name)

    # Create handler
    s_handler = logging.StreamHandler()
    s_handler.setLevel(logging.DEBUG)

    # Create formatters and add it to handlers
    s_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    s_handler.setFormatter(s_format)

    logger.addHandler(s_handler)

    return logger
