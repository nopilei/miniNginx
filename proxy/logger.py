import logging
import logging.config
import sys

from context import client_addr_var


class LoggingFilter(logging.Filter):
    def filter(self, record):
        record.client_addr = client_addr_var.get()
        return True


def setup_logging(level=logging.INFO):
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,

        "formatters": {
            "default": {
                "format": (
                    "[%(asctime)s "
                    "%(levelname)s "
                    "PID %(process)d "
                    "%(name)s "
                    "{client:%(client_addr)s}] "
                    "%(message)s "
                ),
            },
        },

        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "default",
                "filters": ["context"],
            },
        },

        "filters": {
            "context": {
                "()": LoggingFilter,
            }
        },

        "root": {
            "level": level,
            "handlers": ["console"],
        },
    })
