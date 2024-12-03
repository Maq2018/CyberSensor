import logging
from config import Config
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('logs')


def configure_logs():
    logging.getLogger().setLevel(Config.LOG_LEVEL)

    if Config.LOG_STDOUT:

        formatter = logging.Formatter(fmt=Config.LOG_FMT, datefmt=Config.LOG_DATE_FMT)
        stream = logging.StreamHandler()

        stream.setFormatter(formatter)
        logging.getLogger().addHandler(stream)

    if Config.LOG_PATH:

        logger.info(f"configuring logging for {Config.LOG_PATH}")

        file = RotatingFileHandler(
            Config.LOG_PATH, maxBytes=1024 * 1024 * 100, backupCount=20)

        formatter = logging.Formatter(fmt=Config.LOG_FMT, datefmt=Config.LOG_DATE_FMT)
        file.setFormatter(formatter)
        logging.getLogger().addHandler(file)

    if Config.LOG_SYSLOG_ENDPOINT:

        logger.info(f"configuring logging for {Config.LOG_SYSLOG_ENDPOINT}")

        ip, port, msg_size = Config.LOG_SYSLOG_ENDPOINT
        syslog = logging.handlers.SysLogHandler(address=(ip, port))

        formatter = logging.Formatter(fmt=Config.LOG_FMT, datefmt=Config.LOG_DATE_FMT)
        syslog.setFormatter(formatter)
        logging.getLogger().addHandler(syslog)
