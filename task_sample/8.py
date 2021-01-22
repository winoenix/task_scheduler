import logging
import sys

logger = logging.getLogger('root')

formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

handler_log = logging.StreamHandler(sys.stdout)
handler_log.setLevel(logging.INFO)
handler_log.setFormatter(formatter)
logger.addHandler(handler_log)

handler_err = logging.StreamHandler(sys.stderr)
handler_err.setLevel(logging.ERROR)
handler_err.setFormatter(formatter)
logger.addHandler(handler_err)

logger.setLevel(logging.INFO)

if __name__ == "__main__":
    print("I am 8. ")
    logger.info("I am 8. ")
    logger.error("I am wrong 8")

