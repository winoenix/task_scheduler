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

LOOP_INVERVAL = 0.5

LOG_PATH = "./log/{t_date}"
DB_PATH = "./db"        # 临时使用文件作为持久化工具

TASK_INFO = "./cfg/task_info.xls"
# GRP_INFO = "./grp_info.xlsx"

TASK_STATUS_WAITING = 0
TASK_STATUS_COMPLETE = 1
TASK_STATUS_RUNNING = 2
TASK_STATUS_ERROR = -1

MAX_RETRY_CNT = 10
