import logging

formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger('root')
logger.setLevel(logging.INFO)
logger.addHandler(handler)

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
