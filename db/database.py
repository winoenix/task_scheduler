import pymysql
from cfg.settings import DB_IP, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE
from sqlalchemy import create_engine

conn = pymysql.connect(user=DB_USER, password=DB_PASSWORD, host=DB_IP, port=DB_PORT, database=DB_DATABASE)
db_engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"
                          % (DB_USER, DB_PASSWORD, DB_IP, DB_PORT, DB_DATABASE))

TASK_INFO_DRP = """
DROP TABLE IF EXISTS `task_info`
"""
TASK_INFO_CRE = """
CREATE TABLE `task_info` (
    `tid` CHAR(50) NOT NULL,
    `command` CHAR(255) NOT NULL,
    `log_path` CHAR(255) NOT NULL,
    `pre_tids` VARCHAR(4096) NOT NULL,
    `group` CHAR(20) NOT NULL,
    `retry_cmd` CHAR(255) NULL,
    `comment` VARCHAR(1024) NULL,
    `status` INT NOT NULL,
    `retry_cnt` INT NOT NULL,
    `start_time` DATETIME NULL,
    `complete_time` DATETIME NULL,
    PRIMARY KEY (`tid`)
)
"""

TASK_DEPENDENCE_DRP = """
DROP TABLE IF EXISTS `task_dependence`
"""
TASK_DEPENDENCE_CRE = """
CREATE TABLE `task_dependence` (
    `tid` CHAR(50) NOT NULL,
    `pre_tid` CHAR(50) NOT NULL,
    PRIMARY KEY (`tid`, `pre_tid`)
)
"""

GRP_INFO_DRP = """
DROP TABLE IF EXISTS `group_info`
"""
GRP_INFO_CRE = """
CREATE TABLE `group_info` (
    `group` CHAR(50) NOT NULL,
    `processes_lmt` INT NOT NULL,
    `processes` INT NOT NULL,
    PRIMARY KEY (`group`)
)
"""
def create_task_info_tb():
    exec_sql(TASK_INFO_DRP)
    exec_sql(TASK_INFO_CRE)


def create_task_dependence_tb():
    exec_sql(TASK_DEPENDENCE_DRP)
    exec_sql(TASK_DEPENDENCE_CRE)


def create_group_info_tb():
    exec_sql(GRP_INFO_DRP)
    exec_sql(GRP_INFO_CRE)


def init_tbs():
    create_task_info_tb()
    create_task_dependence_tb()
    create_group_info_tb()


def exec_sql(sql):
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.commit()

if __name__ == "__main__":
    init_tbs()
