import pymysql
from cfg.settings import DB_IP, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE
from sqlalchemy import create_engine
import pandas as pd
import datetime

db_engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"
                          % (DB_USER, DB_PASSWORD, DB_IP, DB_PORT, DB_DATABASE))


def init_tbs(t_date):
    ti_tb_name = "task_info_%s" % t_date.strftime("%Y%m%d")
    task_info_drp = """
    DROP TABLE IF EXISTS `%s`
    """ % ti_tb_name
    task_info_cre = """
    CREATE TABLE `%s` (
        `tid` CHAR(255) NOT NULL,
        `command` VARCHAR(1024) NOT NULL,
        `log_path` CHAR(255) NOT NULL,
        `pre_tids` VARCHAR(4096) NOT NULL,
        `group` CHAR(20) NOT NULL,
        `retry_cmd` CHAR(255) NULL,
        `comment` VARCHAR(1024) NULL,
        `skipped` INT NOT NULL,
        `status` INT NOT NULL,
        `retry_cnt` INT NOT NULL,
        `start_time` DATETIME NULL,
        `complete_time` DATETIME NULL,
        PRIMARY KEY (`tid`)
    )
    """ % ti_tb_name
    exec_sql(task_info_drp)
    exec_sql(task_info_cre)

    td_tb_name = "task_dependence_%s" % t_date.strftime("%Y%m%d")
    task_dependence_drp = """
    DROP TABLE IF EXISTS `%s`
    """ % td_tb_name
    task_dependence_cre = """
    CREATE TABLE `%s` (
        `tid` CHAR(255) NOT NULL,
        `pre_tid` CHAR(255) NOT NULL,
        PRIMARY KEY (`tid`, `pre_tid`)
    )
    """ % td_tb_name
    exec_sql(task_dependence_drp)
    exec_sql(task_dependence_cre)

    gi_tb_name = "group_info_%s" % t_date.strftime("%Y%m%d")
    grp_info_drp = """
    DROP TABLE IF EXISTS `%s`
    """ % gi_tb_name
    grp_info_cre = """
    CREATE TABLE `%s` (
        `group` CHAR(50) NOT NULL,
        `processes_lmt` INT NOT NULL,
        `processes` INT NOT NULL,
        PRIMARY KEY (`group`)
    )
    """ % gi_tb_name
    exec_sql(grp_info_drp)
    exec_sql(grp_info_cre)

    general_info_cre = """
    CREATE TABLE IF NOT EXISTS `general_info` (
        `t_date` DATE NOT NULL,
        `completed` INT NOT NULL,
        `start_time` DATETIME NULL,
        `end_time`   DATETIME NULL,
        `ti_tb_name` CHAR(30) NOT NULL,
        `td_tb_name` CHAR(30) NOT NULL,
        `gi_tb_name` CHAR(30) NOT NULL,
        PRIMARY KEY (`t_date`)
    )
    """
    exec_sql(general_info_cre)

    general_info_df = pd.read_sql_query("select * from general_info where t_date = '%s'" % t_date, db_engine)
    if len(general_info_df) > 0:
        exec_sql("update general_info set completed = 0, start_time = '%s', end_time = null where t_date = '%s'"
                 % (datetime.datetime.now(), t_date))
    else:
        exec_sql("INSERT INTO general_info "
                 + "(t_date, completed, start_time, end_time, ti_tb_name, td_tb_name, gi_tb_name) VALUES "
                 + "('%s', 0, '%s', null, '%s', '%s', '%s')" % (t_date, datetime.datetime.now(),
                                                                ti_tb_name, td_tb_name, gi_tb_name))

        # general_info_df = pd.DataFrame(columns=["t_date", "completed", "start_time", "end_time",
        #                                         "ti_tb_name", "td_tb_name", "gi_tb_name"],
        #                                data=[[t_date, 0, datetime.datetime.now(), None,
        #                                       ti_tb_name, td_tb_name, gi_tb_name]])
        # general_info_df.to_sql("general_info", db_engine, if_exists="append", index=False)
    return ti_tb_name, td_tb_name, gi_tb_name


def exec_sql(sql):
    conn = pymysql.connect(user=DB_USER, password=DB_PASSWORD, host=DB_IP, port=DB_PORT, database=DB_DATABASE)
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # init_tbs(t_date)
    pass
