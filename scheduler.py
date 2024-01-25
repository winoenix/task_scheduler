from cfg.settings import *
import pandas as pd
import os
import sys
import time
import multiprocessing
import datetime
from db.database import init_tbs, exec_sql, db_engine

pd.set_option('display.width', 100)
pd.set_option('display.max_columns', 100)  # 打印最大列数
pd.set_option('display.max_rows', 100)  # 打印最大行数
pd.set_option('display.min_rows', 60)  # 打印最小行数


def init_task_info(t_date, mode=0, tids=None):
    """
    :param t_date:
    :param mode: 0从配置文件开始执行，1从持久化信息恢复，2生成父任务表，3生成子任务表
    :param tids:
    :return:
    """
    if mode == 0:
        ti_tb_name, td_tb_name, gi_tb_name = init_from_cfg(t_date)
    elif mode == 1:
        ti_tb_name, td_tb_name, gi_tb_name = restore_from_db(t_date)
    elif mode == 2:
        ti_tb_name, td_tb_name, gi_tb_name = init_from_cfg(t_date)
        gen_fathers()
    elif mode == 3:
        ti_tb_name, td_tb_name, gi_tb_name = init_from_cfg(t_date)
        gen_sons()
    else:
        raise ValueError("Invalid mode: %s. " % mode)
    return ti_tb_name, td_tb_name, gi_tb_name


def init_from_cfg(t_date):
    """
    获取、解析调度配置文件，返回调度信息表
    :return:
    """
    # 读取调度配置文件
    ti_df = pd.read_excel(TASK_INFO, sheet_name='task_info', dtype={
        "tid": str,
        "command": str,
        "log_path": str,
        "pre_tids": str,
        "group": str,
        "retry_cmd": str,
        "retry_interval": str,
        "comment": str,
        "skipped": str,
    }, na_filter=False)
    for col in ti_df.columns:
        ti_df[col] = ti_df[col].str.strip()
    ti_df.retry_interval = [None if v == "" else int(v) for v in ti_df.retry_interval]
    ti_df.skipped.fillna(0, inplace=True)
    ti_df.skipped = ti_df.skipped.astype(int)

    # 检查调度配置文件
    if len(ti_df.tid.drop_duplicates()) != len(ti_df):
        raise ValueError("Duplicate task ids. ")

    # status存放任务的执行状态
    ti_df["status"] = [TASK_STATUS_WAITING if v == 0 else TASK_STATUS_SKIPPED for v in ti_df.skipped]
    ti_df["retry_cnt"] = 0
    ti_df["start_time"] = None
    ti_df["complete_time"] = None
    print(ti_df)
    # 生成依赖关系表
    df_ls = []
    for i in range(len(ti_df)):
        # 将依赖任务转成列格式
        pre_tids = ti_df["pre_tids"].iloc[i]
        pre_tsk_list = pre_tids.split(",")
        pre_tsk_list = [v.strip() for v in pre_tsk_list]
        tmp = pd.DataFrame({"tid": ti_df["tid"].iloc[i], "pre_tid": pre_tsk_list})
        df_ls.append(tmp)
    td_df = pd.concat(df_ls).reset_index(drop=True)

    # 读取分组配置文件
    grp_df = pd.read_excel(TASK_INFO, sheet_name='grp_info', dtype={
        "group": str,
        "processes_lmt": int
    }, na_filter=False)
    grp_df.group = grp_df.group.str.strip()
    grp_df["processes"] = 0

    tmp = ti_df[~ti_df.group.isin(grp_df.group)]
    if len(tmp) > 0:
        raise ValueError("Task group not in group information: \n%s" % str(tmp))

    ti_tb_name, td_tb_name, gi_tb_name = init_tbs(t_date)
    ti_df.to_sql(ti_tb_name, db_engine, if_exists="append", index=False)
    td_df.to_sql(td_tb_name, db_engine, if_exists="append", index=False)
    grp_df.to_sql(gi_tb_name, db_engine, if_exists="append", index=False)
    # print(pd.read_sql("task_info", db_engine))
    # print(pd.read_sql("task_dependence", db_engine))
    # print(pd.read_sql("group_info", db_engine))
    return ti_tb_name, td_tb_name, gi_tb_name


def restore_from_db(t_date):
    """
    从持久化存储获取任务信息
    :return:
    """
    general_info_df = pd.read_sql_query("select * from general_info where t_date = '%s'" % t_date, db_engine)
    if len(general_info_df) > 0:
        ti_tb_name = general_info_df.ti_tb_name.iloc[0]
        td_tb_name = general_info_df.td_tb_name.iloc[0]
        gi_tb_name = general_info_df.gi_tb_name.iloc[0]
        exec_sql("update `%s` set retry_cnt = 0 " % ti_tb_name)
        exec_sql("update `%s` set `status` = %s where `status` not in (%s, %s)"
                 % (ti_tb_name, TASK_STATUS_WAITING, TASK_STATUS_COMPLETE, TASK_STATUS_SKIPPED))
        exec_sql("update `%s` set processes = 0" % gi_tb_name)
    else:
        raise ValueError("No task status in database in date %s" % t_date)
    return ti_tb_name, td_tb_name, gi_tb_name


def check_dag(td_tb_name):
    """
    检查调度依赖关系，并生成执行的调度表
    :return:
    """
    # 判断依赖任务是否在任务表中或者为空字符串
    td_df = pd.read_sql(td_tb_name, db_engine)
    tids = td_df.tid.drop_duplicates().to_list() + [""]
    if len(td_df[~td_df.pre_tid.isin(tids)]) > 0:
        logger.error("Unknown dependencies. ")
        logger.error(td_df[~td_df.pre_tid.isin(tids)])
        return False
    # 按照依赖关系遍历任务
    rem = td_df.copy()
    while len(rem) != 0:
        pop = rem[~rem.pre_tid.isin(rem.tid)]
        # print(pop)
        rem = rem[rem.pre_tid.isin(rem.tid)]
        # print(rem)
        if len(pop) == 0 and len(rem) != 0:
            logger.error("Circle in the dependencies. ")
            logger.error(rem)
            return False
    return True


def gen_fathers():
    """

    :return:
    """
    return


def gen_sons():
    """

    :return:
    """
    return


def exec_task(cmd="sleep 1", seconds=0):
    """
    执行任务对应的命令
    :param cmd:
    :param seconds:
    :return:
    """
    if seconds != 0:
        logger.info("Wait %d seconds to run: %s. " % (seconds, cmd))
        time.sleep(seconds)
    exec_ret = (os.system(cmd) >> 8)
    return exec_ret


def make_cmd(tid, cmd, t_date, log_path=LOG_PATH, no_log=False, retry_cnt=0):
    """
    制作执行语句
    :param tid:
    :param cmd:
    :param t_date:
    :param log_path:
    :param no_log:
    :param retry_cnt:
    :return:
    """
    t_date_str = t_date.strftime("%Y%m%d")

    if log_path is None or log_path == "":
        log_path = LOG_PATH
    log_path = log_path.replace("{tid}", tid)
    log_path = log_path.replace("{t_date}", t_date_str)

    if not os.path.exists(log_path):
        logger.warning("Log path doesnt exist, mkdir %s" % log_path)
        os.makedirs(log_path)

    cmd = cmd.replace("{tid}", tid)
    cmd = cmd.replace("{t_date}", t_date_str)
    if no_log is False:
        log_file = os.path.join(log_path, "%s_%s.log" % (tid, t_date_str))
        if os.path.exists(log_file):
            if retry_cnt == 1:
                # 第一次重试，备份原始日志
                os.system("cp %s %s" % (log_file, log_file + ".origin"))
            os.system("mv %s %s" % (log_file, log_file + ".bak"))
        err_file = os.path.join(log_path, "%s_%s.err.log" % (tid, t_date_str))
        if os.path.exists(err_file):
            if retry_cnt == 1:
                # 第一次重试，备份原始日志
                os.system("cp %s %s" % (err_file, err_file + ".origin"))
            os.system("mv %s %s" % (err_file, err_file + ".bak"))
        cmd += " > %s 2> %s" % (log_file, err_file)
    return cmd


def main_loop(t_date, mode):
    """
    :param mode:
    :param t_date: pd.Timestamp
    :return:
    """
    # 生成调度任务表
    ti_tb_name, td_tb_name, gi_tb_name = init_task_info(t_date, mode)
    # 检查调度任务表
    if not check_dag(td_tb_name):
        raise ValueError("Task dependency is not a DAG! ")
    # 主循环，循环执行满足执行条件的任务
    # 初始化进程池
    pool_ls = []
    pool = multiprocessing.Pool()
    while 1:
        # 遍历所有任务，根据其状态进行处理
        ti_df = pd.read_sql_query("select * from %s where `status` not in (%s, %s)"
                                  % (ti_tb_name, TASK_STATUS_COMPLETE, TASK_STATUS_SKIPPED), db_engine)
        # print(ti_df)
        for tid in ti_df.tid:
            t_info = ti_df[ti_df.tid == tid]
            status = t_info.status.iloc[0]
            # print(tid, status, status == TASK_STATUS_ERROR, type(status))
            # t_depend = td_df[td_df.tid == tid]
            t_depend = pd.read_sql_query("select td.tid, td.pre_tid, ti.`status` pre_sts from %s td " % td_tb_name
                                         + "left join %s ti on td.`pre_tid` = ti.`tid` " % ti_tb_name
                                         + "where td.`tid` = '%s' " % tid,
                                         db_engine)
            # print(t_depend)
            if status in [TASK_STATUS_WAITING, TASK_STATUS_RETRY] \
                    and (len(t_depend[~t_depend.pre_sts.isin([None, TASK_STATUS_COMPLETE, TASK_STATUS_SKIPPED])]) == 0):
                # 任务满足运行条件: 在等待且依赖任务完成
                group = t_info.group.iloc[0]
                grp_info = pd.read_sql_query("select * from %s where `group` = '%s'" % (gi_tb_name, group), db_engine)
                processes = grp_info.processes.iloc[0]
                processes_lmt = grp_info.processes_lmt.iloc[0]
                # print(group, processes, processes_lmt)
                if processes < processes_lmt:
                    # 判断是否满足并行数条件
                    cmd = make_cmd(tid, t_info.command.iloc[0], t_date, t_info.log_path.iloc[0],
                                   retry_cnt=t_info.retry_cnt.iloc[0])
                    if status == TASK_STATUS_RETRY:
                        # 重试任务
                        if t_info.retry_interval.iloc[0] is None:
                            # 重试间隔未设置，使用默认值
                            seconds = RETRY_INTERVAL
                        else:
                            seconds = t_info.retry_interval.iloc[0]
                    else:
                        # 非重试任务
                        seconds = 0
                    logger.info("Task %s: starting, command: %s" % (tid, cmd))
                    pool_ls.append((
                        tid,
                        pool.apply_async(func=exec_task, args=(cmd, seconds))
                    ))
                    # ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_RUNNING
                    # ti_df.loc[ti_df.tid == tid, "start_time"] = pd.Timestamp(datetime.datetime.now())
                    # grp_df.loc[grp_df.group == group, "processes"] = processes + 1
                    exec_sql("update `%s` set `status` = %s, `start_time` = '%s' where `tid` = '%s'"
                             % (ti_tb_name, TASK_STATUS_RUNNING, datetime.datetime.now(), tid))
                    exec_sql("update `%s` set `processes` = `processes` + 1 where `group` = '%s'"
                             % (gi_tb_name, group))
                else:
                    logger.debug("Task %s: processes reach the limit. " % tid)
                # print(ti_df)

            elif status == TASK_STATUS_ERROR:
                # 出错任务处理
                retry_cnt = t_info["retry_cnt"].iloc[0] + 1
                # ti_df.loc[ti_df.tid == tid, "retry_cnt"] = retry_cnt
                if retry_cnt == MAX_RETRY_CNT:
                    # 重试到达限制报警
                    logger.error("Task %s: retry %d, retries reach the limit. " % (tid, retry_cnt))
                    cmd = make_cmd(tid, t_info["retry_cmd"].iloc[0], t_date, no_log=True)
                    exec_task(cmd)
                # 重置错误标识
                logger.info("Task %s: retry %d, reset status to TASK_STATUS_WAITING. " % (tid, retry_cnt))
                # ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_RETRY
                exec_sql("update `%s` set `retry_cnt` = %s, `status` = %s where `tid` = '%s'"
                         % (ti_tb_name, retry_cnt, TASK_STATUS_RETRY, tid))

        # 遍历进程池中的任务，查询完成情况，将完成的任务踢出进程池
        i = 0
        while i < len(pool_ls):
            tid, p = pool_ls[i]
            try:
                ret = p.get(timeout=0)
            except multiprocessing.context.TimeoutError:
                logger.debug("Task %s: running. " % tid)
                i += 1
            else:
                if ret == 0:
                    # 任务返回0，完成
                    logger.info("Task %s: completed, ret is %s" % (tid, ret))
                    # ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_COMPLETE
                    # ti_df.loc[ti_df.tid == tid, "complete_time"] = pd.Timestamp(datetime.datetime.now())
                    exec_sql("update `%s` set `status` = %s, `complete_time` = '%s' where `tid` = '%s'"
                             % (ti_tb_name, TASK_STATUS_COMPLETE, datetime.datetime.now(), tid))
                    # td_df.loc[td_df.pre_tid == tid, "pre_tid"] = ""
                    # exec_sql("update `task_dependence` set `pre_tid` = '' where `pre_tid` = '%s' "
                    #          % tid)
                else:
                    # 任务返回非0，失败
                    logger.error("Error: task %s return %s" % (tid, ret))
                    # ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_ERROR
                    exec_sql("update `%s` set `status` = %s where `tid` = '%s'"
                             % (ti_tb_name, TASK_STATUS_ERROR, tid))
                # print(ti_df)
                # group = ti_df.loc[ti_df.tid == tid, "group"].iloc[0]
                # grp_df.loc[grp_df.group == group, "processes"] -= 1
                exec_sql("update `%s` gi, `%s` ti set gi.processes = gi.processes - 1 " % (gi_tb_name, ti_tb_name)
                         + "where gi.`group` = ti.`group` and ti.tid = '%s'" % tid)
                pool_ls.pop(i)
                # print(grp_df)

        # print(ti_df)

        # 持久化调度任务表
        # update_db(ti_df, td_df, grp_df)

        # 如果所有任务全部完成，则跳出
        if len(ti_df) == 0:
            logger.info("All tasks completed. ")
            break
        time.sleep(LOOP_INVERVAL)

    pool.close()
    pool.join()

    # 更新整体运行状态
    exec_sql("update general_info set completed = 1, end_time = '%s' where t_date = '%s'"
             % (datetime.datetime.now(), t_date))
    # return 0


if __name__ == "__main__":
    date_ = pd.Timestamp(str(sys.argv[1]))
    if len(sys.argv) > 2:
        mode_ = int(sys.argv[2])
    else:
        mode_ = 0
    main_loop(date_, mode_)
    # exit(0)
