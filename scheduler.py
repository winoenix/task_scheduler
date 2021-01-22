from cfg.settings import *
import pandas as pd
import os
import sys
import time
import multiprocessing
import datetime

pd.set_option('display.width', 100)
pd.set_option('display.max_columns', 100)  # 打印最大列数
pd.set_option('display.max_rows', 100)  # 打印最大行数
pd.set_option('display.min_rows', 60)  # 打印最小行数


def get_task_info(mode=0, tids=None):
    """
    :param mode: 0从配置文件开始执行，1从持久化信息恢复，2生成父任务表，3生成子任务表
    :param tids:
    :return:
    """
    if mode == 0:
        return get_task_info_from_cfg()
    elif mode == 1:
        return get_task_info_from_db()
    elif mode == 2:
        t_df = get_task_info_from_cfg()
        t_df = gen_fathers(t_df, tids)
        return t_df
    elif mode == 3:
        t_df = get_task_info_from_cfg()
        t_df = gen_sons(t_df, tids)
        return t_df
    else:
        raise ValueError("Invalid mode: %s. " % mode)


def get_task_info_from_cfg():
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
    }, na_filter=False)
    for col in ti_df.columns:
        ti_df[col] = ti_df[col].str.strip()

    # 检查调度配置文件
    if len(ti_df.tid.drop_duplicates()) != len(ti_df):
        raise ValueError("Duplicate task ids. ")

    # status存放任务的执行状态
    ti_df["status"] = TASK_STATUS_WAITING
    ti_df["retry_cnt"] = 0
    ti_df["start_time"] = None
    ti_df["complete_time"] = None

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
    return ti_df, td_df, grp_df


def check_dag(td_df):
    """
    检查调度依赖关系，并生成执行的调度表
    :param td_df:
    :return:
    """
    # 判断依赖任务是否在任务表中或者为空字符串
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


def update_db(ti_df, td_df, grp_df):
    """
    对任务状态做持久化
    :param ti_df:
    :param td_df:
    :param grp_df:
    :return:
    """
    db_file = os.path.join(DB_PATH, "task_sts.xls")
    with pd.ExcelWriter(db_file) as writer:
        ti_df.to_excel(writer, sheet_name='ti_df', index=False)
        td_df.to_excel(writer, sheet_name='td_df', index=False)
        grp_df.to_excel(writer, sheet_name='grp_df', index=False)
    return


def get_task_info_from_db():
    """
    从持久化存储获取任务信息
    :return:
    """
    db_file = os.path.join(DB_PATH, "task_sts.xls")
    if os.path.exists(db_file):
        bak_file = os.path.join(DB_PATH,
                                "task_sts_%s.xls" % datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        logger.info("DB file exists, backup it to %s. " % bak_file)
        os.system("cp %s %s" % (db_file, bak_file))
    # 查询任务执行状态
    ti_df = pd.read_excel(db_file, sheet_name='ti_df',
                          dtype={
                              "tid": str,
                              "command": str,
                              "log_path": str,
                              "pre_tids": str,
                              "group": str,
                              "retry_cmd": str,
                              'status': int,
                              'retry_cnt': int},
                          na_filter=False)
    ti_df.loc[ti_df.status != TASK_STATUS_COMPLETE, "status"] = TASK_STATUS_WAITING
    ti_df['retry_cnt'] = 0

    td_df = pd.read_excel(db_file, sheet_name='td_df',
                          dtype=str, na_filter=False)
    grp_df = pd.read_excel(db_file, sheet_name='grp_df',
                           dtype={
                               "group": str,
                               "processes_lmt": int},
                           na_filter=False)
    grp_df["processes"] = 0
    return ti_df, td_df, grp_df


def gen_fathers(df, tids):
    """

    :param df:
    :param tids:
    :return:
    """
    return df


def gen_sons(df, tids):
    """

    :param df:
    :param tids:
    :return:
    """
    return df


def exec_task(cmd="sleep 1"):
    """
    执行任务对应的命令
    :param cmd:
    :return:
    """
    exec_ret = (os.system(cmd) >> 8)
    return exec_ret


def make_cmd(tid, cmd, t_date, log_path=LOG_PATH, no_log=False):
    """
    制作执行语句
    :param tid:
    :param cmd:
    :param t_date:
    :param log_path:
    :param no_log:
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
            os.system("mv %s %s" % (log_file, log_file + ".bak"))
        err_file = os.path.join(log_path, "%s_%s.err.log" % (tid, t_date_str))
        if os.path.exists(err_file):
            os.system("mv %s %s" % (err_file, err_file + ".bak"))
        cmd += " > %s 2> %s" % (log_file, err_file)
    return cmd


def main_loop(t_date, mode):
    """
    :param mode:
    :param t_date:
    :return:
    """
    # 生成调度任务表
    ti_df, td_df, grp_df = get_task_info(mode)
    # ti_df: 任务信息表
    # td_df: 任务依赖表
    # grp_df: 分组信息表

    # 检查调度任务表
    if not check_dag(td_df):
        raise ValueError("Task dependency is not a DAG! ")
    # print(ti_df, "\n", td_df, "\n", grp_df, "\n", )

    # 主循环，循环执行满足执行条件的任务
    # 初始化进程池
    pool_ls = []
    pool = multiprocessing.Pool()
    while 1:
        # 遍历所有任务，根据其状态进行处理
        for tid in ti_df.tid:
            t_info = ti_df[ti_df.tid == tid]
            status = t_info.status.iloc[0]
            # print(tid, status, status == TASK_STATUS_ERROR, type(status))
            t_depend = td_df[td_df.tid == tid]
            if (status == TASK_STATUS_WAITING) and (len(t_depend) == len(t_depend[t_depend.pre_tid == ""])):
                # 任务满足运行条件: 在等待且依赖任务完成
                group = t_info.group.iloc[0]
                processes = grp_df[grp_df.group == group].processes.iloc[0]
                processes_lmt = grp_df[grp_df.group == group].processes_lmt.iloc[0]
                if processes < processes_lmt:
                    # 判断是否满足并行数条件
                    cmd = make_cmd(tid, t_info.command.iloc[0], t_date, t_info.log_path.iloc[0])
                    logger.info("Task %s: starting, command: %s" % (tid, cmd))
                    pool_ls.append((
                        tid,
                        pool.apply_async(func=exec_task, args=(cmd,))
                    ))
                    ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_RUNNING
                    ti_df.loc[ti_df.tid == tid, "start_time"] = pd.Timestamp(datetime.datetime.now())
                    grp_df.loc[grp_df.group == group, "processes"] = processes + 1
                else:
                    logger.debug("Task %s: processes reach the limit. " % tid)
                # print(ti_df)

            elif status == TASK_STATUS_ERROR:
                # 出错任务处理
                retry_cnt = t_info["retry_cnt"].iloc[0] + 1
                ti_df.loc[ti_df.tid == tid, "retry_cnt"] = retry_cnt
                if retry_cnt == MAX_RETRY_CNT:
                    # 重试到达限制报警
                    logger.error("Task %s: retries reach the limit. " % tid)
                    cmd = make_cmd(tid, t_info["retry_cmd"].iloc[0], t_date, no_log=True)
                    exec_task(cmd)
                # 重置错误标识
                logger.info("Task %s: reset status to TASK_STATUS_WAITING. " % tid)
                ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_WAITING

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
                    ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_COMPLETE
                    ti_df.loc[ti_df.tid == tid, "complete_time"] = pd.Timestamp(datetime.datetime.now())
                    td_df.loc[td_df.pre_tid == tid, "pre_tid"] = ""
                else:
                    # 任务返回非0，失败
                    logger.error("Error: task %s return %s" % (tid, ret))
                    ti_df.loc[ti_df.tid == tid, "status"] = TASK_STATUS_ERROR
                # print(ti_df)
                group = ti_df.loc[ti_df.tid == tid, "group"].iloc[0]
                grp_df.loc[grp_df.group == group, "processes"] -= 1
                pool_ls.pop(i)
                # print(grp_df)

        # print(ti_df)

        # 持久化调度任务表
        update_db(ti_df, td_df, grp_df)

        # 如果所有任务全部完成，则跳出
        if len(ti_df[ti_df.status != TASK_STATUS_COMPLETE]) == 0:
            logger.info("All tasks completed. ")
            break
        time.sleep(LOOP_INVERVAL)

    pool.close()
    pool.join()
    # return 0


if __name__ == "__main__":
    date_ = pd.Timestamp(str(sys.argv[1]))
    if len(sys.argv) > 2:
        mode_ = int(sys.argv[2])
    else:
        mode_ = 0
    main_loop(date_, mode_)
    # exit(0)
