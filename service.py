#!/usr/bin/python3
import os
import subprocess
import sys
import time


PATH_ROOT = os.path.dirname(__file__)


USER = os.path.expanduser('~').split('/')[2]


def get_pid_from_task(task, user=USER):
    '''获取指定任务的pid'''
    task = f'[{task[0]}]{task[1:]}'
    cmd = f"ps -ef | grep {user} | grep '{task}'"
    cmd += " | awk '{print $2}'"
    # print(cmd)
    pid = subprocess.getoutput(cmd).split('\n')[-1]

    return pid


SERVICES = {
    'scheduler': {
        'cmd': 'python3 ./start.py',
        'log': './log/scheduler.log'
    }
}


def start(cmd, log):
    ret = os.system(f'nohup {cmd} >> {log} &')

    return ret


def stop(cmd):
    pid = get_pid_from_task(cmd)
    if pid is None or pid == '':
        ret = 0
    else:
        ret = os.system(f'kill {pid}')

    return ret


def main():
    os.system(f'cd {PATH_ROOT}')
    service = sys.argv[1]
    cmd = sys.argv[2]

    if service not in SERVICES.keys():
        raise Exception(f'argv: {service} is invalid!')

    service_cmd = SERVICES[service]['cmd']
    service_log = SERVICES[service]['log']

    if cmd == 'start':
        ret = start(service_cmd, service_log)
        if ret == 0:
            print(f'{service} start success!')
        else:
            raise Exception(f'{service} start failed!')
    elif cmd == 'stop':
        ret = stop(service_cmd)
        if ret == 0:
            print(f'{service} stop success!')
        else:
            raise Exception(f'{service} stop failed!')
    elif cmd == 'restart':
        try:
            ret = stop(service_cmd)
            if ret != 0:
                raise Exception(f'{service} stop failed!')
            ret = start(service_cmd, service_log)
            if ret != 0:
                raise Exception(f'{service} start failed!')
            print(f'{service} restart success!')
        except:
            print(f'{service} restart failed!')
    else:
        raise Exception(f'cmd: {cmd} is invalid!')


if __name__ == '__main__':
    main()
