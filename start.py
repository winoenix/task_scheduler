from scheduler import main_loop
import pandas as pd
import datetime


def main():
    s_date = datetime.datetime.now().strftime('%Y%m%d')
    e_date = '21001231'
    mode = 0

    for date in pd.date_range(s_date, e_date):
        main_loop(date, mode)



if __name__ == '__main__':
    main()
