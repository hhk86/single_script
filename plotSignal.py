import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts


class TsTickData(object):

    def __enter__(self):
        if ts.Logined() is False:
            print('天软未登陆或客户端未打开，将执行登陆操作')
            self.__tsLogin()
            return self

    def __tsLogin(self):
        ts.ConnectServer("tsl.tinysoft.com.cn", 443)
        dl = ts.LoginServer("fzzqjyb", "fz123456")
        print('天软登陆成功')

    def __exit__(self, *arg):
        ts.Disconnect()
        print('天软连接断开')

    def ticks(self, ticker, date):
        ts_sql = '''
                setsysparam(pn_stock(),'{0}');
                setsysparam(PN_Cycle(),cy_1s());
                t:=toverdata(inttodate({1}));
                update t set ['time']=datetimetostr(['time']) end;
                return t;
        '''.format(ticker, date)
        # tsl_sql =   '''
        #             begT:= StrToDate('{{start_date}}');
        #             endT:= StrToDate('{{end_date}}');
        #             setsysparam(pn_cycle(),cy_1s());
        #             setsysparam(pn_rate(),0);
        #             setsysparam(pn_RateDay(),rd_lastday);
        #             r:= select datetimetostr(["date"]) as "time", ["price"],
        #                 from markettable datekey begT to endT of "{{code}}" end;
        #             return r;'''


        fail, data, _ = ts.RemoteExecute(ts_sql, {})
        df = pd.DataFrame(data)
        df = df[[b"time", b"price"]]
        df.columns = ["time", "price"]
        df["time"] = df["time"].apply(lambda b: b.decode("gbk"))
        df["time"] = df["time"].apply(lambda s: dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"))
        df.sort_values(by="time", inplace=True)
        start_time = dt.datetime.strptime(date + "093000", "%Y%m%d%H%M%S")
        end_time = dt.datetime.strptime(date + "150000", "%Y%m%d%H%M%S")
        df = df[(df["time"] >= start_time) & (df["time"] <= end_time)]
        df["time"] = df["time"].apply(lambda t: str(t.time()))
        df.index = df["time"]
        N = df.shape[0]
        for i in range(N):
            if i < N - 1:
                price = df.iloc[i, 1]
                time1 = dt.datetime.strptime(df.iloc[i, 0], "%H:%M:%S")
                time2 = dt.datetime.strptime(df.iloc[i + 1, 0], "%H:%M:%S")
                t = time1 + dt.timedelta(0, 1)
                while t < time2:
                    df = df.append(pd.DataFrame([[str(t.time()), price], ], columns=["time", "price"]))
                    t += dt.timedelta(0, 1)
        df.sort_values(by="time", inplace=True)
        df = df[(df["time"] <= "11:30:00") | (df["time"] >= "13:00:00")]
        df["time_offset"] = list(range(df.shape[0]))
        # df.to_csv("debug.csv")
        return df

if __name__ == "__main__":
    date = "20191015"
    df = pd.read_excel("历史委托20191015093614.xlsx", encoding="gbk")
    df.sort_values(by="时间", inplace=True)
    df["ticker"] = df["代码/名称"].apply(lambda s: "SH" + s[:6] if s.startswith('6') else "SZ" + s[:6])
    df["name"] = df["代码/名称"].apply(lambda s: s[7:])
    df.columns = ["time", "ticker/name", "direction", "quantity", "price", "status", "ticker", "name"]
    df = df[["time", "ticker", "name", "price", "quantity", "direction", "status"]]
    df["time"] = df["time"].apply(lambda s: str(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").time()))
    ticker_set = set(sorted(df["ticker"].tolist()))
    i = 0
    for ticker in ticker_set:
        if i < 57:
            pass
        else:
            with TsTickData() as obj:
                data = obj.ticks(ticker=ticker, date=date)
            plt.figure(figsize=(20, 10))
            plt.plot(data["time_offset"], data["price"], color="gray", alpha=0.5)
            sub_df = df[df["ticker"] == ticker]
            sub_df["pct"] = round(sub_df["quantity"] / sub_df["quantity"].sum() * 100).astype(int)
            for key, record in sub_df.iterrows():
                if record["direction"] == "买入" and record["status"] == "已成":
                    marker = 'or'
                elif record["direction"] == "卖出" and record["status"] == "已成":
                    marker = 'sg'
                elif record["direction"] == "买入" and record["status"] == "已撤":
                    marker = '*r'
                elif record["direction"] == "卖出" and record["status"] == "已撤":
                    marker = 'xg'
                time_offset = data[data["time"] == record["time"]]["time_offset"].squeeze()
                plt.plot([time_offset,], [record["price"],], marker, markersize=6)
                plt.text(time_offset, record["price"] + 0.01, str(record["pct"]) +'%')
            xticks = list(range(14401))[::1800]
            xticklabels = ["9:30", "10:00", "10:30", "11:00", "11:30/13:00", "13:30", "14:00", "14；30", "15:00"]
            plt.xticks(xticks, xticklabels)
            plt.title(ticker + " | " + date, fontsize=25)
            plt.savefig("pictures/" + ticker + '_' + date + '.png')
            plt.close()
        i += 1
        print("Process " + str(i) + " stocks")