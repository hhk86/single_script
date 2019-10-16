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
        fail, data, _ = ts.RemoteExecute(ts_sql, {})
        df = pd.DataFrame(data)
        df = df[[b"time", b"price"]]
        df.columns = ["time", "price"]
        df["time"] = df["time"].apply(lambda b: b.decode("gbk"))
        df["time"] = df["time"].apply(lambda s: dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"))
        df.sort_values(by="time", inplace=True)
        start_time = dt.datetime.strptime(date + "093000", "%Y%m%d%H%M%S")
        end_time = dt.datetime.strptime(date + "113000", "%Y%m%d%H%M%S")
        df = df[(df["time"] >= start_time) & (df["time"] <= end_time)]
        df["time"] = df["time"].apply(lambda t: t.time())
        # df.index = pd.to_datetime(df["time"])
        # df.resample("1S")
        # print(df)
        return df

if __name__ == "__main__":
    date = "20191015"
    df = pd.read_excel("历史委托20191015093614.xlsx", encoding="gbk")
    df.sort_values(by="时间", inplace=True)
    df["ticker"] = df["代码/名称"].apply(lambda s: "SH" + s[:6] if s.startswith('6') else "SZ" + s[:6])
    df["name"] = df["代码/名称"].apply(lambda s: s[7:])
    df.columns = ["time", "ticker/name", "direction", "quantity", "price", "status", "ticker", "name"]
    df = df[["time", "ticker", "name", "price", "quantity", "direction", "status"]]
    df["time"] = df["time"].apply(lambda s: dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").time())
    ticker_set = set(sorted(df["ticker"].tolist()))
    print(len(ticker_set))
    for ticker in ticker_set:
        with TsTickData() as obj:
            data = obj.ticks(ticker=ticker, date=date)
        plt.figure(figsize=(30, 15))
        plt.plot(data["time"], data["price"], color="gray", alpha=0.5)
        sub_df = df[df["ticker"] == ticker]
        sub_df["pct"] = round(sub_df["quantity"] / sub_df["quantity"].sum() * 100).astype(int)
        for key, record in sub_df.iterrows():
            # print(str(record["time"]))
            # print(dt.datetime.strftime(record["time"], "%H:%M:%S"))
            if str(record["time"])> "11:30:00":
                continue
            if record["direction"] == "买入" and record["status"] == "已成":
                marker = 'or'
            elif record["direction"] == "卖出" and record["status"] == "已成":
                marker = 'sg'
            elif record["direction"] == "买入" and record["status"] == "已撤":
                marker = '*r'
            elif record["direction"] == "卖出" and record["status"] == "已撤":
                marker = 'xg'
            plt.plot([record["time"],], [record["price"],], marker, markersize=10)
            plt.text(record["time"], record["price"] + 0.01, str(record["pct"]) +'%')
        # plt.xlim(df["time"].tolist()[0], df["time"].tolist()[-1])
        # print(sub_df["time"].tolist())
        # print(sub_df["price"].tolist())
        # plt.plot(sub_df["time"], sub_df["price"], "*")
        plt.title(ticker + " | " + date, fontsize=25)
        plt.savefig("pictures_only_morning/" + ticker + '_' + date + '.png')
        plt.close()