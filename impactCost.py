import pandas as pd
import datetime as dt
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
from jinja2 import Template
from dateutil.parser import parse as dateparse

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

    def ticks(self, code, start_date, end_date):
        ts_template = Template('''setsysparam(pn_stock(),'SH510500');
                                  begT:= StrToDate('{{start_date}}');
                                  endT:= StrToDate('{{end_date}}');
                                  setsysparam(pn_cycle(),cy_1s());
                                  setsysparam(pn_rate(),0);
                                  setsysparam(pn_RateDay(),rd_lastday);
                                  r:= select ["StockID"] as 'ticker', datetimetostr(["date"]) as "time", ["price"],
                                             ["buy1"], ["sale1"]
                                      from markettable datekey begT to endT of "{{code}}" end;
                                  return r;''')
        ts_sql = ts_template.render(start_date=dateparse(start_date).strftime('%Y-%m-%d'),
                                    end_date=dateparse(end_date).strftime('%Y-%m-%d'),
                                    code=code)
        fail, data, _ = ts.RemoteExecute(ts_sql, {})

        def gbk_decode(strlike):
            if isinstance(strlike, (str, bytes)):
                strlike = strlike.decode('gbk')
            return strlike

        def bytes_to_unicode(record):
            return dict(map(lambda s: (gbk_decode(s[0]), gbk_decode(s[1])), record.items()))

        if not fail:
            unicode_data = list(map(bytes_to_unicode, data))
            return pd.DataFrame(unicode_data).set_index(['time', 'ticker'])
        else:
            raise Exception("Error when execute tsl")


if __name__ == "__main__":
    df = pd.read_excel("退补价格测算2.xlsx", sheet_name="305-1025", encoding="gbk")
    col_names = list(df.columns)
    df["ticker"] = df["stkId"].apply(lambda k: "SH" + str(k) if str(k).startswith('6') else "SZ" + str(k))
    df["time"] = df["knockTime"].apply(lambda t: dt.datetime.strftime(t, "%H:%M:%S"))
    ticker_set = set(df["ticker"].tolist())
    final_df = pd.DataFrame()
    pd.set_option("display.max_columns", None)
    i = 0
    for ticker in ticker_set:
        one_stock_df = df[df["ticker"] == ticker]
        with TsTickData() as obj:
            data = obj.ticks(code=ticker, start_date="20191025", end_date="20191026")
        data["index"] = data.index
        data["time"] = data["index"].apply(lambda tu: tu[0][-8:])
        data["time"] = data["time"].apply(lambda s:s[-8: ])
        for key, record in one_stock_df.iterrows():
            row_df = data[data["time"]== record["time"]]
            price = row_df["price"].tolist()[0]
            buy1 = row_df["buy1"].tolist()[0]
            record["最新价"] = price
            record["买一价"] = buy1
            final_df = final_df.append(record)
        i += 1
        print(ticker, i)
    final_df["最新价冲击成本"] = final_df["knockprice"].div(final_df["最新价"]) - 1
    final_df["买一价冲击成本"] = final_df["knockprice"].div(final_df["买一价"]) - 1
    final_df.sort_values(by=["stkId", "knockTime"], inplace=True)
    final_df = final_df[col_names + ["最新价", "买一价", "最新价冲击成本","买一价冲击成本"]]
    print(final_df)
    final_df.to_excel("退补价格测算_305-1025.xlsx", encoding="gbk", index=None)
