#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import glob
import time
import pandas as pd
import talib

from HuobiDMService import HuobiDM
from pprint import pprint
import logging
import csv
import os


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
import logging,sys

filelog = True
path = r'../log.txt'

logger = logging.getLogger('log')
logger.setLevel(logging.DEBUG)

# 调用模块时,如果错误引用，比如多次调用，每次会添加Handler，造成重复日志，这边每次都移除掉所有的handler，后面在重新添加，可以解决这类问题
while logger.hasHandlers():
    for i in logger.handlers:
        logger.removeHandler(i)

# file log
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
if filelog:
    fh = logging.FileHandler(path,encoding='utf-8')
    logger.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

# console log
formatter = logging.Formatter('%(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
#logging.basicConfig(filename='my%s.log' % time.time(), level=logging.DEBUG,format=LOG_FORMAT, datefmt=DATE_FORMAT)


# 从配置文件中获取初始化参数
__iniFilePath = glob.glob("../config.ini")
print(__iniFilePath)
cfg = configparser.ConfigParser()
cfg.read(__iniFilePath, encoding='utf-8')
accessKey = cfg.get('ws', 'accessKey')  # 账号
secretKey = cfg.get('ws', 'secretKey')  # 密码
_type = cfg.get('ws', 'type')  # 合约或？
size = cfg.get('ws', 'size')  # 长度
period = cfg.get('ws', 'period')  # 时间长度
symbol = cfg.get('ws', 'symbol')  # 合约种类：BTC等
symbol_type = cfg.get('ws', 'symbol_type')  # 合约种类：BTC等

protocol = cfg.get('ws', 'protocol')  # 链接前缀:HTTPS或者sockets
volume = cfg.get('ws', 'volume')  # 合约张数
lever_rate = cfg.get('ws', 'lever_rate')  # 最前五
contract_type = cfg.get('ws', 'contract_type')  # 当前合约时间：this_week,this_year等
MTimePeriod = cfg.get('ws', 'MTimePeriod')  # MA线参数
TTimePeriod = cfg.get('ws', 'TTimePeriod')  # EMA线参数
LogCsvFile = cfg.get('ws', 'LogCsvFile')  # EMA线参数

_host = cfg.get('ws', '_host')  # 服务器地址
url = protocol + _host

csv_file = cfg.get('ws', 'LogCsvFile')  # 程序交易日志csv地址


headers = ['contract_code', 'lever_rate', 'created_at', 'direction', 'offset', 'order_price_type',
           'volume', 'price', 'trade_volume', 'trade_avg_price', 'profit', 'fee', 'status']  # 交易数据表头
# 合约 杠杆 委托时间 交易 类型 委托类型 委托量(张) 委托价(USD) 成交量(张) 成交均价(USD) 收益均价(BTC) 手续费(BTC) 状态
if not os.path.isfile('../%s.csv'%(LogCsvFile)):
    with open('../%s.csv'%(LogCsvFile), 'w', newline='') as f:  # newline=" "是为了避免写入之后有空行
        ff = csv.writer(f)
        ff.writerow(headers)
        f.close()


trading_interval = 2
BTC_Trade = False  # 判断交易是否成功
Already_Judged = False  # 判断是否交易完成和撤销失败订单
Braiding = False
trade_time = None
trade_number = None
trade_volume = None
trade_sell_time = None
trade_sell_number = None
trade_sell_volume = None
BuyOpen = None
price =''
direction = None
offset = None
order_price_type = None
# 订单报价类型 "limit":限价 "opponent":对手价 "post_only":只做maker单,post only下单只受用户持仓数量限制,
# optimal_5：最优5档、optimal_10：最优10档、optimal_20：最优20档，ioc:IOC订单，fok：FOK订单
sell_BuyOpen = False
sell_direction = None
sell_offset = None
sell_order_price_type = None
MA_EMA = None
Already_SELL_Judged = False
time1=0
dm = HuobiDM(url, accessKey, secretKey)
while 1:
    time6 = time.clock()
    print(time6-time1)
    if time6-time1<=30:
        time.sleep(30-(time6-time1))
    time1 = time.clock()  # 获取时间0x01
    try:

        Klines = dm.get_contract_kline(symbol=symbol, period=period, size=size)  # 获取k线
        logger.info(u' 获取K线数据：%s ' % str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(Klines['ts'] / 1000))))

    except:
        continue

    kl = []
    time2 = time.clock()  # 获取时间0x02
    # 处理数据改为pd格式
    for kline in Klines['data']:
        kl.append([kline['id'], kline['high'], kline['low'], kline['open'], kline['vol'], kline['close'],
                   kline['amount']])
    data_df = pd.DataFrame(kl, columns=['id', 'high', 'low', 'open', 'vol', 'close', 'amount'])
    time3 = time.clock()  # 获取时间0x03
    Klines.clear()
    MA = talib.MA(data_df['close'], timeperiod=int(MTimePeriod), matype=0)  # 获取MA线值
    EEMA = talib.EMA(data_df['close'], timeperiod=int(TTimePeriod))  # 获取EMA线值
    time4 = time.clock()  # 获取时间0x04
    logger.info("时间间隔：%s%s%s"%(str(time2 - time1), str(time3 - time2),str( time4 - time3)))
    logger.info("MA值：%s，时间戳：%s"%(str(MA[len(MA) - 1]),1))
    logger.info("EMA：%s"% str(EEMA[len(EEMA) - 1]))
    # 如果该程序未下单，继续进行判断MA与EMA，是否合适开单。如果已经下单，需要判断，是否已经交易，和五分钟后取消订单情况。
    print(BTC_Trade)
    print("MA值：%s"%str(MA[len(MA) - 1]),"EMA：%s"% str(EEMA[len(EEMA) - 1]))
    del data_df

    if not BTC_Trade:
        if not Braiding:
            if MA[len(MA) - 1] > EEMA[len(EEMA) - 1]: # 此时判断是否合适买入
                BuyOpen = False # 等待买入BuySell(买空)
                direction = 'sell'
                offset = 'open'
                price = MA[len(MA) - 1]
                order_price_type = 'opponent'
                sell_direction = 'buy'
                sell_offset = 'close'
                sell_order_price_type = 'optimal_5'
                logger.info("# 此时判断是否合适买入:%s%s"%(BuyOpen,"买空"))

                print("# 此时判断是否合适买入:",BuyOpen,"买空")

            else:
                BuyOpen = True # 等待买入BuySell(买多)
                direction = 'buy'
                offset = 'open'
                price = MA[len(MA) - 1]
                order_price_type = 'opponent'
                sell_direction = 'sell'
                sell_offset = 'close'
                sell_order_price_type = 'optimal_5'
                logger.info("# 此时判断是否合适买入:%s%s"%(BuyOpen,"买多"))

                print("# 此时判断是否合适买入:",BuyOpen, "买多")

            Braiding = True
        if BuyOpen:
            MA_EMA = MA[len(MA) - 1] > EEMA[len(EEMA) - 1]
        else:
            MA_EMA = MA[len(MA) - 1] <= EEMA[len(EEMA) - 1]
        logger.info('BuyOpen%s%s%s'%(BuyOpen,'购买时机：', MA_EMA))

        print('BuyOpen', BuyOpen,'购买时机：', MA_EMA)
        if MA_EMA:
            # 建立订单 价格：MA[int(size) - 1]
            print(symbol_type,contract_type,int(volume),direction,offset,lever_rate,order_price_type)
            Order = dm.send_contract_order(symbol=symbol_type, contract_type=contract_type, contract_code='',
                                           client_order_id='', price=price,
                                           volume=int(volume),
                                           direction=direction,
                                           offset=offset, lever_rate=lever_rate, order_price_type=order_price_type)
            # 打印订单信息
            logger.info("下单返回信息：", Order)
            pprint(Order)
            # 判断订单是否成功
            if Order['status'] == 'error':
                logger.error("下单失败：%s"% Order['err_msg'])
                print(Order['err_msg'])
                break
            elif Order['status'] == 'ok':
                BTC_Trade = True
                Already_Judged = True
                trade_time = time.time()
                print(Order['data'])
                trade_number = Order['data']['order_id_str']
                print(trade_number)


            Order.clear()
        else:
            continue

    else:  # 判断是否需要平仓
        if Already_Judged:
            time5 = time.time()  # 获取当前时间
            # 如果时间差大于定额时间，
            if time5 - trade_time >= trading_interval:
                print(symbol,trade_number)
                Order = dm.get_contract_order_info(symbol=symbol_type, order_id=trade_number)
                logger.info("查询合约订单返回信息：%s"%Order)

                pprint(Order)
                # 判断订单是否成功
                if Order['status'] == 'error':
                    logger.error("查询合约订单返回查询失败：%s"%Order['err_msg'])
                    print(Order['err_msg'])
                    continue
                elif Order['status'] == 'ok':

                    data_btc = Order['data'][0]

                    head = [data_btc['contract_code'], data_btc['lever_rate'], data_btc['created_at'],
                            data_btc['direction'], data_btc['offset'], data_btc['order_price_type'],
                            data_btc['volume'], data_btc['price'], data_btc['trade_volume'],
                            data_btc['trade_avg_price'], data_btc['profit'], data_btc['fee'],
                            data_btc['status']]  # 交易数据表头
                    trade_change_number = data_btc['trade_volume']
                    # 合约 杠杆 委托时间 交易 类型 委托类型 委托量(张) 委托价(USD) 成交量(张) 成交均价(USD)
                    # 收益均价(BTC) 手续费(BTC) 状态
                    with open('%s.csv'%(LogCsvFile), 'a', newline='') as f:  # newline=" "是为了避免写入之后有空行
                        ff = csv.writer(f)
                        ff.writerow(head)
                        f.close()
                        # (1准备提交 2准备提交 3已提交 4部分成交 5部分成交已撤单 6全部成交 7已撤单 11撤单中)
                    print(data_btc['status'],type(data_btc['status']))
                    if data_btc['status'] != 6:
                        Order1 = dm.cancel_contract_order(symbol=symbol_type, order_id=trade_number)
                        if len(Order1['data']['errors']) > 0:
                            for order_error in Order1['data']['errors']:
                                logger.error("撤单失败：%s"%order_error)
                                print("撤单失败：", order_error)
                            continue
                        elif Order1['data']['successes'] == trade_number:

                            BTC_Trade = False
                            logger.error("撤单成功：%s"%trade_number)
                            print("撤单成功：", trade_number)
                            continue
                    elif data_btc['status']==4 or data_btc['status']==5or data_btc['status']==7:
                        Already_Judged = False
                        logger.error("下单成功全部成交：%s" % trade_number)
                        print("下单成功全部成交：", trade_number)
                        continue
                    elif data_btc['status'] == 6:
                        Already_Judged = False
                        logger.error("下单成功全部成交：%s"%trade_number)
                        print("下单成功全部成交：", trade_number)
                        continue
                Order.clear()
            else:
                continue
        else:
            if Already_SELL_Judged:
                time5 = time.time()  # 获取当前时间
                # 如果时间差大于定额时间，
                if time5 - trade_time >= trading_interval:
                    Order = dm.get_contract_order_info(symbol=symbol_type, order_id=trade_sell_number)
                    logger.info("查询合约订单返回信息：%s"%Order)
                    # 判断订单是否成功
                    if Order['status'] == 'error':
                        logger.error("查询合约订单返回查询失败：%s"%Order['err_msg'])
                        print(Order['err_msg'])
                        continue
                    elif Order['status'] == 'ok':
                        data_btc = Order['data'][0]

                        head = [data_btc['contract_code'], data_btc['lever_rate'], data_btc['created_at'],
                                data_btc['direction'], data_btc['offset'], data_btc['order_price_type'],
                                data_btc['volume'], data_btc['price'], data_btc['trade_volume'],
                                data_btc['trade_avg_price'], data_btc['profit'], data_btc['fee'],
                                data_btc['status']]  # 交易数据表头
                        trade_change_number = data_btc['trade_volume']
                        # 合约 杠杆 委托时间 交易 类型 委托类型 委托量(张) 委托价(USD) 成交量(张) 成交均价(USD)
                        # 收益均价(BTC) 手续费(BTC) 状态
                        print(head)
                        with open('%s.csv'%(LogCsvFile), 'a', newline='') as f:  # newline=" "是为了避免写入之后有空行
                            ff = csv.writer(f)
                            ff.writerow(head)
                            f.close()
                            # (1准备提交 2准备提交 3已提交 4部分成交 5部分成交已撤单 6全部成交 7已撤单 11撤单中)
                        if data_btc['status'] == 6:
                            trading_interval = 1
                            BTC_Trade = False  # 判断交易是否成功
                            Already_Judged = False  # 判断是否交易完成和撤销失败订单
                            Braiding = False
                            trade_time = None
                            trade_number = None
                            trade_volume = None
                            trade_sell_time = None
                            trade_sell_number = None
                            trade_sell_volume = None
                            BuyOpen = None
                            direction = None
                            offset = None
                            order_price_type = None
                            # 订单报价类型 "limit":限价 "opponent":对手价 "post_only":只做maker单,post only下单只受用户持仓数量限制,
                            # optimal_5：最优5档、optimal_10：最优10档、optimal_20：最优20档，ioc:IOC订单，fok：FOK订单
                            sell_BuyOpen = False
                            sell_direction = None
                            sell_offset = None
                            sell_order_price_type = None
                            MA_EMA = None
                            Already_SELL_Judged = False
                            continue
                        else:
                            Order1 = dm.cancel_contract_order(symbol=symbol, order_id=trade_number)
                            if len(Order1['data']['errors']) > 0:
                                for order_error in Order1['data']['errors']:
                                    logger.error("撤单失败：%s"%order_error)
                                    print("撤单失败：", order_error)
                                continue
                            elif Order1['data']['successes'] == trade_number:
                                BTC_Trade = False
                                logger.error("撤单成功：%s"%trade_number)
                                print("撤单成功：", trade_number)
                                Order_sell = dm.send_contract_order(symbol=symbol_type, contract_type=contract_type,
                                                                    contract_code='',
                                                                    client_order_id='', price="",
                                                                    volume=volume, direction=sell_direction,
                                                                    offset=sell_offset,
                                                                    lever_rate=lever_rate,
                                                                    order_price_type=sell_order_price_type)
                                logger.info("下单返回信息：%s"%Order_sell)
                                continue
                    logger.info("如果时间差大于定额时间：%s"%(time5 - trade_time >= trading_interval))

                    Order.clear()
                    continue
                else:
                    times = time5 - trade_time
                    logger.info("如果时间差大于定额时间：%s%s%s"%(times,trading_interval,time5 - trade_time >= trading_interval))
                    trading_interval = 1
                    BTC_Trade = False  # 判断交易是否成功
                    Already_Judged = False  # 判断是否交易完成和撤销失败订单
                    Braiding = False
                    trade_time = None
                    trade_number = None
                    trade_volume = None
                    trade_sell_time = None
                    trade_sell_number = None
                    trade_sell_volume = None
                    BuyOpen = None
                    direction = None
                    offset = None
                    order_price_type = None
                    # 订单报价类型 "limit":限价 "opponent":对手价 "post_only":只做maker单,post only下单只受用户持仓数量限制,
                    # optimal_5：最优5档、optimal_10：最优10档、optimal_20：最优20档，ioc:IOC订单，fok：FOK订单
                    sell_BuyOpen = False
                    sell_direction = None
                    sell_offset = None
                    sell_order_price_type = None
                    MA_EMA = None
                    Already_SELL_Judged = False
                    continue

            # 判断是否穿线
            if BuyOpen:
                MA_EMA = MA[len(MA) - 1] <= EEMA[len(EEMA) - 1]
            else:
                MA_EMA = MA[len(MA) - 1] > EEMA[len(EEMA) - 1]
            logger.info('BuyOpen%s%s%s' % (BuyOpen, '卖出时机：', MA_EMA))

            print('BuyOpen', BuyOpen, '卖出时机：', MA_EMA)
            if MA_EMA:
                Order_sell = dm.send_contract_order(symbol=symbol_type, contract_type=contract_type,
                                                    contract_code='',
                                                    client_order_id='', price="",
                                                    volume=volume, direction=sell_direction, offset=sell_offset,
                                                    lever_rate=lever_rate, order_price_type=sell_order_price_type)
                logger.info("下单返回信息：", Order_sell)
                # 判断订单是否成功
                if Order_sell['status'] == 'error':
                    logger.error("下单失败：%s"%Order_sell['err_msg'])
                    print(Order_sell['err_msg'])
                    continue
                elif Order_sell['status'] == 'ok':
                    trade_sell_time = Order_sell['ts']
                    trade_sell_number = Order_sell['data']['order_id']
                    Already_SELL_Judged = True
                    logger.error("下单成功：%s"%Order_sell['status'])
                Order_sell.clear()

