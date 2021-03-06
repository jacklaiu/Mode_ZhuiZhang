import base.Dao as dao
import base.Log as log
import base.Util as util
import tushare as ts
import requests
import time

Prepare_TimeLen = 100

Filter_FutureRate = 1.03

Filter_Left_PreReate_Continue = -2

Filter_High_Distance = 20

Listen_CurrentLimitRate = 14

me = {}

def prepare():
    #securities
    securities = [item['code'] for item in dao.select("select distinct code from t_security_concept", ())]
    me.setdefault('securities', securities)
    #code_items_rel
    code_items_rel = {}
    starttime = util.preOpenDate(util.getLastestOpenDate(), Prepare_TimeLen)
    total = dao.select('select count(0) count from t_security_daily where date>=%s order by date desc', (starttime))[0]['count']
    fromindex = 0
    pagesize = 100
    count = 0
    while True:
        print("Range: " + str(fromindex) + "->" + str(fromindex + pagesize))
        tem_codes = me['securities'][fromindex:(fromindex+pagesize)]
        items = dao.select('select code, date, pre_close, high, low, close, open from t_security_daily where code in %s and date>=%s order by date desc',(tem_codes, starttime))
        for item in items:
            code = item['code']
            date = item['date']
            if code not in code_items_rel.keys():
                code_items_rel.setdefault(code, [item])
            else:
                code_items_rel[code].append(item)
            count = count + 1
            log.log('Code: ' + code + " Date: " + date)
        if tem_codes.__len__() != pagesize or count >= total:
            break
        fromindex = fromindex + pagesize
    me.setdefault('code_items_rel', code_items_rel)

    #code_lastestClose_rel
    code_lastestItem_rel = {}
    items = dao.select(
        'select code, date, pre_close, high, low, close, open from t_security_daily where date in (select max(date) max from t_security_daily) order by date desc',
        ())
    for item in items:
        code = item['code']
        code_lastestItem_rel.setdefault(code, item)
    me.setdefault('code_lastestItem_rel', code_lastestItem_rel)

def filter():
    securities = me['securities']
    code_items_rel = me['code_items_rel']
    code_lastestItem_rel = me['code_lastestItem_rel']
    #code_distance_rel
    code_distance_rel = {}
    #candidates
    #code_preCloseDistance_rel
    code_preCloseDistance_rel = {}
    index = 0
    for code in securities:
        if code not in code_items_rel.keys(): continue
        if code not in code_lastestItem_rel.keys(): continue
        # 过滤昨日涨跌幅小于LT_PreReate_Continue的个股----------------------------------
        item = code_lastestItem_rel[code]
        close = float(item['close'])
        pre_close = float(item['pre_close'])
        
        rate = round((close - pre_close) / pre_close * 100, 2)
        if rate < Filter_Left_PreReate_Continue:
            continue
            
        # 过滤新高距离小于High_Distance的个股-------------------------------------------
        items = code_items_rel[code]
        if code == '002087':
            print()
        x1 = 0
        for item in items[1:]:
            close = float(item['close'])
            if float(code_lastestItem_rel[code]['close']) >= close:
                x1 = x1 + 1
            else:
                code_preCloseDistance_rel.setdefault(code, x1)
                break
        x2 = 0
        for item in items[1:]:
            close = float(item['close'])
            if round(float(code_lastestItem_rel[code]['close']) * Filter_FutureRate, 2) > close:
                x2 = x2 + 1
            else:
                break
        distance = x2 - x1
        if distance < Filter_High_Distance:
            continue
        code_distance_rel.setdefault(code, distance)
        index = index + 1
    candidates = sorted(code_distance_rel.items(), key=lambda item: item[1], reverse=True)
    me.setdefault('candidates', candidates)
    me.setdefault('code_preCloseDistance_rel', code_preCloseDistance_rel)

def listen():
    denyCodes = []
    code_items_rel = me['code_items_rel']
    code_preCloseDistance_rel = me['code_preCloseDistance_rel']
    candidates = me['candidates']
    codes = [item[0] for item in candidates]
    while True:
        if util.getHMS() > '15:00:00': break
        ret = util.getRealTime_Prices(codes)
        code_price_rel = ret['code_price_rel']
        code_rate_rel = ret['code_rate_rel']
        for code in code_price_rel.keys():
            if code not in code_rate_rel.keys() or code_rate_rel[code] > Listen_CurrentLimitRate: continue
            if code in denyCodes: continue
            nowDistance = 0
            for item in code_items_rel[code]:
                close = float(item['close'])
                if code_price_rel[code] >= close:
                    nowDistance = nowDistance + 1
                else:
                    break
            distance = nowDistance - code_preCloseDistance_rel[code]

            #print(distance)
            # 发现符合标的，发送通知给我审核
            if distance > Filter_High_Distance:
                try:
                    requests.get('http://95.163.200.245:64210/smtpclient/zhuizhang_allow_security_buy/'+code+'/jacklaiu@163.com')
                except:
                    time.sleep(5)
                log.log("Send: " + code + " Distance: " + str(distance))
                denyCodes.append(code)

        time.sleep(10)





log.log("Start Mode_ZhuiZhang")
prepareComplete = False
filterComplete = False
while True:
    today = util.getYMD()
    now = util.getHMS()
    if prepareComplete is False and (util.isOpen(today) and now > '09:00:00'):
        log.log('start prepare...')
        prepare()
        prepareComplete = True
    if filterComplete is False and (util.isOpen(today) and now > '09:00:00'):
        log.log('start filter...')
        filter()
        filterComplete = True
    if util.isOpen(today) and'09:30:00' < now < '15:00:00':
        log.log('start listen...')
        listen()
        me = {}
        prepareComplete = False
        filterComplete = False

    time.sleep(60)

# arr = [1,2,3,4]
# print(arr[0:2])
# print(arr[2:4])

# d = {'300293': 10, '600891': 2, '600892': 42, '600893': 12, '600894': 2, '600895': 2, '600896': 1}
# items = d.items()
# d = sorted(d.items(), key=lambda item: item[1], reverse=True)
# print(d)

#
# ld = util.getLastestOpenDate()
# df = ts.get_k_data('601318', util.getLastestOpenDate(), util.getLastestOpenDate())
# index = df.index
# print(df['close'][df.index.values[0]])