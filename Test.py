import base.Dao as dao
import base.Log as log
import base.Util as util

Prepare_TimeLen = 70

Filter_High_Distance = 30

me = {}

def _log(ctn):
    file = open("log.out", "a")
    file.write(ctn + "\n")
    file.close()


def prepare(tradingDate):
    securities = [item['code'] for item in dao.select("select distinct code from t_security_concept", ())]
    me.setdefault('securities', securities)
    #code_items_rel
    code_items_rel = {}
    starttime = util.preOpenDate(tradingDate, Prepare_TimeLen)
    total = dao.select('select count(0) count from t_security_daily where date>=%s order by date desc', (starttime))[0]['count']
    fromindex = 0
    pagesize = 3600
    count = 0
    while True:
        print("Range: " + str(fromindex) + "->" + str(fromindex + pagesize))
        tem_codes = me['securities'][fromindex:(fromindex+pagesize)]
        items = dao.select('select code, date, pre_close, high, low, close, open from t_security_daily where code in %s and date>=%s and date < %s order by date desc',(tem_codes, starttime, tradingDate))
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
    code_tradingDateItem_rel = {}
    items = dao.select(
        'select code, date, pre_close, high, low, close, open from t_security_daily where date = %s order by date desc',
        (tradingDate))
    for item in items:
        code = item['code']
        code_tradingDateItem_rel.setdefault(code, item)
    me.setdefault('code_tradingDateItem_rel', code_tradingDateItem_rel)


def testZhangtingSuccessRate(tradingDate):
    securities = me['securities']
    code_items_rel = me['code_items_rel']
    code_tradingDateItem_rel = me['code_tradingDateItem_rel']

    highIsZhangting_count = 0
    closeIsZhangting_count = 0
    for code in securities:

        if code not in code_items_rel.keys(): continue
        if code not in code_tradingDateItem_rel.keys(): continue

        # 过滤新高距离小于High_Distance的个股-------------------------------------------
        items = code_items_rel[code]
        x1 = 0
        for item in items[1:]:
            close = float(item['close'])
            if float(items[0]['close']) >= close:
                x1 = x1 + 1
            else:
                break
        x2 = 0
        for item in items[1:]:
            close = float(item['close'])
            if round(float(code_tradingDateItem_rel[code]['pre_close'])*1.04, 2) > close:
                x2 = x2 + 1
            else:
                break

        distance = x2 - x1
        if distance < Filter_High_Distance:
            continue

        item = code_tradingDateItem_rel[code]

        open = float(item['open'])
        close = float(item['close'])
        high = float(item['high'])
        pre_close = float(item['pre_close'])
        close_rate = round((close - pre_close) / pre_close * 100, 2)
        open_rate = round((open - pre_close) / pre_close * 100, 2)
        high_rate = round((high - pre_close) / pre_close * 100, 2)

        if open_rate > 5:
            continue
        if high_rate >= 9.89:
            highIsZhangting_count = highIsZhangting_count + 1
        if close_rate >= 9.89:
            closeIsZhangting_count = closeIsZhangting_count + 1
            _log("Success Code: " + code)
        if high_rate >= 9.89 and close_rate < 9.89:
            _log("Fail Code: " + code)

    if highIsZhangting_count == 0:
        _log("tradingDate: " + tradingDate + " sr: 0.00")
    else:
        _log("tradingDate: " + tradingDate + " sr: " + str(round(closeIsZhangting_count / highIsZhangting_count * 100, 2)))


tradingDate = '2018-08-17'
while True:
    _log("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@Start Testing: " + tradingDate)
    prepare(tradingDate)
    testZhangtingSuccessRate(tradingDate)
    tradingDate = util.preOpenDate(tradingDate, 1)
    me = {}









