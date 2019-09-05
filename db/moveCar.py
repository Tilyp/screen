#! -*- coding: utf-8 -*-

from moveCar_oracle import Dba
import xlwt
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

db = Dba()

def caseAccordCount(begin_time, end_time):
    sql = "select case_accord_type, count(case_accord_type) from SDTRANSFER_SERVER where create_date >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss') group by case_accord_type" 
    query_sql = sql % (begin_time, end_time)
    data = db.query_data(query_sql) 
    keyword = {"01":u"交通出行","02":u"民生服务","03":u"民政社区",
        "04":u"住房保障","05":u"劳动人事","06":u"商贸经济","07":u"医疗卫生",
        "08":u"财税金融","09":u"城乡建设","10":u"公共安全","11":u"环境保护",
        "12":u"科教文体","13":u"农林牧渔","14":u"政法监察","15":u"政务党团"}
    result = []
    for d in data:
        if d[0]:
            result.append((keyword[d[0]], d[1]))
    return result


def countCard(begin_time, end_time):
    sql = "select concat(caraddr,concat(carletter,car_number)) as card from SDTRANSFER_SERVER where create_date >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss')"
    query_sql = sql % (begin_time, end_time)
    data = db.query_data(query_sql)
    result = {}
    setL = set()
    for d in data:
        if d[0] not in setL:
            setL.add(d[0])
            result[d[0]] = 1
        else:
            result[d[0]] += 1
    result = sorted(result.items(),key = lambda x:x[1],reverse = True)
    return result

def road(begin_time, end_time, caseCode): 
    sql = "select detail_address, count(detail_address) as num from SDTRANSFER_SERVER where create_date >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and  case_accord3='%s' group by detail_address order by num desc"
    query_sql = sql % (begin_time, end_time, caseCode)
    data = db.query_data(query_sql)
    return data

def focus(begin_time, end_time):
    sql = "select detail_address, count(detail_address) as num from SDTRANSFER_SERVER where create_date >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss') group by detail_address order by num desc"
    query_sql = sql % (begin_time, end_time)
    data = db.query_data(query_sql)
    return data

def lng_lat(begin_time, end_time):
    sql = "select lon,lat from SDTRANSFER_SERVER where create_date >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss')"
    query_sql = sql % (begin_time, end_time)
    data = db.query_data(query_sql)
    return data

def order_total(begin_time, end_time):
    finshed_sql = "select count(*) from SDTRANSFER_SERVER where order_status='17' and create_date>= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss')" 
    query_finsh = finshed_sql % (begin_time, end_time)
    finsh = db.query_data(query_finsh)[0][0]
    total_sql = "select count(*) from SDTRANSFER_SERVER where create_date>= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss')"
    query_total = total_sql % (begin_time, end_time)
    total = db.query_data(query_total)[0][0]
    return ({"order_total": total, "order_finsh": finsh})

def queue_total(begin_time, end_time):
    detail_sql = "select count(*)  from  SZYQ.TBL_DETAILMASTERLOG where DTCALLSTARTTIME >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and DTCALLSTARTTIME < to_date('%s', 'yyyy-mm-dd hh24:mi:ss')"
    query_detail = detail_sql % (begin_time, end_time)
    detail = db.query_data(query_detail)[0][0]
    queue_sql = "select count(*) from SZYQ.TBL_QUEUEDETAILLOG where INTQUEUERESULT=1 and DTQUEUEENDTIME >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and DTQUEUEENDTIME  < to_date('%s', 'yyyy-mm-dd hh24:mi:ss')"
    query_queue = queue_sql % (begin_time, end_time)
    queue = db.query_data(query_queue)[0][0]
    return {"detail": detail, "queue": queue}

def out_total(begin_time, end_time):
    out_sql = "select count(*) from SZYQ.TBL_OUTPHONELOG where DTOUTPHONESTARTTIME >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and DTOUTPHONESTARTTIME < to_date('%s', 'yyyy-mm-dd hh24:mi:ss')"
    query_out = out_sql % (begin_time, end_time)
    out = db.query_data(query_out)[0][0]
    return {"out": out}   


def countdata(begin_time, end_time, caseCode):
    sql = "select count(*) from SDTRANSFER_SERVER where create_date >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and  case_accord3='%s'"
    query_sql = sql % (begin_time, end_time, caseCode)
    data = db.query_data(query_sql)
    return data

def read_xls(start, end, d,worksheet, row):
    data = road(start, end, d["id"])
    count = countdata(start, end, d["id"])[0][0]
    for ind, d in enumerate(data):
        those = round(float(d[1]*100)/float(count), 2)
        worksheet.write(ind+3, row+0, label=str(d[0]))
        worksheet.write(ind+3, row+1, label=str(d[1]))
        worksheet.write(ind+3, row+2, label=str(those))

def sendMail(filename, mouth):
    fromaddr = 'z13814808627@163.com'
    password = '12345678abc'
    toaddrs = ['820017886@qq.com', '935315996@qq.com', 'xfs@sipac.gov.cn']

    content = '你好！, {mouth}月报已发送，请注意查收，如有问题请及时告知.'.format(mouth=mouth)
    textApart = MIMEText(content)

    pdfApart = MIMEApplication(open(filename, 'rb').read())
    pdfApart.add_header('Content-Disposition', 'attachment', filename=filename)

    m = MIMEMultipart()
    m.attach(textApart)
    m.attach(pdfApart)
    m['Subject'] = '12345移车数据{mouth}月报'.format(mouth=mouth)
    m['From'] = '<z13814808627@163.com>'
    m['To'] = "820017886@qq.com, 935315996@qq.com, xfs@sipac.gov.cn"
    try:
        server = smtplib.SMTP('smtp.163.com')
        server.login(fromaddr, password)
        server.sendmail(fromaddr, toaddrs, m.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print('error:', e)  


def report_form():
    data = [{"id": "030806", "name": "Village", "key": u"小区移车"},
            {"id": "010907", "name": "Place", "key": u"公共场所移车"},
            {"id": "010999", "name": "Road", "key": u"道路移车"}]
    xls_title = [u"地址",u"数量",u"占比（%）"]
    try:
        first = datetime.date(datetime.date.today().year, datetime.date.today().month-1, 1)
    except:
        first = datetime.date(datetime.date.today().year-1, (datetime.date.today().month - 1) + 12, 1)
    last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
    mouth = first.strftime('%Y-%m')
    start = first.strftime('%Y-%m-%d') + " 00:00:00"
    end = last.strftime('%Y-%m-%d') + " 00:00:00"
    row = 0
    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet(u'移车高发地址统计', cell_overwrite_ok=True)
    worksheet.write_merge(0, 0, 0, 8, u'移车高发地址统计')
    for d in data:
        read_xls(start,end,d, worksheet, row)
        worksheet.write_merge(1, 1, row, row+2, d["key"])
        for inde, xls in enumerate(xls_title):
            worksheet.write(2, row+inde, xls)        
        row += 3
    cardsheet = workbook.add_sheet(u'相对集中车辆号牌', cell_overwrite_ok=True)
    cardsheet.write_merge(0, 0, 0, 1, u'相对集中车辆号牌')
    cardsheet.write(1,0, u"车牌号码")
    cardsheet.write(1,1, u"工单数量")
    result = countCard(start, end)
    for ind, ds in enumerate(result):
        cardsheet.write(ind+2, 0, label=str(ds[0]))
        cardsheet.write(ind+2, 1, label=str(ds[1]))
    filename = "12345moveCarData{mouth}_Report_Form.xls".format(mouth=mouth)
    workbook.save(filename)
    sendMail(filename, mouth)
    

if __name__ == "__main__":
    # caseAccordCount("2019-01-26 00:00:00", "2019-01-27 00:00:00")
    # countCard("2019-03-01 00:00:00", "2019-04-01 00:00:00")
    # road("2019-01-26 00:00:00", "2019-01-27 00:00:00", "010999")
    # focus("2019-01-26 00:00:00", "2019-01-27 00:00:00")
    # lng_lat("2018-01-26 00:00:00", "2019-01-27 00:00:00")
    # order_total("2019-01-26 00:00:00", "2019-01-27 00:00:00")
    # queue_total("2019-01-26 00:00:00", "2019-01-27 00:00:00")
    # read_xls()
    report_form()
