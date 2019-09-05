#! -*- coding: utf-8 -*-
from con_oracle import *
import requests
import datetime
from lnglat import lng_lat
car = CarDba()
phone = PhoneDba()
dba = Dba()
carcur = car.cursor()
phcur = phone.cursor()
dbacur = dba.cursor()


areaCode = {"320500": u"苏州市", "320501": u"市辖区", "320502": u"工业园区",	
 "320505": u"高新区", "320506": u"吴中区", "320507": u"相城区",	
 "320508": u"姑苏区", "320509": u"吴江区", "320581": u"常熟市",	
 "320582": u"张家港市",	"320583": u"昆山市", "320585": u"太仓市"}

def get_lat_lng(address):
    url = "http://api.map.baidu.com/geocoder/v2/?address=%s&output=json&ak=ROUZ3zeB69pURmbyIISXGA55lL1DCghZ"
    req_url = url % (address)
    sess = requests.session()
    data = sess.get(req_url).json()
    # print(data["message"])
    return data["result"]["location"]

def moveCar(start_time, end_time):
    sql = "select * from (select id ,area_code,sex, customer_name, create_code,create_date,is_right,order_no,inbound_number,case_accord2,case_accord3,mobile_number_customer,caraddr,carletter,car_number,cartype,vehicle_brand,car_color,mobile_num,phone_num,detail_address,case_accord_type,contact_results,reply_model,reply_detail,sms_to_customer,sms_to_carowner,phone_customer,phone_carowner,sms_model_customer,sms_model_carowner,sms_content_cus,sms_content_carowner,update_code,appeal_code,appeal_content,order_status,demand_goal,demand_title,access_content,street, rownum RN from T_TRANSFER_SERVER where create_date < to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and create_date >= to_date('%s', 'yyyy-mm-dd hh24:mi:ss') and area_code='320502') WHERE RN BETWEEN 0 and 90000"
    dbsql = "insert into SDTRANSFER_SERVER (id ,area_code,sex, customer_name, create_code,create_date,is_right,order_no,inbound_number,case_accord2,case_accord3,mobile_number_customer,caraddr,carletter,car_number,cartype,vehicle_brand,car_color,mobile_num,phone_num,detail_address,case_accord_type,contact_results,reply_model,reply_detail,sms_to_customer,sms_to_carowner,phone_customer,phone_carowner,sms_model_customer,sms_model_carowner,sms_content_cus,sms_content_carowner,update_code,appeal_code,appeal_content,order_status,demand_goal,demand_title,access_content,street) values('%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    query_sql = sql % (end_time, start_time)
    data = car.query_data(query_sql)
    con = dba.connect()
    cur = con.cursor()
    print(start_time, "move T_TRANSFER_SERVER data:", len(data))
    for d in data:
        try: 
            d = list(d)[:-1]
            if d[22] == None:
                d[22] = ""
            insql = dbsql % tuple(d)
            cur.execute(insql)
        except Exception as e:
           print(e)
           pass 
           print d[1], d[20], d[22]
           # break
    con.commit()     
    cur.close()
    con.close()

def moveOutPhone(start_time, end_time):
   sql = "select strcallserialno,strivrcallingno,dtenteroutphonetime,intconnectstate,stroutphoneno,stroutphonedisplayno,stroutphoneoriginalno,stroutphonetype,dtoutphonestarttime,dtoutphoneendtime,dtoutphoneringtime,dtoutphoneanswertime,intoutphonedealtype,inthandupflag from  szyq12345.v_tbl_outphonelog where dtenteroutphonetime < to_date('%s','yyyy-mm-dd hh24:mi:ss') and dtenteroutphonetime >= to_date('%s','yyyy-mm-dd hh24:mi:ss')"
   insql = "insert into TBL_OUTPHONELOG(strcallserialno,strivrcallingno,dtenteroutphonetime,intconnectstate,stroutphoneno,stroutphonedisplayno,stroutphoneoriginalno,stroutphonetype,dtoutphonestarttime,dtoutphoneendtime,dtoutphoneringtime,dtoutphoneanswertime,intoutphonedealtype,inthandupflag) values ('%s', '%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'), '%s', '%s') "
   query_sql = sql % (end_time, start_time)
   data = phone.query_data(query_sql)
   con = dba.connect()
   cur = con.cursor()
   print(start_time, "move  OutPhone data:", len(data))
   for d in data:
       outsql = insql % d
       cur.execute(outsql)
       con.commit()
   cur.close()
   con.close()      
 

def moveDetail(start_time, end_time):
    sql = "select  strcallserialno,strcallingno,strivrcallingno,strcalledno,stroriginalcalledno,dtcallstarttime, dtcallendtime,intcalltype,intoutlimitflag,strcallserialno_intercall,inthangupflag,intsystem,intsystemerrorflag from  szyq12345.v_tbl_detailmasterlog where dtcallstarttime < to_date('%s','yyyy-mm-dd hh24:mi:ss') and dtcallstarttime >= to_date('%s','yyyy-mm-dd hh24:mi:ss')"
    insql = "insert into TBL_DETAILMASTERLOG ( strcallserialno,strcallingno,strivrcallingno,strcalledno,stroriginalcalledno,dtcallstarttime, dtcallendtime,intcalltype,intoutlimitflag,strcallserialno_intercall,inthangupflag,intsystem,intsystemerrorflag) values ('%s', '%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s', '%s')"
    query_sql = sql % (end_time, start_time)
    data = phone.query_data(query_sql)
    con = dba.connect()
    cur = con.cursor()
    print(start_time, "move DetailMater data:", len(data))
    for d in data:
        outsql = insql % d
        cur.execute(outsql)
        con.commit()
    cur.close()
    con.close()
     
def moveQueue(start_time, end_time):
    sql = "select strcallserialno,strivrcallingno,strcallingtype,intskillno,dtqueuestarttime,dtqueueendtime,dtseatanswertime,dtseatcallendtime,intqueueresult,intagentid,strseatphoneno,intconnectstate,intqueueerrorflag,intgetqueuestatus,intgetwaitcount,intgetqueuemaxwaittime,intgetqueueaveragewaittime,intgetqueueestimatewaittime,intqueuetype,strcalledno,dtseatringtime from  szyq12345.v_queuedetaillog where dtqueuestarttime < to_date('%s','yyyy-mm-dd hh24:mi:ss') and dtqueuestarttime >= to_date('%s','yyyy-mm-dd hh24:mi:ss')"   
    insql = "insert into TBL_QUEUEDETAILLOG (strcallserialno,strivrcallingno,strcallingtype,intskillno,dtqueuestarttime,dtqueueendtime,dtseatanswertime,dtseatcallendtime,intqueueresult,intagentid,strseatphoneno,intconnectstate,intqueueerrorflag,intgetqueuestatus,intgetwaitcount,intgetqueuemaxwaittime,intgetqueueaveragewaittime,intgetqueueestimatewaittime,intqueuetype,strcalledno,dtseatringtime) values ('%s', '%s','%s','%s', to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'))"
    query_sql = sql % (end_time, start_time)
    data = phone.query_data(query_sql)
    con = dba.connect()
    cur = con.cursor()
    print(start_time, "move QueueDetail data:", len(data))
    for d in data:
        outsql = insql % d
        cur.execute(outsql)
        con.commit()
    cur.close()
    con.close()

def moveWork():
    sql = "select id,data_source,user_code,age,remark,area_code,organ_id,sex,deal_organ_id,deal_organ,create_time,customer_id,customer_name,agent_no,workorder_title,order_no,inbound_number,organ_name,accept_time,att_name,case_accord2,case_accord3,record_url,order_status,demand_goal,access_content,mobile_number1,mobile_number2,belong_area,email_address,identity_number,inbound_time,relation_organ,is_emergency,emergency_degree,demand_type,result_content,deal_way,accept_way,requir_deal_time_sq,requir_deal_time_yq,refuse_time_sq,refuse_time_yq,is_secrecy,is_call_back,att_content,channel_source,case_accord1,sendsmg,feedback_time,recent_visit_time,belonged_to_staff,callback_status,obtain_time,staff_obtain_time,revoke_time,revoke_reson,relation_organ_id,department_accept_time,satisfaction,saverevisitflag,savesatisfa,saverevistrsult,inspect_status,accept_timesq,accept_timeyq,follow_timesq,follow_timeyq,time_delay_hour,applay_status,refuse_reason,time_delay,delate_hour_sq,follow_hour_sq,delate_hour_yq,follow_hour_yq,delate_reson_sq,follow_reson_sq,delate_reson_yq,follow_reson_yq,reqaccept_time_sq,reqaccept_time_yq,recive_phone,sendsms_content,assigned_opinionsq,assigned_opinionyq,callback_content,revoke_flag,remind_time,user_name,address,certificate_type,case_emotion,case_lnglat,case_historynum,delay_sq_time,follow_sq_time,isapplynorevisit,applynorevisitreason,applynorevisitdate,applynorevisitoper,norevisitauditflag,norevisitauditresult,norevisitaudittime,trackrecord,visit_type,visit_name,subject_attitude_satify,visit_time,is_submitflag,modify_time,theme_serial,case_source_detail,relate_serial from T_WORKORDERS where create_time >= to_date('%s','yyyy-mm-dd hh24:mi:ss')"
    insql = "insert into WORKORDERS (id,data_source,user_code,age,remark,area_code,organ_id,sex,deal_organ_id,deal_organ,create_time,customer_id,customer_name,agent_no,workorder_title,order_no,inbound_number,organ_name,accept_time,att_name,case_accord2,case_accord3,record_url,order_status,demand_goal,access_content,mobile_number1,mobile_number2,belong_area,email_address,identity_number,inbound_time,relation_organ,is_emergency,emergency_degree,demand_type,result_content,deal_way,accept_way,requir_deal_time_sq,requir_deal_time_yq,refuse_time_sq,refuse_time_yq,is_secrecy,is_call_back,att_content,channel_source,case_accord1,sendsmg,feedback_time,recent_visit_time,belonged_to_staff,callback_status,obtain_time,staff_obtain_time,revoke_time,revoke_reson,relation_organ_id,department_accept_time,satisfaction,saverevisitflag,savesatisfa,saverevistrsult,inspect_status,accept_timesq,accept_timeyq,follow_timesq,follow_timeyq,time_delay_hour,applay_status,refuse_reason,time_delay,delate_hour_sq,follow_hour_sq,delate_hour_yq,follow_hour_yq,delate_reson_sq,follow_reson_sq,delate_reson_yq,follow_reson_yq,reqaccept_time_sq,reqaccept_time_yq,recive_phone,sendsms_content,assigned_opinionsq,assigned_opinionyq,callback_content,revoke_flag,remind_time,user_name,address,certificate_type,case_emotion,case_lnglat,case_historynum,delay_sq_time,follow_sq_time,isapplynorevisit,applynorevisitreason,applynorevisitdate,applynorevisitoper,norevisitauditflag,norevisitauditresult,norevisitaudittime,trackrecord,visit_type,visit_name,subject_attitude_satify,visit_time,is_submitflag,modify_time,theme_serial,case_source_detail,relate_serial) values ('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s', to_date('%s','yyyy-mm-dd hh24:mi:ss'), '%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s','%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s','%s','%s',)"
    querysql = sql % "2019-01-29 00:00:00"
    data = car.query_data(sql)
    con = dba.connect()
    cur = con.cursor()
    for d in data:
        outsql = insql % d
        cur.execute(outsql)
        con.commit()
    cur.close()
    con.close()

def main():
    today = datetime.datetime.today()
    start_time = (today - datetime.timedelta(1)).strftime("%Y-%m-%d %hh:%mm:%ss")
    end_time = today.strftime("%Y-%m-%d %hh:%mm:%ss")
    #start_time = "2019-06-02 00:00:00"
    #end_time = "2019-06-03 00:00:00"
    start_time = start_time.split(" ")[0] + " 00:00:00"
    end_time = end_time.split(" ")[0] + " 00:00:00"
    moveCar(start_time, end_time)
    moveOutPhone(start_time, end_time)
    moveDetail(start_time, end_time)
    moveQueue(start_time, end_time)
    lng_lat()    



if __name__ == "__main__":
    # moveCar("2017-01-29 00:00:00", "2019-03-01 00:00:00")
    # moveOutPhone("2017-01-29 00:00:00", "2019-03-01 00:00:00")
    # moveDetail("2015-01-29 00:00:00", "2019-03-01 00:00:00")
    #lng_lat()
    main()
    
    # moveQueue("2019-01-29 00:00:00", "2019-03-01 00:00:00")


