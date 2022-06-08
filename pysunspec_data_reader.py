import json
from logging import exception
from operator import index, invert
from tkinter import Frame
import sunspec2.modbus.client as client
import sunspec2.mdef as mdef
import pandas as pd 
import datetime as datetime
import secrets
import dataFrames
"""
- NOTLAR -

ABB İNVERTERLERDE (ÖR/CENA) FAZ BAZINDA GÜÇ DEĞERİ GELMİYOR. AYRICA HESAPLANMALI

"""
inverter_list = secrets.inverter_list
pd.get_option("display.max_columns")
threePhaseInvDatas = ["AphA","AphB","AphC","PhVphA","PhVphB","PhVphC","W","WH","Hz","VAr_SF","TmpSnk","TmpCab","St","Evt1","Evt2"]
inventerIndicator = 1
compiledFrame = pd.DataFrame()
dt = datetime.datetime.now()
ts = datetime.datetime.timestamp(dt)
for invert in inverter_list:
    port = invert["ipport"]
    print (f"PORT {port} Verrileri Yükleniyor..")
    try:
        d = client.SunSpecModbusClientDeviceTCP(slave_id=invert["slave_id"], ipaddr=invert["ipaddr"], ipport=invert["ipport"])
        d.scan()
        data_obtained= True
    except Exception as e:
        data_obtained = False
        print("DATA ALINAMADI")
        pass
    if data_obtained:
        current_time = datetime.datetime.now().strftime("%H:%M:%S").replace(":","") 
        models= d.models #Modelleri içeren sözlük
        #print(models)
        inverter = d.inverter[0] #3 fazlı invertere ait verileri içeren model+
        mppt = d.mppt[0]
        measurements_Status = d.status[0]#Measurement status verilerini içeren model
        inverterInfo = d.common[0]

        
        #inverterJson= inverter.get_json() #json haline dönüşüm
        inverterDict = inverter.get_dict() #Sözlük haliine dönüşüm
        mpptDict= mppt.get_dict()
        mpptJson = mppt.get_json()
        inverterInfoDict = inverterInfo.get_dict()
        measurements_Status = measurements_Status.get_dict()
        print(mpptJson)
        mpptDict = mpptDict["module"]
        mppt_df =pd.DataFrame(mpptDict)
        inverter_df= pd.Series(inverterDict).to_frame()
        inverterInfo_df =pd.Series(inverterInfoDict).to_frame()
        inverter_df= inverter_df.loc[threePhaseInvDatas]
        measurements_df = pd.Series(measurements_Status).to_frame()
        file_name_inv = f"A_{port}_{current_time}"
        file_name_mrm= f"B_{port}_{current_time}"
        
        measurements_df.to_csv(fr'C:\Users\hasan\Desktop\pysunspec2-datacollector\Datas\{file_name_mrm}.csv')
       
        inverterFrame = dataFrames.inverterFrame
        print(inverter_df)
        print(inverterInfo_df)
        print(mppt_df)


            #DC DAta
        inverterFrame["DateTime"]=[ts]
        inverterFrame["InvID"]=[invert["InvID"]]
        inverterFrame["DC_U1"]=mppt_df.iloc[0]["DCV"]
        inverterFrame["DC_I1"]=mppt_df.iloc[0]["DCA"]
        inverterFrame["DC_P1"]=mppt_df.iloc[0]["DCW"]
        inverterFrame["DC_U2"]=mppt_df.iloc[1]["DCV"]
        inverterFrame["DC_I2"]=mppt_df.iloc[1]["DCA"]
        inverterFrame["DC_P2"]=mppt_df.iloc[1]["DCW"]
        inverterFrame["DC_U3"]=mppt_df.iloc[2]["DCV"]
        inverterFrame["DC_I3"]=mppt_df.iloc[2]["DCA"]
        inverterFrame["DC_P3"]=mppt_df.iloc[2]["DCW"]
            #inverter Phase Data
        inverterFrame["AC_U1"]=inverterDict["PhVphA"]
        inverterFrame["AC_I1"]=inverterDict["AphA"]
        #inverterFrame["AC_P1"]=inverterDict["AphA"]
        inverterFrame["AC_U2"]=inverterDict["PhVphB"]
        inverterFrame["AC_I2"]=inverterDict["AphB"]
        #inverterFrame["AC_P2"]=inverterDict["AphA"]
        inverterFrame["AC_U3"]=inverterDict["PhVphC"]
        inverterFrame["AC_I3"]=inverterDict["AphC"]
        #inverterFrame["AC_P3"]=inverterDict["AphA"]
        inverterFrame["AC_PSum"]=inverterDict["W"]
        inverterFrame["DailyYield"]=inverterDict["WH"]
        inverterFrame["AC_Frq"]=inverterDict["Hz"]
        inverterFrame["CosPhi"]=inverterDict["VAr_SF"]
        inverterFrame["Temp1"]=inverterDict["TmpSnk"]
        inverterFrame["Temp2"]=inverterDict["TmpCab"]
        inverterFrame["State1"]=inverterDict["St"]
        inverterFrame["Err1"]=inverterDict["Evt1"]
        inverterFrame["Err2"]=inverterDict["Evt2"]

        compiledFrame = pd.concat([compiledFrame, inverterFrame], ignore_index=True)
        compiledFrame.fillna(0, inplace=True)
        
#compiledFrame.set_index("DateTime",inplace=True)   

print(compiledFrame)

compiledFrame.to_csv (fr'C:\Users\hasan\Desktop\pysunspec2-datacollector\Datas\{file_name_inv}.csv',sep=";" , index=False)
