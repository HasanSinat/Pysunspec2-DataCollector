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

ABB İNVERTERLERDE (ÖR/CENA) FAZ BAZINDA GÜÇ DEĞERİ GELMİYOR. AYRICA HESAPLANMALI.

"""
inverter_list = secrets.inverter_list
pd.get_option("display.max_columns")
threePhaseInvDatas = ["AphA","AphB","AphC","PPVphAB","PPVphBC","PPVphCA","W","WH","Hz","VAr_SF","TmpSnk","TmpCab","St","Evt1","Evt2"]
compiled_Inverter_Frame = pd.DataFrame()
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

        #Dataların sözlük halinde çekilemsi
        inverterDict = inverter.get_dict() 
        mpptDict= mppt.get_dict()
        inverterInfoDict = inverterInfo.get_dict()
        measurements_Status = measurements_Status.get_dict()

        mpptDict = mpptDict["module"] #nested mppt değerlerinin ayrıştırılması

        #Sözlük formatında çekilen dataların dataframeler haline getirilmesi
        mppt_df =pd.DataFrame(mpptDict).fillna(0)
        inverter_df= pd.Series(inverterDict).to_frame().fillna(0)
        inverterInfo_df =pd.Series(inverterInfoDict).to_frame()
        measurements_df = pd.Series(measurements_Status).to_frame().fillna(0)

        #gelen dataların kendi dataframeimiz için tanımların datalara karşılık gelenlenlerinin ayıklanması
        inverter_df= inverter_df.loc[threePhaseInvDatas]
        
        file_name_inv = f"A_{port}_{current_time}"
        file_name_mrm= f"B_{port}_{current_time}"
        
        #measurements_df.to_csv(fr'C:\Users\hasan\Desktop\pysunspec2-datacollector\Datas\{file_name_mrm}.csv')
       
        inverterFrame = dataFrames.inverterFrame
        qMeter_Frame_1 = dataFrames.qMeter_1
        print(inverter_df)
        #print(inverterInfo_df)
        #print(mppt_df)

        #Toplam DC güç değeri gelmediğinden burda ayrıca hesaplatıyorum
        DcPSum = 0
        for i in range(6):
            DcPSum += mppt_df.iloc[i]["DCW"] 


        #DC Data

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
        inverterFrame["DC_U4"]=mppt_df.iloc[3]["DCV"]
        inverterFrame["DC_I4"]=mppt_df.iloc[3]["DCA"]
        inverterFrame["DC_P4"]=mppt_df.iloc[3]["DCW"]
        inverterFrame["DC_U5"]=mppt_df.iloc[4]["DCV"]
        inverterFrame["DC_I5"]=mppt_df.iloc[4]["DCA"]
        inverterFrame["DC_P5"]=mppt_df.iloc[4]["DCW"]
        inverterFrame["DC_U6"]=mppt_df.iloc[5]["DCV"]
        inverterFrame["DC_I6"]=mppt_df.iloc[5]["DCA"]
        inverterFrame["DC_P6"]=mppt_df.iloc[5]["DCW"]
        inverterFrame["DC_PSum"]=DcPSum

        #inverter Phase Data

        inverterFrame["AC_U1"]=inverter_df.loc["PPVphAB"]
        inverterFrame["AC_I1"]=inverter_df.loc["AphA"]
        #inverterFrame["AC_P1"]=inverter_df.loc["AphA"]
        inverterFrame["AC_U2"]=inverter_df.loc["PPVphBC"]
        inverterFrame["AC_I2"]=inverter_df.loc["AphB"]
        #inverterFrame["AC_P2"]=inverter_df.loc["AphB"]
        inverterFrame["AC_U3"]=inverter_df.loc["PPVphCA"]
        inverterFrame["AC_I3"]=inverter_df.loc["AphC"]
        #inverterFrame["AC_P3"]=inverter_df.loc["Aphc"]
        inverterFrame["AC_PSum"]=inverter_df.loc["W"]
        inverterFrame["DailyYield"]=inverter_df.loc["WH"]
        inverterFrame["AC_Frq"]=inverter_df.loc["Hz"]
        inverterFrame["CosPhi"]=inverter_df.loc["VAr_SF"]
        inverterFrame["Temp1"]=inverter_df.loc["TmpSnk"]
        inverterFrame["Temp2"]=inverter_df.loc["TmpCab"]
        inverterFrame["State1"]=inverter_df.loc["St"]
        inverterFrame["Err1"]=inverter_df.loc["Evt1"]
        inverterFrame["Err2"]=inverter_df.loc["Evt2"]

        compiled_Inverter_Frame = pd.concat([compiled_Inverter_Frame, inverterFrame], ignore_index=True)
        compiled_Inverter_Frame.fillna(0, inplace=True)


        #Qmeter-1
        qMeter_Frame_1["Datetime"] = [ts]
        




        
#compiled_Inverter_Frame.set_index("DateTime",inplace=True)   

print(compiled_Inverter_Frame)

#CSV Formatına Dönüşüm
with open(fr'C:\Users\hasan\Desktop\pysunspec2-datacollector\Datas\{file_name_inv}.csv', 'w' , newline='') as fp:
    fp.write('#INVERTERS\n')
    compiled_Inverter_Frame.to_csv(fp, sep=";" ,index=False)
    fp.write('#SENSORS\n')
    dataFrames.sensorFrame.to_csv(fp, sep=";" ,index=False)
    fp.write('\n')
    fp.write("#END")

#compiled_Inverter_Frame.to_csv (fr'C:\Users\hasan\Desktop\pysunspec2-datacollector\Datas\{file_name_inv}.csv',sep=";" , index=False)
