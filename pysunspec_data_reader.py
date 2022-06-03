import json
from logging import exception
from operator import index, invert
import sunspec2.modbus.client as client
import sunspec2.mdef as mdef
import pandas as pd 
import datetime as datetime
import secrets

inverter_list = secrets.inverter_list

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
        inverter = d.inverter[0] #3 fazlı invertere ait verileri içeren model
        measurements_Status = d.status[0]#Measurement status verilerini içeren model
        inverterJson= inverter.get_json() #json haline dönüşüm
        inverterDict = inverter.get_dict() #Sözlük haliine dönüşüm
        measurements_Status = measurements_Status.get_dict()
        inverter_df= pd.Series(inverterDict).to_frame()
        measurements_df = pd.Series(measurements_Status).to_frame()
        file_name_inv = f"A_{port}_{current_time}"
        file_name_mrm= f"B_{port}_{current_time}"
        inverter_df.to_csv (fr'C:\Users\hasan\Desktop\pysunspecDF\Datas\{file_name_inv}.csv', )
        measurements_df.to_csv(fr'C:\Users\hasan\Desktop\pysunspecDF\Datas\{file_name_mrm}.csv')
        print(inverter_df)
        print("-------------------")
        print(measurements_df)
