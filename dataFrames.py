import pandas as pd 

inverterFrame= pd.DataFrame(columns=["DateTime","InvID","DC_U1", "DC_I1", "DC_P1", "DC_U2", "DC_I2", "DC_P2","DC_U3", "DC_I3", "DC_P3","DC_U4", "DC_I4", "DC_P4", "DC_U5", "DC_I5", "DC_P5","DC_U6", "DC_I6", "DC_P6","DC_PSum","AC_U1","AC_I1","AC_P1","AC_U2","AC_I2","AC_P2","AC_U3","AC_I3","AC_P3","AC_PSum","DailyYield","AC_Frq","CosPhi","Temp1",	"Temp2",	"Err1",	"Err2",	"State1",	"State2",	"State3",	"State4",])
#inverterFrame["InvID"] = [1,2,3,4,5,6,7]
inverterFrame.fillna(value = 0,inplace=True)
