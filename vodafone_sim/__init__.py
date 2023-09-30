import pandas as pd
# imports full data
data = pd.read_csv('F:\Code\sim\Data\SIM_Inventory_Full.csv')
# cuts full data to 4 collumns
data = data[["IMSI", "SIM_State",
            "Has_Been_Active.Test"]]
# sets IMSI as index for data
data = data.set_index("IMSI")
# creates a series which contains true or false values (TRUE = SIM_State: Active.live)
filt = (data["SIM_State"] == "Active.Live")
# deletes columns with other state then (SIM_State: Active.live)
data = data[filt]
# filt = (data[""] == "Active.Live")

print(filt)
print(data)
