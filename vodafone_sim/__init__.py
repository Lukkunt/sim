# THIS SCRIPT TAKES A SIM_Inventory_Full.csv FILE FROM VODAFONE AND CREATES A SMALLER CSV FILLE FROM IT, WHICH CONTAINS "IMSI", "SIM_State", "Has_Been_Active.Test" COLUMNS
import pandas as pd
# imports full data

# 'C:\Code\sim\Data\SIM_Inventory_Full.csv'
# "C:\Code\sim\Data\Filtered_Data.csv"


def main(SIM_Inventory_Full_File_Path, Filtered_Data_Output_File_Path):
    data = pd.read_csv(SIM_Inventory_Full_File_Path)
    # cuts full data to 4 collumns
    data = data[["IMSI", "SIM_State",
                "Has_Been_Active.Test"]]
    # sets IMSI as index for data
    data = data.set_index("IMSI")
    # creates a series which contains true or false values (TRUE = SIM_State: Active.live)
    filt = (data["SIM_State"] == "Active.Live")
    # deletes columns with other state then (SIM_State: Active.live)
    data = data[filt]
    # creates a csv file containing all active vodafone simcards with Has_Been_Active.Test : Y or N
    data.to_csv(Filtered_Data_Output_File_Path)


main('C:\Code\sim\Data\SIM_Inventory_Full.csv',
     "C:\Code\sim\Data\Filtered_Data.csv")
