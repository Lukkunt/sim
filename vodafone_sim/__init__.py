# THIS SCRIPT TAKES A SIM_Inventory_Full.csv FILE FROM VODAFONE AND CREATES A SMALLER CSV FILLE FROM IT, WHICH CONTAINS "IMSI", "SIM_State", "Has_Been_Active.Test" COLUMNS
import pandas as pd
from loguru import logger
# 'C:\Code\sim\Data\SIM_Inventory_Full.csv'
# "C:\Code\sim\Data\Filtered_Data.csv"


def main(SIM_Inventory_Full_File_Path, Filtered_Data_Output_File_Path):
    data = pd.read_csv(SIM_Inventory_Full_File_Path)

    # cuts full data to 4 collumns
    data = data[["ICCID", "IMSI", "SIM_State"]]
    # sets ICCID as index for data
    data = data.set_index("ICCID")
    # creates a csv file containing all active vodafone simcards with Has_Been_Active.Test : Y or N
    data.to_csv(Filtered_Data_Output_File_Path)


main('C:\Code\sim\Data\SIM_Inventory_Full.csv',
     "C:\Code\sim\Data\Filtered_Data.csv")
