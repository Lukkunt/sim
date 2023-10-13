# THIS SCRIPT TAKES A SIM_Inventory_Full.csv FILE FROM VODAFONE AND CREATES A SMALLER CSV FILLE FROM IT, WHICH CONTAINS "IMSI", "SIM_State", "Has_Been_Active.Test" COLUMNS
import pandas as pd
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
     "C:\Code\sim\Data\Filtered_Full.csv")


def get_sim_activity(Itemised_data_usage_for_device_File_Path, Filtered_Data_Output_File_Path, Filter_File_Path):
    data = pd.read_csv(Itemised_data_usage_for_device_File_Path)
    # cuts full data to 2 collumns
    data = data[["ICCID", "Total_Kbytes"]]
    # groups all comunication rows for each device
    # displays the max "Total_Kbytes" value ---- if it is "0.00" it means that this device did not communicate in the time range of selected report
    data = data.groupby('ICCID').agg("max")
    # Saves file to csv
    data.to_csv(Filtered_Data_Output_File_Path)
    # CREATES FILTER; TRUE != "0.00";
    filt = (data["Total_Kbytes"] != "0.00")
    # saves filter
    filt.to_csv(Filter_File_Path)
    # filters data to contain only != "0.00" values


get_sim_activity("C:\Code\sim\Data\Itemised_data_usage_for_device_(STCU)_20231001_20231031_2023-10-10T84546390Z.csv",
                 "C:\Code\sim\Data\Filtered_Itemised.csv", "C:\Code\sim\Data\Filter.csv")


def join_full_filter(Filtered_Full, Filter):
    df1 = pd.read_csv(Filtered_Full)
    df2 = pd.read_csv(Filter)
    df1 = df1.set_index("ICCID")
    df2 = df2.set_index("ICCID")
    df3 = df1.join(df2)
    print(df1.head())
    print(df2.head())

    df3.to_csv("C:\Code\sim\Data\Final.csv")


join_full_filter("C:\Code\sim\Data\Filtered_Full.csv",
                 "C:\Code\sim\Data\Filter.csv")
