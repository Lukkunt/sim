import pandas as pd
import os
import sys
import datetime
from pathlib import Path

script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
# script_directory = script_directory.replace("", "")
print(script_directory)
script_directory = os.path.dirname(script_directory)

# Specify the folder paths
# input_folder_path = script_directory.replace("/vodafone_sim", "/Data/Source")
input_folder_path = os.path.join(script_directory, "Data", "Source")
output_folder_path = os.path.join(script_directory, "Data", "Output")
# output_folder_path = script_directory.replace("/vodafone_sim", "/Data/Output")
print(input_folder_path)
print(output_folder_path)


def process_sim_data(source_file_path, filtered_file_path):
    # Read data from the CSV file
    data = pd.read_csv(source_file_path, low_memory=False)

    # Keep only the "ICCID" and "Total_Kbytes" and  columns
    data = data[["ICCID", "Total_Kbytes",]]

    # This function converts the "Total_Kbytes" column in a pandas DataFrame from string to float, handling commas and non-convertible data.
    def convert_total_kbytes_to_float(dataframe):
        # Define a function to convert a single value from string to float
        def convert_single_value(s):
            try:
                # Remove commas and convert to float
                return float(s.replace(',', ''))
            except ValueError:
                return s  # Return the original value if conversion fails

        # Apply the conversion function to the "Total_Kbytes" column
        dataframe['Total_Kbytes'] = dataframe['Total_Kbytes'].apply(
            convert_single_value)

        return dataframe

    # Convert the "Total_Kbytes" column to float
    data = convert_total_kbytes_to_float(data)
    # Now, the "Total_Kbytes" column contains float values

    # Group by "ICCID" and sum the "Total_Kbytes" values
    data = data.groupby('ICCID').agg("sum")

    # Sum the "Total_Kbytes" column values
    total_sum = data['Total_Kbytes'].sum()

    # Save the filtered data to a CSV file
    data.to_csv(filtered_file_path)

    return total_sum,


def join_full_filter(filtered_file_path, filtered_full_path):

    df1 = pd.read_csv(filtered_file_path)

    df2 = pd.read_csv(filtered_full_path)

    df1 = df1.set_index("ICCID")
    df1 = df1.rename(columns={"Total_Kbytes": info})
    df2 = df2.set_index("ICCID")

    df3 = df2.join(df1)
    df3.to_csv(filtered_full_path)

# Filers the SIM_Inventory_Full file to only keep the ICCID, IMSI and SIM_State columns
# saves result to filtered_SIM_Inventory_Full file


def sim_inventory_filter(input_folder_path, output_folder_path):

    for filename in os.listdir(input_folder_path):

        if filename.startswith('SIM_Inventory_Full___'):
            info = filename.split('SIM_Inventory_Full___')[
                1].split('.')[0]
            # Construct the full path to the file
            source_file_path = os.path.join(input_folder_path, filename)
            # Construct the output file paths
            filtered_full_path = os.path.join(
                output_folder_path, 'filtered_SIM_Inventory_Full___' + info + '.csv')
            df1 = pd.read_csv(source_file_path)
            df1 = df1[["ICCID", "IMSI", "SIM_State"]]
            df1.to_csv(filtered_full_path)
            return filtered_full_path, source_file_path


filtered_full_path, source_file_path = sim_inventory_filter(
    input_folder_path, output_folder_path)

# Function new sims from the SIM_Inventory_Full file
# takes the SIM_Inventory_Full file as input
# saves the new sims to new_sim_file


def remove_new_sim(source_file_path, filtered_full_path):
    # Create new sim file path
    new_sim_file_path = (f"{output_folder_path}/new_sim_file.csv")
    # Read data from the CSV file
    data = pd.read_csv(source_file_path)

    # Keep only the "ICCID" and "Total_Kbytes" and  columns
    data = data[["ICCID", "First_Used"]]
    data = data.set_index("ICCID")
    data = data.dropna()
    # Get today's date
    today = datetime.date.today()
    # Define a timedelta of selected days
    d = datetime.timedelta(days=14)
    # Subtract the timedelta from today's date
    a = today - d
    # Define a function to convert the datetime string

    def convert_datetime(s):
        dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S %z")
        return dt.date()  # Return a datetime.date object

    # Apply the function to the column in the DataFrame
    data["First_Used"] = data["First_Used"].apply(convert_datetime)

    # Now you can compare the "First_Used" dates with 'a'
    filt = (data["First_Used"] >= a)
    data = filt[filt == True]
    data.to_csv(new_sim_file_path)
    f_Sim_full = pd.read_csv(filtered_full_path)
    f_Sim_full = f_Sim_full.set_index("ICCID")
    f_Sim_full = f_Sim_full.drop(data.index, axis=0)
    f_Sim_full.to_csv(filtered_full_path)


remove_new_sim(source_file_path, filtered_full_path)

# Loop through all files in the folder
for filename in os.listdir(input_folder_path):

    # Check if the filename starts with "Itemised_data_usage_for_device_(STCU)_"
    if filename.startswith('Itemised_data_usage_for_device_(STCU)_'):
        # Extract the information after "Itemised_data_usage_for_device_(STCU)_"
        info = filename.split('Itemised_data_usage_for_device_(STCU)_')[
            1].split('.')[0]

        # Construct the full path to the file
        source_file_path = os.path.join(input_folder_path, filename)

        # Construct the output file paths
        filtered_file_path = os.path.join(
            output_folder_path, 'filtered_' + info + '.csv')

        # Call the process_sim_data() function for the current file
        total_sum = process_sim_data(
            source_file_path, filtered_file_path,)

        join_full_filter(filtered_file_path,
                         filtered_full_path)


def sum_all_comunication(filtered_full_path):
    df1 = pd.read_csv(filtered_full_path)
    df2 = df1
    df1 = df1.drop(['Unnamed: 0', 'IMSI', 'SIM_State'], axis=1)
    df1 = df1.set_index("ICCID")
    df2 = df2.set_index("ICCID")
    column_names = df1.columns.tolist()
    df1 = df1[column_names].sum(axis=1)
    df1.name = "Sum of all comunication"
    df3 = df2.join(df1)
    df3.to_csv(filtered_full_path)

    return df3


sum_all_comunication(filtered_full_path)


def suspend_list(filtered_full_path):
    df1 = pd.read_csv(filtered_full_path)
    df1 = df1.set_index("ICCID")
    filt = ((df1["Sum of all comunication"] == 0.00)
            & (df1["SIM_State"] == "Active.Live"))
    filt.name = "Suspend"
    df2 = df1.join(filt)
    df2.to_csv(filtered_full_path)
    filt = filt[filt == True]
    filt.to_csv(f"{output_folder_path}/suspend_list.csv")


suspend_list(filtered_full_path)

print("Done")
