import pandas as pd
import os
from loguru import logger

# Specify the folder paths
input_folder_path = 'C:\Code\sim\Data\Source'
output_folder_path = 'C:\Code\sim\Data\Output'

# Get a list of file paths in the input folder
file_paths = [os.path.join(input_folder_path, f) for f in os.listdir(
    input_folder_path) if f.endswith('.csv')]

itemised_data_paths = []
sim_inventory_paths = []
for file_path in file_paths:
    if "Itemised_data_usage_for_device_(STCU)_" in file_path:
        itemised_data_paths.append(file_path)
    elif "SIM_Inventory_Full" in file_path:
        sim_inventory_paths.append(file_path)

logger.debug(itemised_data_paths)
logger.debug(sim_inventory_paths)

# Specify the file paths
Itemised_data_Path = "C:\\Code\\sim\\Data\\Source\\Itemised_data_usage_for_device_(STCU)_20231001_20231020_2023-10-21T123413273Z.csv"
Filtered_Itemised_Path = "C:\\Code\\sim\\Data\\Output\\Filtered_Itemised.csv"
Filter_Path = "C:\\Code\\sim\\Data\\Output\\Filter.csv"
Filtered_Full = "C:\\Code\\sim\\Data\\Output\\Filtered_Full.csv"
Output_Path = "C:\\Code\\sim\\Data\\Output\\Output.csv"
input_folder_path = 'C:\Code\sim\Data\Source'
output_folder_path = 'C:\Code\sim\Data\Output'


def process_sim_data(Itemised_data_Path, Filtered_Itemised_Path, Filter_Path):
    # Read data from the CSV file
    data = pd.read_csv(Itemised_data_Path)

    # Keep only the "ICCID" and "Total_Kbytes" columns
    data = data[["ICCID", "Total_Kbytes"]]

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
    data.to_csv(Filtered_Itemised_Path)

    # Create a filter for values not equal to 0.00
    filt = (data["Total_Kbytes"] != 0.00)

    # Rename the "Total_Kbytes" column to "Active"
    filt = filt.rename("Active")

    # Save the filter to a CSV file (you may want to save it as a boolean mask)
    filt.to_csv(Filter_Path)

    # Filter data to contain only non-zero values
    non_zero_data = data[filt]

    # Print the total sum of Total_Kbytes
    logger.debug({total_sum})
    # print(Itemised_data_Path)

    return total_sum, non_zero_data


for path in itemised_data_paths:
    logger.info(path)
    total_sum, non_zero_data = process_sim_data(
        path, Filtered_Itemised_Path, Filter_Path)
    print(f"Total sum for {path}: {total_sum}")

 # Loop through all files in the folder
for filename in os.listdir(input_folder_path):
    # Check if the filename starts with "Itemised_data_usage_for_device_(STCU)_"
    if filename.startswith('Itemised_data_usage_for_device_(STCU)_'):
        # Extract the information after "Itemised_data_usage_for_device_(STCU)_"
        info = filename.split('Itemised_data_usage_for_device_(STCU)_')[
            1].split('.')[0]

        # Construct the full path to the file
        file_path = os.path.join(input_folder_path, filename)

        # Construct the output file paths
        filtered_file_path = os.path.join(
            output_folder_path, 'filtered_' + info + '.csv')
        filter_path = os.path.join(
            output_folder_path, 'filter_' + info + '.csv')

        # Call the process_sim_data() function for the current file
        total_sum, non_zero_data = process_sim_data(
            file_path, filtered_file_path, filter_path) 

# Print the total sum of Total_Kbytes


def join_full_filter(Filtered_Full, Filter, Filtered_Itemised_Path, Output_Path):
    df1 = pd.read_csv(Filtered_Full)
    df2 = pd.read_csv(Filter_Path)
    df3 = pd.read_csv(Filtered_Itemised_Path)

    df1 = df1.set_index("ICCID")
    df2 = df2.set_index("ICCID")
    df3 = df3.set_index("ICCID")

    df4 = df1.join([df2, df3])

    df4.to_csv(Output_Path)


join_full_filter(Filtered_Full, Filter_Path,
                 Filtered_Itemised_Path, Output_Path)
