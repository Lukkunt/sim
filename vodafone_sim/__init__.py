import pandas as pd
import os
from loguru import logger

# Specify the folder paths
input_folder_path = 'C:\Code\sim\Data\Source'
output_folder_path = 'C:\Code\sim\Data\Output'


def process_sim_data(source_file_path, filtered_file_path, filter_Path):
    # Read data from the CSV file
    data = pd.read_csv(source_file_path)

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
    data.to_csv(filtered_file_path)

    # Create a filter for values not equal to 0.00
    filt = (data["Total_Kbytes"] != 0.00)

    # Rename the "Total_Kbytes" column to "Active"
    filt = filt.rename("Active")

    # Save the filter to a CSV file (you may want to save it as a boolean mask)
    filt.to_csv(filter_Path)

    # Filter data to contain only non-zero values
    non_zero_data = data[filt]

    # Print the total sum of Total_Kbytes
    logger.debug({total_sum})
    # print(Itemised_data_Path)

    return total_sum, non_zero_data


def join_full_filter(filtered_file_path, filtered_full_path):

    df1 = pd.read_csv(filtered_file_path)

    df2 = pd.read_csv(filtered_full_path)

    df1 = df1.set_index("ICCID")
    df1 = df1.rename(columns={"Total_Kbytes": info})
    df2 = df2.set_index("ICCID")

    df3 = df2.join(df1)
    df3.to_csv(filtered_full_path)


def sim_inventory_filter(input_folder_path, output_folder_path):

    for filename in os.listdir(input_folder_path):

        if filename.startswith('SIM_Inventory_Full___'):
            logger.debug("1")
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
            return filtered_full_path


filtered_full_path = sim_inventory_filter(
    input_folder_path, output_folder_path)

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
        filter_path = os.path.join(
            output_folder_path, 'filter_' + info + '.csv')

        # Call the process_sim_data() function for the current file
        total_sum, non_zero_data = process_sim_data(
            source_file_path, filtered_file_path, filter_path)

        join_full_filter(filtered_file_path,
                         filtered_full_path)


def sum_all_comunication(filtered_full_path):
    df1 = pd.read_csv(filtered_full_path)
    df1 = df1.drop(['Unnamed: 0', 'IMSI', 'SIM_State'], axis=1)
    df1.set_index("ICCID")
    column_names = df1.columns.tolist()
    print(df1[column_names].sum().head())

    return df1


sum_all_comunication(filtered_full_path)
