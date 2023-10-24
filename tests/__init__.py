import os
import pandas as pd


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
    print(filt.head())
    filt.to_csv(Filter_Path)

    # Filter data to contain only non-zero values
    non_zero_data = data[filt]

    return total_sum, non_zero_data


# Get the path to the folder containing the CSV files
folder_path = 'C:\Code\sim\Data\Source'

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    # Check if the filename starts with "Itemised_data_usage_for_device_(STCU)_"
    if filename.startswith('Itemised_data_usage_for_device_(STCU)_'):
        # Extract the information after "Itemised_data_usage_for_device_(STCU)_"
        info = filename.split('Itemised_data_usage_for_device_(STCU)_')[
            1].split('.')[0]

        # Construct the full path to the file
        file_path = os.path.join(folder_path, filename)

        # Construct the output file paths
        filtered_file_path = os.path.join(
            folder_path, 'filtered_' + info + '.csv')
        filter_path = os.path.join(folder_path, 'filter_' + info + '.csv')

        # Call the process_sim_data() function for the current file
        total_sum, non_zero_data = process_sim_data(
            file_path, filtered_file_path, filter_path)
