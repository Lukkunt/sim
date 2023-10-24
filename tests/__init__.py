import pandas as pd
import os

# Specify the folder paths
input_folder_path = 'C:\Code\sim\Data\Source'
output_folder_path = 'C:\Code\sim\Data\Output'


def process_sim_data(file_paths, output_folder_path):

    for file_path in file_paths:
        # if file_path.startswith('Itemised_data_usage_for_device_(STCU)_'):
        # Read data from the CSV file
        print(file_path)
        data = pd.read_csv(file_path)

        # Keep only the "ICCID" and "Total_Kbytes" columns
        print(data.head())
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

        # Group the data by "ICCID" and sum the "Total_Kbytes" values
        grouped_data = data.groupby("ICCID").sum()

        # Filter out any rows with a "Total_Kbytes" value of 0.00
        non_zero_data = grouped_data[grouped_data["Total_Kbytes"] != 0.00]

        # Save the filtered data to a new CSV file
        file_name = os.path.basename(file_path)
        output_file_path = os.path.join(
            output_folder_path, "filtered_" + file_name)
        non_zero_data.to_csv(output_file_path)

        # Create a filter for non-zero values and save it to a CSV file
        filter_path = os.path.join(
            output_folder_path, "filter_" + file_name)
        filter_data = pd.DataFrame({"ICCID": non_zero_data.index})
        filter_data.to_csv(filter_path, index=False)

    # Return the sum of the "Total_Kbytes" column and the filtered data
    total_sum = non_zero_data["Total_Kbytes"].sum()
    return total_sum, non_zero_data


# Get a list of file paths in the input folder
file_paths = [os.path.join(input_folder_path, f) for f in os.listdir(
    input_folder_path) if f.endswith('.csv')]
print(file_paths)
# Call the process_sim_data() function with the list of file paths
total_sum, non_zero_data = process_sim_data(file_paths, output_folder_path)

# Do something with the results, if needed
