import pandas as pd


def process_sim_data(Itemised_data_usage_for_device_File_Path, Filtered_Data_Output_File_Path, Filter_File_Path):
    # Read data from the CSV file
    data = pd.read_csv(Itemised_data_usage_for_device_File_Path)

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
    data.to_csv(Filtered_Data_Output_File_Path)

    # Create a filter for values not equal to 0.00
    filt = (data["Total_Kbytes"] != 0.00)

    # Save the filter to a CSV file (you may want to save it as a boolean mask)
    filt.to_csv(Filter_File_Path, index=False)

    # Filter data to contain only non-zero values
    non_zero_data = data[filt]

    return total_sum, non_zero_data


# Specify the file paths
Itemised_data_usage_for_device_File_Path = "C:\\Code\\sim\\Data\\Itemised_data_usage_for_device_(STCU)_20231001_20231031_2023-10-10T84546390Z.csv"
# Itemised_data_usage_for_device_File_Path = "C:\\Code\\sim\\Data\\test_111 - Sheet1.csv"
Filtered_Data_Output_File_Path = "C:\\Code\\sim\\Data\\Filtered_Itemised2.csv"
Filter_File_Path = "C:\\Code\\sim\\Data\\Filter.csv"

# Call the function to process the data and save results
total_sum, non_zero_data = process_sim_data(
    Itemised_data_usage_for_device_File_Path, Filtered_Data_Output_File_Path, Filter_File_Path)

# Print the total sum of Total_Kbytes
print("Total Sum of Total_Kbytes:", total_sum)
