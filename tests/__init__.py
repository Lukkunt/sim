import pandas as pd


def process_sim_data(Itemised_data_usage_for_device_File_Path, Filtered_Data_Output_File_Path, Filter_File_Path):
    # Read data from the CSV file
    data = pd.read_csv(Itemised_data_usage_for_device_File_Path)

    # Keep only the "ICCID" and "Total_Kbytes" columns
    data = data[["ICCID", "Total_Kbytes"],]

    # Function to sepate strings with multiple periods and convert them to float

    def convert_to_float(s):
        parts = s.split(',')  # Split by ","
        return sum(map(float, parts))

    # Apply the conversion function to "Total_Kbytes" column
    data['Total_Kbytes'] = data['Total_Kbytes'].apply(convert_to_float)

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
Filtered_Data_Output_File_Path = "C:\\Code\\sim\\Data\\Filtered_Itemised2.csv"
Filter_File_Path = "C:\\Code\\sim\\Data\\Filter.csv"

# Call the function to process the data and save results
total_sum, non_zero_data = process_sim_data(
    Itemised_data_usage_for_device_File_Path, Filtered_Data_Output_File_Path, Filter_File_Path)

# Print the total sum of Total_Kbytes
print("Total Sum of Total_Kbytes:", total_sum)
