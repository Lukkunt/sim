HELLO!

This script processes reports from Vodafone IoT portal and returns:
1)suspend_list.csv file - which contains all ICCIDs that did not communicate in all inserted files and are in Active.live state
2)filtered_SIM_Inventory_Full\_\_\_ file - which contains:
a)total_kbytes communicated for of each Itemised_data_usage_for_device report
b)sum of total_kbytes communicated in each Itemised_data_usage_for_device reports for each ICCID,
c)If the sim should be suspended or not
3)new_sim_file - contains Sims which were first time active in last 14 days - these Sims are REMOVED from suspend_list and filtered_SIM_Inventory_Full

To run the script:
1)Open the project repository called: sim
2)Insert SIM_Inventory_Full report to the sim/Data/Source folder - (report should be current)
3)Insert Itemised_data_usage_for_device files to sim/Data/Source folder - (the time range of the imported files determines the time range of output data)
5)Open CMD
6)cd "repository path"
5)venv\Scripts\activate
6)pip install -r requirements.txt
4)python "path to sim/vodafone_sim/**init**.py"
5)If Done is returned: The output files were created in sim/Data/Output
