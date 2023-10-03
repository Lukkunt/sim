# Following code transforms data dataframe into 2 series fitered by Y on N "Has_Been_Active.Test" value
""""
# creates a series which contains true or false values (TRUE = Has_Been_Active.Test: N)
filt_Y = (data["Has_Been_Active.Test"] == "Y")
filt_N = (data["Has_Been_Active.Test"] == "N")
# deletes columns with other state then (TRUE = Has_Been_Active.Test: N)
data_Y = data[filt_Y]
data_N = data[filt_N]
# prints the Y and N data
print(data_Y)
print(data_N)
"""