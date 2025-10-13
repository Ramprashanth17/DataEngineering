import pandas as pd
from ydata_profiling import ProfileReport

file_path_1 = "Austin_Crashes_Data.tsv"
file_path_2 = "Chicago_Crashes_Data.tsv"
file_path_3 = "NYC_Crashes_Data.tsv"
file_path_4 = "processed_file.tsv"

df_1 = pd.read_csv(file_path_1, sep='\t')
df_2 = pd.read_csv(file_path_2, sep='\t')
df_3 = pd.read_csv(file_path_3, sep='\t')
df_4 = pd.read_csv(file_path_4, sep='\t')

profile_1 = ProfileReport(df_1, title="Profiling Report")
profile_1.to_file("Y_Data_Profiling_Op/Austin_Profiling.html")

profile_2 = ProfileReport(df_2, title="Profiling Report")
profile_2.to_file("Y_Data_Profiling_Op/Chicago_Profiling.html")

profile_3 = ProfileReport(df_3, title="Profiling Report")
profile_3.to_file("Y_Data_Profiling_Op/NYC_Profiling.html")

profile_4 = ProfileReport(df_4, title="Profiling Report")
profile_4.to_file("Y_Data_Profiling_Op/MGY_Profiling.html")
