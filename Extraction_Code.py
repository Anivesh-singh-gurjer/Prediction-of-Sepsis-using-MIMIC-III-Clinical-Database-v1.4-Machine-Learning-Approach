
import pandas as pd

# Load the CSV file
csv_file = '/home/anivesh/CSV/CHARTEVENTS.csv'
df = pd.read_csv(csv_file)

# Filter rows where ITEMID is 12203, 12234, 15531, or 19902
filtered_df = df[df['ITEMID'].isin([1536, 1538, 1539, 227443, 6, 2566, 1542, 3083, 1529, 228368, 223761, 223762, 220179, 3603, 220181, 51222, 535, 51221, 1530, 220180, 225309, 225310, 543, 225312, 51237, 6701, 6702, 220210, 51, 52, 50803, 224828, 51265, 224322, 580, 581, 220228, 3655, 51274, 51275, 74, 51279, 223834, 223835, 4193, 4194, 4195, 51300, 227429, 4197, 615, 1127, 4200, 618, 51301, 1126, 4196, 50802, 113, 626, 227442, 116, 220277, 50806, 50804, 50808, 50809, 227444, 50811, 50810, 50813, 227456, 227457, 50818, 50820, 50821, 646, 50822, 227464, 50824, 1162, 227466, 3724, 3725, 654, 3726, 3727, 227467, 4753, 227468, 3736, 3737, 3740, 160, 3744, 3746, 3747, 676, 224421, 224422, 678, 3750, 681, 677, 679, 684, 50861, 50862, 8368, 3761, 220339, 3766, 190, 50878, 194, 50883, 50882, 50885, 198, 3784, 50889, 3785, 50893, 3789, 226512, 3792, 211, 3796, 50902, 727, 3799, 3801, 3802, 3803, 1671, 3807, 50912, 226534, 226536, 226537, 226540, 50931, 8440, 8441, 3834, 3835, 3836, 3837, 3838, 3839, 228096, 8448, 770, 769, 772, 768, 1286, 2311, 51464, 3337, 777, 779, 780, 781, 778, 776, 50960, 786, 787, 788, 791, 1817, 50971, 50976, 228640, 803, 50983, 807, 811, 813, 814, 815, 816, 821, 30006, 823, 824, 825, 51002, 51003, 828, 829, 51006, 834, 837, 848, 228177, 849, 851, 1366, 225624, 225625, 220507, 1372, 3420, 861, 227686, 8555, 225651, 224639, 225664, 220545, 220546, 225667, 225668, 51081, 220045, 220050, 226707, 220052, 220051, 225170, 225690, 224167, 226730, 227243, 220074, 224684, 224686, 224687, 224691, 224695, 224696, 224697, 220602, 443, 224700, 445, 444, 442, 448, 450, 455, 456, 220615, 220621, 467, 470, 471, 227287, 220635, 220645, 3050, 491, 492, 490, 1520, 1521, 1522, 1523, 1525, 1527, 505, 506, 1531, 1532, 1533, 1535])]

# Select only the required columns
selected_columns = ['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 'ITEMID', 'CHARTTIME', 'STORETIME', 'VALUENUM']
result_df = filtered_df[selected_columns]

# Save the resulting CSV
result_csv_file = '/home/anivesh/CSV/filtered_CHARTEVENTS.csv'
result_df.to_csv(result_csv_file, index=False)

print("Filtered CSV saved successfully.")

import pandas as pd

# Read the original CSV file
icustays_data_path = '/home/anivesh/CSV/ICUSTAYS.csv'
icustays_df = pd.read_csv(icustays_data_path)

# Select only the columns ICUSTAY_ID and INTIME
selected_columns = ['ICUSTAY_ID', 'INTIME']
icustays_selected_df = icustays_df[selected_columns]

# Save the resulting DataFrame to a new CSV file
output_csv_path = '/home/anivesh/CSV/ICUSTAYS_selected.csv'
icustays_selected_df.to_csv(output_csv_path, index=False)

print(f"Selected ICUSTAY_ID and INTIME columns saved to {output_csv_path}")


import pandas as pd

# Read the ICUSTAYS_selected.csv file
icustays_selected_df = pd.read_csv('/home/anivesh/CSV/ICUSTAYS_selected.csv')

# Read the filtered_CHARTEVENTS.csv file
filtered_chartevents_df = pd.read_csv('/home/anivesh/CSV/filtered_CHARTEVENTS.csv')

# Merge the two DataFrames based on the 'ICUSTAY_ID' column
merged_df = pd.merge(icustays_selected_df, filtered_chartevents_df, on='ICUSTAY_ID', how='inner')

# Save the merged DataFrame to a new CSV file
output_csv_path = '/home/anivesh/CSV/merged_data.csv'
merged_df.to_csv(output_csv_path, index=False)

print(f"Merged data saved to {output_csv_path}")

import pandas as pd

# Read the merged data from the CSV file
merged_data_path = r'/home/anivesh/CSV/merged_data.csv'
merged_data = pd.read_csv(merged_data_path)

# Convert 'CHARTTIME' and 'INTIME' columns to datetime format
merged_data['CHARTTIME'] = pd.to_datetime(merged_data['CHARTTIME'])
merged_data['INTIME'] = pd.to_datetime(merged_data['INTIME'])

# Calculate the hours elapsed since admission for each row
merged_data['hours_since_admission'] = (merged_data['CHARTTIME'] - merged_data['INTIME']).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
merged_data['chart_interval'] = merged_data['hours_since_admission'] // 1

# Save the modified DataFrame to a new CSV file
output_csv_path = r'/home/anivesh/CSV_1/CHARTEVENTS_V0.csv'
merged_data.to_csv(output_csv_path, index=False)

print(f"Modified merged data saved to {output_csv_path}")

import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'/home/anivesh/CSV_1/CHARTEVENTS_V0.csv'
ce_v2_df = pd.read_csv(ce_v2_path)

# Drop unnecessary columns
columns_to_drop = ['HADM_ID','CHARTTIME','STORETIME','admission_time','hours_since_admission']
ce_v2_df = ce_v2_df.drop(columns_to_drop, axis=1)

ce_v2_df = ce_v2_df.rename(columns={'ICUSTAY_ID': 'Patient_ID'})
ce_v2_df = ce_v2_df.rename(columns={'chart_interval': 'Hour'})

average_output_path = r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv'
ce_v2_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'ITEMID', 'VALUENUM'])

import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv'
ce_v2_df = pd.read_csv(ce_v2_path)

# Filter rows where itemid is 211 or 220045
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([211, 220045])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'HR'})

# Filter out rows with non-numeric 'HR' values
filtered_df = filtered_df[filtered_df['HR'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'HR' column to numeric
filtered_df['HR'] = pd.to_numeric(filtered_df['HR'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'HR' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['HR'].mean()

# Save the resulting DataFrame to a new CSV file
average_output_path = r'/home/anivesh/CSV_1/HR.csv'
average_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'HR'])

print(f"New CSV file 'average_hr.csv' saved at {average_output_path}")


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([220277])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'O2Sat'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['O2Sat'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['O2Sat'] = pd.to_numeric(filtered_df['O2Sat'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['O2Sat'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/O2Sat.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/HR.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/O2Sat.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_1.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# List of desired item_ids
desired_item_ids = [223762, 676, 677, 678, 679, 223761]

# Filter DataFrame based on desired_item_ids
desired_items_df = ce_v2_df[ce_v2_df['ITEMID'].isin(desired_item_ids)]

# Conversion function from Fahrenheit to Celsius
def fahrenheit_to_celsius(f):
    return (f - 32) * 5/9

# Apply the conversion only to rows where 'itemid' is associated with Fahrenheit
desired_items_df.loc[desired_items_df['ITEMID'].isin([678, 679, 223761]), 'VALUENUM'] = desired_items_df['VALUENUM'].apply(fahrenheit_to_celsius)

# Save the modified DataFrame to a new CSV file
desired_items_df.to_csv(r'/home/anivesh/CSV_1/Temp_Celsius.csv', index=False)


# Read your Temp_CE.csv file and manipulate the data as per your requirements
temp_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
temp_df = temp_df.drop(columns_to_drop, axis=1)

# Rename columns
temp_df = temp_df.rename(columns={'VALUENUM': 'Temp'})

# Filter out rows with non-numeric 'Temp' values
temp_df = temp_df[temp_df['Temp'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'Temp' column to numeric
temp_df['Temp'] = pd.to_numeric(temp_df['Temp'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'Temp' for each group
average_temp_df = temp_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Temp'].mean()

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_1.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Temp_Celsius.csv')

# Merge the DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')
merged_df = pd.merge(merged_df, average_temp_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_2.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([51, 442, 455, 6701, 220179, 220050])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'SBP'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['SBP'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['SBP'] = pd.to_numeric(filtered_df['SBP'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['SBP'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/SBP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_2.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/SBP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Remove duplicates from the merged DataFrame
merged_df.drop_duplicates(inplace=True)

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_3.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([456, 52, 6702, 443, 220052, 220181, 225312])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'MAP'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['MAP'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['MAP'] = pd.to_numeric(filtered_df['MAP'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['MAP'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/MAP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_3.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/MAP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Remove duplicates from the merged DataFrame
merged_df.drop_duplicates(inplace=True)

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_4.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([8368, 8440, 8441, 8555, 220180, 220051])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'DBP'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['DBP'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['DBP'] = pd.to_numeric(filtered_df['DBP'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['DBP'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/DBP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_4.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/DBP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_5.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([780, 1126, 3839, 4753, 50820])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'PH'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['PH'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['PH'] = pd.to_numeric(filtered_df['PH'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['PH'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/PH.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_5.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/PH.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_6.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([227443,	50882,50803])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'HCO3'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['HCO3'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['HCO3'] = pd.to_numeric(filtered_df['HCO3'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['HCO3'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/HCO3.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_6.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/HCO3.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_7.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([791, 3750, 1525, 220615])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Creatinine'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Creatinine'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Creatinine'] = pd.to_numeric(filtered_df['Creatinine'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Creatinine'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Creatinine.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_7.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Creatinine.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_8.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([821,1532,220635,50960])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Magnesium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Magnesium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Magnesium'] = pd.to_numeric(filtered_df['Magnesium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Magnesium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Magnesium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_8.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Magnesium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_9.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([861, 1127, 1542, 220546])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'WBC'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['WBC'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['WBC'] = pd.to_numeric(filtered_df['WBC'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['WBC'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/WBC.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_9.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/WBC.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_10.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([781, 1162, 225624, 220546])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'BUN'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['BUN'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['BUN'] = pd.to_numeric(filtered_df['BUN'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['BUN'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/BUN.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_10.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/BUN.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_11.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([814,220228,50811,51222])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Hgb'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Hgb'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Hgb'] = pd.to_numeric(filtered_df['Hgb'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Hgb'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Hgb.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_11.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Hgb.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_12.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([813,220545,3761,226540,51221,50810])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Hct'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Hct'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Hct'] = pd.to_numeric(filtered_df['Hct'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Hct'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Hct.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_12.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Hct.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_13.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([829,1535,227442,227464,3792,50971,50822])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Potassium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Potassium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Potassium'] = pd.to_numeric(filtered_df['Potassium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Potassium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Potassium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_13.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Potassium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_14.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium'])

import os

# Define the file paths
old_file_paths = [
    '/home/anivesh/CSV_1/Merge_13.csv'
]

new_file_names = [
    'Merge_12_5.csv'
]

# Rename the files
for old_path, new_name in zip(old_file_paths, new_file_names):
    new_path = os.path.join(os.path.dirname(old_path), new_name)
    os.rename(old_path, new_path)

print("Files renamed successfully.")

import os

# Define the file paths
old_file_paths = [
    '/home/anivesh/CSV_1/Merge_14.csv'
]

new_file_names = [
    'Merge_13.csv'
]

# Rename the files
for old_path, new_name in zip(old_file_paths, new_file_names):
    new_path = os.path.join(os.path.dirname(old_path), new_name)
    os.rename(old_path, new_path)

print("Files renamed successfully.")


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([837,220645,4194,3725,3803,226534,1536,4195,3726,50983,50824])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Sodium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Sodium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Sodium'] = pd.to_numeric(filtered_df['Sodium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Sodium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Sodium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_13.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Sodium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_14.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([225664,807,811,1529,220621,226537,3744,50809,50931])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Glucose'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Glucose'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Glucose'] = pd.to_numeric(filtered_df['Glucose'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Glucose'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Glucose.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_14.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Glucose.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_15.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([227468])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Fibrinogen'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Fibrinogen'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Fibrinogen'] = pd.to_numeric(filtered_df['Fibrinogen'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Fibrinogen'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Fibrinogen.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_15.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Fibrinogen.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_16.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([837,220645,4194,3725,3803,226534,1536,4195,3726,50983,50824])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Sodium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Sodium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Sodium'] = pd.to_numeric(filtered_df['Sodium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Sodium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Sodium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_13.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Sodium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_14.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([225664,807,811,1529,220621,226537,3744,50809,50931])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Glucose'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Glucose'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Glucose'] = pd.to_numeric(filtered_df['Glucose'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Glucose'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Glucose.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_14.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Glucose.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_15.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([227468])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Fibrinogen'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Fibrinogen'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Fibrinogen'] = pd.to_numeric(filtered_df['Fibrinogen'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Fibrinogen'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Fibrinogen.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_15.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Fibrinogen.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_16.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([788,220602,1523,4193,3724,226536,3747,50902,50806])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Chloride'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Chloride'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Chloride'] = pd.to_numeric(filtered_df['Chloride'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Chloride'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Chloride.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_16.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Chloride.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_17.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([786,225625,1522,3746,50893])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Calcium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Calcium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Calcium'] = pd.to_numeric(filtered_df['Calcium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Calcium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_17.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_18.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([816,225667,3766,50808])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Ion_Calcium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Ion_Calcium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Ion_Calcium'] = pd.to_numeric(filtered_df['Ion_Calcium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Ion_Calcium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Ion_Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_18.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Ion_Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_19.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([788,220602,1523,4193,3724,226536,3747,50902,50806])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Chloride'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Chloride'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Chloride'] = pd.to_numeric(filtered_df['Chloride'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Chloride'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Chloride.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_16.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Chloride.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_17.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([786,225625,1522,3746,50893])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Calcium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Calcium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Calcium'] = pd.to_numeric(filtered_df['Calcium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Calcium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_17.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_18.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([816,225667,3766,50808])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Ion_Calcium'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Ion_Calcium'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Ion_Calcium'] = pd.to_numeric(filtered_df['Ion_Calcium'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Ion_Calcium'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Ion_Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_18.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Ion_Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_19.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium'])




import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([803,1527,225651,50883])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Bilirubin_direct'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Bilirubin_direct'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Bilirubin_direct'] = pd.to_numeric(filtered_df['Bilirubin_direct'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Bilirubin_direct'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Bilirubin_direct.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_19.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Bilirubin_direct.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_20.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([227429,851,51002,51003])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Troponin'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Troponin'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Troponin'] = pd.to_numeric(filtered_df['Troponin'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Troponin'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Troponin.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_20.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Troponin.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_21.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([227429,851,51002,51003])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'CRP'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['CRP'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['CRP'] = pd.to_numeric(filtered_df['CRP'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['CRP'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/CRP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_21.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/CRP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_22.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([825,1533,227466,3796,51275])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'PTT'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['PTT'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['PTT'] = pd.to_numeric(filtered_df['PTT'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['PTT'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/PTT.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_22.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/PTT.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_23.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([815,1530,227467,51237])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'INR'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['INR'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['INR'] = pd.to_numeric(filtered_df['INR'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['INR'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/INR.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_23.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/INR.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_24.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([776,224828,3736,4196,3740,74,50802])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Arterial_BE'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Arterial_BE'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Arterial_BE'] = pd.to_numeric(filtered_df['Arterial_BE'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Arterial_BE'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Arterial_BE.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_24.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Arterial_BE.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_25.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE'])



import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([225668,1531,50813])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Arterial_lactate'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Arterial_lactate'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Arterial_lactate'] = pd.to_numeric(filtered_df['Arterial_lactate'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Arterial_lactate'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Arterial_lactate.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_25.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Arterial_lactate.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_26.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate'])

import pandas as pd

# Read the CSV file
inputevents_df = pd.read_csv('/home/anivesh/CSV/INPUTEVENTS_MV.csv')

# Extract SUBJECT_ID and PATIENTWEIGHT
selected_columns = ['ICUSTAY_ID', 'PATIENTWEIGHT']
extracted_data = inputevents_df[selected_columns]

# Rename 'SUBJECT_ID' to 'Patient_ID'
extracted_data = extracted_data.rename(columns={'ICUSTAY_ID': 'Patient_ID'})

# Extract unique rows based on 'Patient_ID'
unique_patient_data = extracted_data.drop_duplicates(subset='Patient_ID')

# Save the result to a new CSV file
unique_patient_data.to_csv('/home/anivesh/CSV_1/Weight.csv', index=False)

# Read the CSV file
inputevents_mv_df = pd.read_csv('/home/anivesh/CSV/INPUTEVENTS_MV.csv')

# Extract SUBJECT_ID, STARTTIME, ITEMID, AMOUNT
selected_columns = ['ICUSTAY_ID', 'STARTTIME', 'ITEMID', 'AMOUNT']
extracted_data = inputevents_mv_df[selected_columns]

# Rename 'SUBJECT_ID' to 'Patient_ID'
extracted_data = extracted_data.rename(columns={'ICUSTAY_ID': 'Patient_ID'})

# Save the result to a new CSV file
extracted_data.to_csv('/home/anivesh/CSV_1/Extracted_MV.csv', index=False)

import pandas as pd

# Read the CSV files
merged_26_df = pd.read_csv('/home/anivesh/CSV_1/Merge_26.csv')
patient_weight_v2_df = pd.read_csv('/home/anivesh/CSV_1/Weight.csv')

# Merge DataFrames based on 'Patient_ID', using left join
merged_data = pd.merge(merged_26_df, patient_weight_v2_df, on='Patient_ID', how='left')

# Save the result to a new CSV file
merged_data.to_csv('/home/anivesh/CSV_1/Merge_27.csv', index=False)

import pandas as pd

# Assuming your DataFrame is named 'modified_cv1_df'
modified_cv1_df = pd.read_csv('/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Extract Patient_ID and admission_time
selected_columns = ['ICUSTAY_ID', 'admission_time']
patient_admission_data = modified_cv1_df[selected_columns].drop_duplicates()

# Save the result to a new CSV file
patient_admission_data.to_csv('/home/anivesh/CSV_1/Admission.csv', index=False)

import pandas as pd

# Read the merged data from the CSV file
merged_data_path = r'/home/anivesh/CSV_1/Extracted_MV_1.csv'
merged_data = pd.read_csv(merged_data_path)

# Convert 'STARTTIME' column to datetime format
merged_data['STARTTIME'] = pd.to_datetime(merged_data['STARTTIME'])

# Calculate the hours elapsed since admission for each row
merged_data['hours_since_admission'] = (merged_data['STARTTIME'] - pd.to_datetime(merged_data['INTIME'])).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
merged_data['chart_interval'] = merged_data['hours_since_admission'] // 1

# Save the modified DataFrame to a new CSV file
output_csv_path = r'/home/anivesh/CSV_1/Extracted_MV_2.csv'
merged_data.to_csv(output_csv_path, index=False)

print(f"Modified merged data saved to {output_csv_path}")


import pandas as pd

# File paths
file_path_cv1 = r'/home/anivesh/CSV_1/Extracted_MV_2.csv'


# Columns to extract
columns_to_extract = ['Patient_ID', 'ITEMID', 'AMOUNT', 'chart_interval']

# Read and extract columns from Modified_CV1.csv
cv1_df = pd.read_csv(file_path_cv1, usecols=columns_to_extract)


# Save the extracted data to new CSV files
cv1_df.to_csv(r'/home/anivesh/CSV_1/Extracted_MV_3.csv', index=False)

print("Columns extracted and saved successfully.")


import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'/home/anivesh/CSV_1/Extracted_MV_3.csv'
ce_v2_df = pd.read_csv(ce_v2_path)

# Filter rows where itemid is 211 or 220045
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([225170])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'AMOUNT': 'Platelets'})
filtered_df = filtered_df.rename(columns={'chart_interval': 'Hour'})

# Filter out rows with non-numeric 'HR' values
filtered_df = filtered_df[filtered_df['Platelets'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'HR' column to numeric
filtered_df['Platelets'] = pd.to_numeric(filtered_df['Platelets'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'HR' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Platelets'].mean()

# Save the resulting DataFrame to a new CSV file
average_output_path = r'/home/anivesh/CSV_1/Platelets.csv'
average_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'Platelets'])

print(f"New CSV file 'Platelets.csv' saved at {average_output_path}")

import pandas as pd

# Read the CSV file
inputevents_mv_df = pd.read_csv('/home/anivesh/CSV/INPUTEVENTS_CV.csv')

# Extract SUBJECT_ID, STARTTIME, ITEMID, AMOUNT
selected_columns = ['ICUSTAY_ID', 'CHARTTIME', 'ITEMID', 'AMOUNT']
extracted_data = inputevents_mv_df[selected_columns]

# Rename 'SUBJECT_ID' to 'Patient_ID'
extracted_data = extracted_data.rename(columns={'ICUSTAY_ID': 'Patient_ID'})

# Save the result to a new CSV file
extracted_data.to_csv('/home/anivesh/CSV/Extracted_CV.csv', index=False)

import pandas as pd

# Read the CSV files
merged_26_df = pd.read_csv('/home/anivesh/CSV/Extracted_CV.csv')
patient_weight_v2_df = pd.read_csv('/home/anivesh/CSV_1/Admission.csv')

patient_weight_v2_df = patient_weight_v2_df.rename(columns={'ICUSTAY_ID': 'Patient_ID'})

# Merge DataFrames based on 'Patient_ID', using left join
merged_data = pd.merge(merged_26_df, patient_weight_v2_df, on='Patient_ID', how='left')

# Save the result to a new CSV file
merged_data.to_csv('/home/anivesh/CSV_1/Extracted_CV_1.csv', index=False)

import pandas as pd

# Read the merged data from the CSV file
merged_data_path = r'/home/anivesh/CSV_1/Extracted_CV_1.csv'
merged_data = pd.read_csv(merged_data_path)

# Convert 'STARTTIME' column to datetime format
merged_data['CHARTTIME'] = pd.to_datetime(merged_data['CHARTTIME'])

# Calculate the hours elapsed since admission for each row
merged_data['hours_since_admission'] = (merged_data['CHARTTIME'] - pd.to_datetime(merged_data['INTIME'])).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
merged_data['chart_interval'] = merged_data['hours_since_admission'] // 1

# Save the modified DataFrame to a new CSV file
output_csv_path = r'/home/anivesh/CSV_1/Extracted_CV_2.csv'
merged_data.to_csv(output_csv_path, index=False)

print(f"Modified merged data saved to {output_csv_path}")

import pandas as pd

# File paths
file_path_cv1 = r'/home/anivesh/CSV_1/Extracted_CV_2.csv'


# Columns to extract
columns_to_extract = ['Patient_ID', 'ITEMID', 'AMOUNT', 'chart_interval']

# Read and extract columns from Modified_CV1.csv
cv1_df = pd.read_csv(file_path_cv1, usecols=columns_to_extract)


# Save the extracted data to new CSV files
cv1_df.to_csv(r'/home/anivesh/CSV_1/Extracted_CV_3.csv', index=False)

print("Columns extracted and saved successfully.")


import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'/home/anivesh/CSV_1/Extracted_CV_3.csv'
ce_v2_df = pd.read_csv(ce_v2_path)

# Filter rows where itemid is 211 or 220045
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([30006])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'AMOUNT': 'Platelets'})
filtered_df = filtered_df.rename(columns={'chart_interval': 'Hour'})

# Filter out rows with non-numeric 'HR' values
filtered_df = filtered_df[filtered_df['Platelets'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'HR' column to numeric
filtered_df['Platelets'] = pd.to_numeric(filtered_df['Platelets'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'HR' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Platelets'].mean()

# Save the resulting DataFrame to a new CSV file
average_output_path = r'/home/anivesh/CSV_1/Platelets_1.csv'
average_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'Platelets'])

print(f"New CSV file 'Platelets.csv' saved at {average_output_path}")


import pandas as pd

# File paths
file_path_cv1 = r'/home/anivesh/CSV_1/Platelets.csv'
file_path_cv2 = r'/home/anivesh/CSV_1/Platelets_1.csv'

# Read Platelets_1.csv and Platelets_2.csv
cv1_df = pd.read_csv(file_path_cv1)
cv2_df = pd.read_csv(file_path_cv2)

# Merge the dataframes
merged_df = pd.concat([cv1_df, cv2_df], ignore_index=True)

# Filter out rows with negative 'Hour' values
merged_df = merged_df[merged_df['Hour'] >= 0]

# Save the merged data to a new CSV file
output_path = r'/home/anivesh/CSV_1/Platelets.csv'
merged_df.to_csv(output_path, index=False)

print(f"Merged data saved at {output_path}")

import pandas as pd

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_27.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Platelets.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_28.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([1817,228640])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'ETCO2'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['ETCO2'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['ETCO2'] = pd.to_numeric(filtered_df['ETCO2'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['ETCO2'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/ETCO2.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_28.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/ETCO2.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_29.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([778,3784,3836,3835,50818])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'paCO2'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['paCO2'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['paCO2'] = pd.to_numeric(filtered_df['paCO2'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['paCO2'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/paCO2.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_29.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/paCO2.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_30.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([779,490,3785,3838,3837,50821])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'paO2'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['paO2'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['paO2'] = pd.to_numeric(filtered_df['paO2'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['paO2'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/paO2.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_30.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/paO2.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_31.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([824,1286,51274])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'PT'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['PT'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['PT'] = pd.to_numeric(filtered_df['PT'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['PT'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/PT.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_31.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/PT.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_32.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([4197,3799,51279])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'RBC'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['RBC'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['RBC'] = pd.to_numeric(filtered_df['RBC'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['RBC'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/RBC.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_32.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/RBC.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_33.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([626])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'SVR'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['SVR'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['SVR'] = pd.to_numeric(filtered_df['SVR'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['SVR'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/SVR.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_33.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/SVR.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_34.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([198])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'GCS'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['GCS'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['GCS'] = pd.to_numeric(filtered_df['GCS'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['GCS'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/GCS.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_34.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/GCS.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_35.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([220179, 225309, 6701, 6, 227243, 224167, 51, 455])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'SysBP'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['SysBP'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['SysBP'] = pd.to_numeric(filtered_df['SysBP'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['SysBP'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/SysBP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_35.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/SysBP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_36.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS', 'SysBP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([1538,848,225690	,51464,	50885])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'Bilirubin_Total'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['Bilirubin_Total'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['Bilirubin_Total'] = pd.to_numeric(filtered_df['Bilirubin_Total'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['Bilirubin_Total'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/Bilirubin_Total.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_36.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/Bilirubin_Total.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_37.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS', 'SysBP', 'Bilirubin_Total'])




import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'/home/anivesh/CSV_1/CHARTEVENTS_V1.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([220277,646,834])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'SpO2'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['SpO2'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['SpO2'] = pd.to_numeric(filtered_df['SpO2'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['SpO2'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'/home/anivesh/CSV_1/SpO2.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'/home/anivesh/CSV_1/Merge_37.csv')
o2sat_df = pd.read_csv(r'/home/anivesh/CSV_1/SpO2.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'/home/anivesh/CSV_1/Merge_38.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS', 'SysBP', 'Bilirubin_Total', 'SpO2'])

import pandas as pd

# Read the ADMISSIONS.csv file
admissions_df = pd.read_csv('/home/anivesh/CSV/ADMISSIONS.csv')

# Drop rows where 'DIAGNOSIS' column has NaN values
admissions_df = admissions_df.dropna(subset=['DIAGNOSIS'])

# Filter the DataFrame for patients diagnosed with SEPSIS
sepsis_patients = admissions_df[admissions_df['DIAGNOSIS'].str.contains('SEPSIS', case=False)]

# Save the resulting DataFrame to a new CSV file
sepsis_patients.to_csv('/home/anivesh/CSV_1/ADMISSIONS_1.csv', index=False)

import pandas as pd

# Read the modified CSV file without negative intervals
chartevents_df = pd.read_csv('/home/anivesh/CSV_1/ADMISSIONS_1.csv')

# Select specific columns: 'SUBJECT_ID', 'ITEMID', 'AMOUNT'
selected_columns = chartevents_df[['HADM_ID', 'ADMITTIME', 'DIAGNOSIS']]

# Save the selected columns to a new CSV file
selected_columns.to_csv('/home/anivesh/CSV_1/Sepsis.csv', index=False)

import pandas as pd

csv = pd.read_csv('/home/anivesh/CSV/ICUSTAYS.csv')

csv.rename(columns={'ICUSTAY_ID':'Patient_ID'}, inplace=True)

csv = csv[['HADM_ID','Patient_ID']]

csv.to_csv('/home/anivesh/CSV_1/ICUSTAYS_1.csv', index=False)

import pandas as pd

csv1 = pd.read_csv('/home/anivesh/CSV_1/Merge_38.csv')
csv2 = pd.read_csv('/home/anivesh/CSV_1/ICUSTAYS_1.csv')

csv3 = pd.merge(csv1,csv2, how= 'left', on='Patient_ID')

csv3.to_csv('/home/anivesh/CSV_1/Merged_38.csv',index=False)

import pandas as pd

csv2 = pd.read_csv('/home/anivesh/CSV_1/ICUSTAYS_1.csv')
csv1 = pd.read_csv('/home/anivesh/CSV_1/Sepsis.csv')

csv = pd.merge(csv1,csv2, on='HADM_ID',how='left')

csv.to_csv('/home/anivesh/CSV_1/Sepsis_1.csv', index=False)

import pandas as pd

csv = pd.read_csv('/home/anivesh/CSV_1/CHARTEVENTS_V0.csv')

csv.rename(columns={'ICUSTAY_ID': 'Patient_ID'}, inplace=True)

csv = csv[['Patient_ID','INTIME']]

csv.to_csv('/home/anivesh/CSV_1/INTIME.csv', index=False)

csv2 = pd.read_csv('/home/anivesh/CSV_1/INTIME.csv')
csv1 = pd.read_csv('/home/anivesh/CSV_1/Sepsis_1.csv')

csv = pd.merge(csv1,csv2, on='Patient_ID',how='inner')

csv.to_csv('/home/anivesh/CSV_1/Sepsis_2.csv', index=False)

import pandas as pd

# Read the DataFrame from the provided CSV file
chartevents_df = pd.read_csv('/home/anivesh/CSV_1/Sepsis_2.csv')

chartevents_df.drop_duplicates(inplace=True)

# Convert 'admittime' and 'admission_time' columns to datetime format
chartevents_df['ADMITTIME'] = pd.to_datetime(chartevents_df['ADMITTIME'])
chartevents_df['INTIME'] = pd.to_datetime(chartevents_df['INTIME'])

# Calculate the hours elapsed since admission for each row
chartevents_df['hours_since_admission'] = (chartevents_df['ADMITTIME'] - chartevents_df['INTIME']).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
chartevents_df['chart_interval'] = chartevents_df['hours_since_admission'].apply(lambda x: 1 if x < 0 else x // 1 + 1)

chartevents_df = chartevents_df[['Patient_ID', 'DIAGNOSIS', 'chart_interval']]
chartevents_df.rename(columns={'chart_interval': 'Hour'}, inplace=True)

# Fill the 'DIAGNOSIS' column with 1 where patient is diagnosed with sepsis, else NaN or empty string
chartevents_df['DIAGNOSIS'] = chartevents_df['DIAGNOSIS'].apply(lambda x: 1 if 'SEPSIS' in str(x).upper() else '')

chartevents_df.rename(columns={'DIAGNOSIS': 'Sepsis_label'}, inplace=True)

# Select specific columns: 'Patient_ID', 'ADMITTIME', 'DIAGNOSIS'
selected_columns = chartevents_df[['Patient_ID', 'Hour', 'Sepsis_label']]

# Save the modified DataFrame to a new CSV file
chartevents_df.to_csv('/home/anivesh/CSV_1/Sepsis_3.csv', index=False)

# Read the first CSV file
data1 = pd.read_csv('/home/anivesh/CSV_1/Merged_38.csv')

# Read the second CSV file
data2 = pd.read_csv('/home/anivesh/CSV_1/Sepsis_3.csv')

# Merge the two DataFrames based on 'Patient_ID'
merged_data = pd.merge(data1, data2, on=['Hour', 'Patient_ID'], how='outer')

# Save the merged data to a new CSV file
merged_data.to_csv('/home/anivesh/CSV_1/Merged_39.csv', index=False)

import pandas as pd

# Read the DataFrame from the provided CSV file
data = pd.read_csv('/home/anivesh/CSV_1/Merged_39.csv')

# Convert 'Hour' column to numeric if it's not already
data['Hour'] = pd.to_numeric(data['Hour'], errors='coerce')

# Find the first occurrence of Sepsis_label = 1 for each Patient_ID
first_sepsis = data[data['Sepsis_label'] == 1].groupby('Patient_ID')['Hour'].min().reset_index()

# Merge the first Sepsis_label = 1 hour with the original data
data = pd.merge(data, first_sepsis, on='Patient_ID', suffixes=('', '_first'), how='left')

# Update Sepsis_label based on the hour comparison
data['Sepsis_label'] = (data['Hour'] >= data['Hour_first']).astype(int)

# Drop the intermediate 'Hour_first' column
data.drop(columns='Hour_first', inplace=True)

# Save the modified DataFrame to a new CSV file
data.to_csv('/home/anivesh/CSV_1/Merged_40.csv', index=False)

csv = pd.read_csv('/home/anivesh/CSV_1/CHARTEVENTS_V0.csv')

csv.rename(columns={'ICUSTAY_ID': 'Patient_ID'}, inplace=True)

csv = csv[['Patient_ID','SUBJECT_ID']]

csv.drop_duplicates(inplace=True)

csv.to_csv('/home/anivesh/CSV_1/SUBJECT_ID.csv', index=False)

import pandas as pd

csv1 = pd.read_csv('/home/anivesh/CSV_1/Merged_40.csv')
csv2 = pd.read_csv('/home/anivesh/CSV_1/SUBJECT_ID.csv')

csv = pd.merge(csv1,csv2, on='Patient_ID',how='inner')

csv.to_csv('/home/anivesh/CSV_1/Merged_41.csv',index=False)

import pandas as pd

# Read the CSV file
csv_file_path = '/home/anivesh/CSV/PATIENTS.csv'  # Replace with your CSV file path
data = pd.read_csv(csv_file_path)

# Extract 'Patient_ID' and 'gender' columns
data = data[['SUBJECT_ID', 'GENDER']]

# Convert gender to numeric (assuming Male = 0, Female = 1)
data['GENDER'] = data['GENDER'].map({'M': 0, 'F': 1})

# Save the updated DataFrame to a new CSV file
data.to_csv('/home/anivesh/CSV_1/Gender.csv', index=False)



import pandas as pd

# Read the extracted CSV file with Patient_ID and gender
extracted_data = pd.read_csv('/home/anivesh/CSV_1/Gender.csv')

# Read the other DataFrame (replace 'other_data.csv' with your file name)
other_data = pd.read_csv('/home/anivesh/CSV_1/Merged_41.csv')

# Merge the two DataFrames on 'Patient_ID'
merged_data = pd.merge(extracted_data, other_data, on='SUBJECT_ID', how='inner')

# Save the merged DataFrame to a new CSV file
merged_data.to_csv('/home/anivesh/CSV_1/Merged_42.csv', index=False)

import pandas as pd

# Path to the files
patients_file = "/home/anivesh/CSV/PATIENTS.csv"
merged_file = "/home/anivesh/CSV_1/Merged_42.csv"

# Read the PATIENTS.csv file and select only the 'SUBJECT_ID' and 'DOB' columns
patients_df = pd.read_csv(patients_file, usecols=['SUBJECT_ID', 'DOB'])

# Save the extracted data into a new CSV file
patients_df.to_csv("/home/anivesh/CSV_1/DOB.csv", index=False)

# Read the Merged_42.csv file and select unique 'SUBJECT_ID'
merged_df = pd.read_csv(merged_file)
unique_subject_ids = merged_df['SUBJECT_ID'].unique()

# Create a DataFrame with unique SUBJECT_IDs
unique_subject_ids_df = pd.DataFrame({'SUBJECT_ID': unique_subject_ids})

# Save the unique SUBJECT_IDs into a new CSV file
unique_subject_ids_df.to_csv("/home/anivesh/CSV_1/subject_ids.csv", index=False)


import pandas as pd

# Path to the ICUSTAYS.csv file
icustays_file = "/home/anivesh/CSV/ICUSTAYS.csv"

# Read the ICUSTAYS.csv file and select the required columns
icustays_df = pd.read_csv(icustays_file, usecols=['SUBJECT_ID', 'ICUSTAY_ID', 'INTIME'])

# Save the resulting DataFrame into a new CSV file
icustays_df.to_csv("/home/anivesh/CSV/extracted_icustays.csv", index=False)

import pandas as pd

# Path to the extracted_icustays.csv file
icustays_file = "/home/anivesh/CSV/extracted_icustays.csv"
dob_file = "/home/anivesh/CSV_1/DOB.csv"

# Read the CSV files
icustays_df = pd.read_csv(icustays_file)
dob_df = pd.read_csv(dob_file)

# Perform an inner join on 'SUBJECT_ID' column
result_df = pd.merge(icustays_df, dob_df, on='SUBJECT_ID', how='inner')

# Save the resulting DataFrame into a new CSV file
result_df.to_csv("/home/anivesh/CSV_1/merged_icustays_dob.csv", index=False)

import pandas as pd

# Path to the merged_icustays_dob.csv file
merged_file = "/home/anivesh/CSV_1/merged_icustays_dob.csv"

# Read the merged CSV file
merged_df = pd.read_csv(merged_file)

# Convert 'INTIME' and 'DOB' columns to datetime format
merged_df['INTIME'] = pd.to_datetime(merged_df['INTIME'])
merged_df['DOB'] = pd.to_datetime(merged_df['DOB'])

# Calculate age by subtracting DOB from INTIME, and convert to years
merged_df['AGE'] = (merged_df['INTIME'].dt.year - merged_df['DOB'].dt.year)

# Drop 'INTIME' and 'DOB' columns
merged_df.drop(['INTIME', 'DOB'], axis=1, inplace=True)

# Save the resulting DataFrame into a new CSV file
merged_df.to_csv("/home/anivesh/CSV_1/icustay_age.csv", index=False)

import pandas as pd

# Path to the icustay_age.csv file
icustay_age_file = "/home/anivesh/CSV_1/icustay_age.csv"

# Read the icustay_age.csv file
icustay_age_df = pd.read_csv(icustay_age_file)

# Filter rows where age is between 13 and 90 (inclusive)
filtered_df = icustay_age_df[(icustay_age_df['AGE'] >= 13) & (icustay_age_df['AGE'] <= 90)]

# Save the resulting DataFrame into a new CSV file
filtered_df.to_csv("/home/anivesh/CSV_1/icustay_age_filtered.csv", index=False)

import pandas as pd

# Path to the Merged_42.csv file and icustay_age_filtered.csv file
merged_file = "/home/anivesh/CSV_1/Merged_42.csv"
icustay_age_filtered_file = "/home/anivesh/CSV_1/icustay_age_filtered.csv"

# Read the Merged_42.csv file and extract the 'Patient_ID' column
merged_df = pd.read_csv(merged_file)
patient_id_df = merged_df[['Patient_ID']]

# Save the extracted 'Patient_ID' column into a new CSV file
patient_id_df.to_csv("/home/anivesh/CSV_1/patient_id.csv", index=False)

# Read the icustay_age_filtered.csv file
icustay_age_filtered_df = pd.read_csv(icustay_age_filtered_file)

# Rename the 'ICUSTAY_ID' column to 'Patient_ID'
icustay_age_filtered_df.rename(columns={'ICUSTAY_ID': 'Patient_ID'}, inplace=True)

# Save the resulting DataFrame into the same CSV file, overwriting it
icustay_age_filtered_df.to_csv(icustay_age_filtered_file, index=False)

import pandas as pd

# Path to the patient_id.csv file
patient_id_file = "/home/anivesh/CSV_1/patient_id.csv"

# Read the patient_id.csv file
patient_id_df = pd.read_csv(patient_id_file)

# Drop duplicate rows
patient_id_df.drop_duplicates(inplace=True)

# Save the DataFrame without duplicates to the same CSV file, overwriting it
patient_id_df.to_csv(patient_id_file, index=False)


import pandas as pd

# Paths to the CSV files
patient_id_file = "/home/anivesh/CSV_1/patient_id.csv"
icustay_age_filtered_file = "/home/anivesh/CSV_1/icustay_age_filtered.csv"

# Read the CSV files
patient_id_df = pd.read_csv(patient_id_file)
icustay_age_filtered_df = pd.read_csv(icustay_age_filtered_file)

# Perform an inner join on 'Patient_ID' column
result_df = pd.merge(patient_id_df, icustay_age_filtered_df, on='Patient_ID', how='inner')

# Save the resulting DataFrame into a new CSV file
result_df.to_csv("/home/anivesh/CSV_1/joined_data.csv", index=False)


import pandas as pd

# Paths to the CSV files
patient_id_file = "/home/anivesh/CSV_1/joined_data.csv"
icustay_age_filtered_file = "/home/anivesh/CSV_1/Merged_42.csv"

# Read the CSV files
patient_id_df = pd.read_csv(patient_id_file)
icustay_age_filtered_df = pd.read_csv(icustay_age_filtered_file)

# Perform an inner join on 'Patient_ID' column
result_df = pd.merge(patient_id_df, icustay_age_filtered_df, on='Patient_ID', how='inner')

# Save the resulting DataFrame into a new CSV file
result_df.to_csv("/home/anivesh/CSV_1/Merged_43.csv", index=False)

import pandas as pd

# Path to the Merged_43.csv file
merged_file = "/home/anivesh/CSV_1/Merged_43.csv"

# Read the Merged_43.csv file
merged_df = pd.read_csv(merged_file)

# Drop the column 'SUBJECT_ID_y'
merged_df.drop(columns=['SUBJECT_ID_y'], inplace=True)

# Rename the column 'SUBJECT_ID_x' to 'SUBJECT_ID'
merged_df.rename(columns={'SUBJECT_ID_x': 'SUBJECT_ID'}, inplace=True)

# Save the modified DataFrame back to the CSV file
merged_df.to_csv(merged_file, index=False)

import pandas as pd

# Read the extracted CSV file with Patient_ID and gender
extracted_data = pd.read_csv('/home/anivesh/CSV/ICUSTAYS.csv')
csv = pd.read_csv('/home/anivesh/CSV_1/Merged_43.csv')

extracted_data = extracted_data[['ICUSTAY_ID','LOS']]

extracted_data 
extracted_data = extracted_data.rename(columns={'ICUSTAY_ID': 'Patient_ID'})

csv1 = pd.merge(csv,extracted_data, on='Patient_ID', how='left')

csv1.to_csv('/home/anivesh/CSV_1/Merged_44.csv', index=False)

import pandas as pd

# Read ICUSTAYS.csv and filter for FIRST_CAREUNIT = MICU or SICU
icu_stays = pd.read_csv('/home/anivesh/CSV/ICUSTAYS.csv', usecols=['ICUSTAY_ID', 'FIRST_CAREUNIT'])
icu_stays_filtered = icu_stays[icu_stays['FIRST_CAREUNIT'].isin(['MICU', 'SICU'])]

# Change FIRST_CAREUNIT values as per the condition
icu_stays_filtered['FIRST_CAREUNIT'] = icu_stays_filtered['FIRST_CAREUNIT'].map({'MICU': 0, 'SICU': 1})

# Rename columns
icu_stays_filtered = icu_stays_filtered.rename(columns={'ICUSTAY_ID': 'Patient_ID', 'FIRST_CAREUNIT': 'ICU'})

# Read Merged_44.csv
merged_44 = pd.read_csv('/home/anivesh/CSV_1/Merged_44.csv')

# Merge on Patient_ID
merged_data = pd.merge(merged_44, icu_stays_filtered, on='Patient_ID', how='left')

# Print or save the merged data
merged_data.to_csv('/home/anivesh/CSV_1/Merged_45.csv',index=False)

import pandas as pd

# Read the Merged_45.csv file
merged_data = pd.read_csv('/home/anivesh/CSV_1/Merged_45.csv')

# Group by Patient_ID and find the maximum Hour value for each group
max_hours = merged_data.groupby('Patient_ID')['Hour'].max()

# Filter Patient_IDs based on the condition (max hour > 25)
filtered_patient_ids = max_hours[max_hours > 25].index

# Filter the merged data based on the filtered Patient_IDs
filtered_data = merged_data[merged_data['Patient_ID'].isin(filtered_patient_ids)]

# Print or save the filtered data

filtered_data.to_csv('/home/anivesh/CSV_1/MIMIC3_dataset.csv', index=False)

import pandas as pd

# Read the MIMIC3_dataset.csv file
dataset = pd.read_csv('/home/anivesh/CSV_1/MIMIC3_dataset.csv')

# Group by Patient_ID and find the maximum Hour value for each group
max_hours = dataset.groupby('Patient_ID')['Hour'].max()

# Filter Patient_IDs based on the condition (max hour < 407)
filtered_patient_ids = max_hours[max_hours < 407].index

# Filter the dataset based on the filtered Patient_IDs
filtered_data = dataset[dataset['Patient_ID'].isin(filtered_patient_ids)]

# Print or save the filtered data
filtered_data.to_csv('/home/anivesh/CSV_1/MIMIC3_dataset_V1.csv', index=False)

import pandas as pd

# Read the MIMIC3_dataset_V1.csv file
dataset = pd.read_csv('/home/anivesh/CSV_1/MIMIC3_dataset_V1.csv')

# 1. Remove all the rows with Hour < 1
dataset = dataset[dataset['Hour'] >= 0]

# 2. Remove all the Patient_ID's where Age < 13
# 3. Remove all the Patient_ID's where Age > 90
# 4. If the Age is NULL, then don't perform above Age related operation
dataset = dataset[~((dataset['Age'].notnull()) & ((dataset['Age'] < 13) | (dataset['Age'] > 90)))]

# Print or save the filtered data
dataset.to_csv('/home/anivesh/CSV_1/MIMIC3_dataset_V2.csv', index=False)

import pandas as pd

# Read the original CSV file
original_df = pd.read_csv("/home/anivesh/CSV/ICUSTAYS.csv")

# Extract the required columns
extracted_df = original_df[['SUBJECT_ID', 'ICUSTAY_ID', 'INTIME']]

# Save the extracted data into a new CSV file
extracted_df.to_csv("/home/anivesh/CSV_2/extracted_data.csv", index=False)

print("Extraction and saving completed successfully.")



import pandas as pd

# Read the extracted data CSV file
extracted_df = pd.read_csv("/home/anivesh/CSV_2/extracted_data.csv")

# Convert INTIME column to datetime format
extracted_df['INTIME'] = pd.to_datetime(extracted_df['INTIME'])

# Sort the dataframe by SUBJECT_ID and INTIME
extracted_df.sort_values(by=['SUBJECT_ID', 'INTIME'], inplace=True)

# Group by SUBJECT_ID and assign N values based on the order of INTIME
extracted_df['N'] = extracted_df.groupby('SUBJECT_ID').cumcount() + 1

# Save the updated data into the same CSV file
extracted_df.to_csv("/home/anivesh/CSV_2/extracted_data_V1.csv", index=False)

print("New column 'N' added successfully.")

import pandas as pd

# Read the extracted data CSV file
extracted_df = pd.read_csv("/home/anivesh/CSV_2/extracted_data_V1.csv")

# Extract the required columns
extracted_columns_df = extracted_df[['ICUSTAY_ID', 'N']]

# Save the extracted data into a new CSV file
extracted_columns_df.to_csv("/home/anivesh/CSV_2/extracted_data_V2.csv", index=False)

print("Extraction completed successfully.")

import pandas as pd

# Read the CSV files
extracted_data_df = pd.read_csv("/home/anivesh/CSV_2/extracted_data_V2.csv")
mimic3_df = pd.read_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL.csv")

# Rename the column 'SUBJECT_ID_x' to 'SUBJECT_ID'
extracted_data_df.rename(columns={'ICUSTAY_ID': 'Patient_ID'}, inplace=True)

# Merge the dataframes based on a common column, if available, or simply concatenate them
merged_df = pd.merge(extracted_data_df, mimic3_df, how='inner', on='Patient_ID')  # Change 'on' to the common column if available, else remove it

# Save the merged data into a new CSV file
merged_df.to_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL_V1.csv", index=False)

print("Merge completed successfully.")

import pandas as pd

# Read the CSV file
data_df = pd.read_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL_V1.csv")

# Sort the dataframe by SUBJECT_ID, N, and Hour
data_df.sort_values(by=['SUBJECT_ID', 'N', 'Hour'], inplace=True)

# Initialize T_Hour column with 0
data_df['T_Hour'] = 0

# Iterate through each SUBJECT_ID
for subject_id in data_df['SUBJECT_ID'].unique():
    # Filter rows for the current SUBJECT_ID
    subject_df = data_df[data_df['SUBJECT_ID'] == subject_id]
    
    # Iterate through each row of the filtered dataframe
    for index, row in subject_df.iterrows():
        # Find the maximum T_Hour for previous N for the current SUBJECT_ID
        max_t_hour_previous_n = subject_df[subject_df['N'] == row['N'] - 1]['T_Hour'].max()
        
        # If max_t_hour_previous_n is NaN (for N=1), set it to 0
        if pd.isna(max_t_hour_previous_n):
            max_t_hour_previous_n = 0
        
        # Calculate T_Hour based on the maximum T_Hour of previous N
        data_df.loc[index, 'T_Hour'] = row['Hour'] + max_t_hour_previous_n

# Save the modified dataframe to a new CSV file
data_df.to_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL_V2.csv", index=False)

print("T_Hour calculation completed successfully.")

import pandas as pd

# Read the original CSV file
original_df = pd.read_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL_V2.csv")

# Extract the required columns
extracted_df = original_df[['SUBJECT_ID', 'N', 'Hour' , 'T_Hour']] 

# Save the extracted data into a new CSV file
extracted_df.to_csv("/home/anivesh/CSV_2/extracted_data_V0.csv", index=False)

print("Extraction and saving completed successfully.")

import pandas as pd

# Read the CSV file
data_df = pd.read_csv("/home/anivesh/CSV_2/extracted_data_V0.csv")

# Sort the dataframe by SUBJECT_ID, N, and Hour
data_df.sort_values(by=['SUBJECT_ID', 'N', 'Hour'], inplace=True)

# Initialize a dictionary to store the maximum T_Hour for each (SUBJECT_ID, N) group
max_t_hour_dict = {}

# Initialize T_Hour column with 0
data_df['T_Hour'] = 0

# Iterate through each row
for index, row in data_df.iterrows():
    # Calculate the maximum T_Hour for previous N for the current row's SUBJECT_ID
    max_t_hour_previous_n = max_t_hour_dict.get((row['SUBJECT_ID'], row['N'] - 1), 0)
    
    # Calculate T_Hour based on the maximum T_Hour of previous N
    data_df.loc[index, 'T_Hour'] = row['Hour'] + max_t_hour_previous_n
    
    # Update max_t_hour_dict with the current row's T_Hour for the current (SUBJECT_ID, N) group
    max_t_hour_dict[(row['SUBJECT_ID'], row['N'])] = data_df.loc[index, 'T_Hour']

# Save the modified dataframe to a new CSV file
data_df.to_csv("/home/anivesh/CSV_2/extracted_data_V5.csv", index=False)

print("T_Hour calculation completed successfully.")

import pandas as pd

# Read the original CSV file
original_df = pd.read_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL_V1.csv")

# Extract the required columns
extracted_df = original_df[['Patient_ID', 'Hour', 'Sepsis_label']]

# Save the extracted data into a new CSV file
extracted_df.to_csv("/home/anivesh/CSV_2/extracted_Sepsis.csv", index=False)

print("Extraction and saving completed successfully.")

import pandas as pd

# Read the CSV files
sepsis_df = pd.read_csv("/home/anivesh/CSV_2/extracted_Sepsis.csv")
data_df = pd.read_csv("/home/anivesh/CSV_2/extracted_data_V5.csv")

# Perform inner join on Patient_ID and Hour columns
merged_df = pd.merge(sepsis_df, data_df, on=['Patient_ID', 'Hour'], how='inner')

# Save the merged data into a new CSV file
merged_df.to_csv("/home/anivesh/CSV_2/merged_data.csv", index=False)

print("Inner join completed successfully.")

import pandas as pd

# Read the original CSV file
original_df = pd.read_csv("/home/anivesh/CSV_2/merged_data.csv")

# Extract the required columns
extracted_df = original_df[['Patient_ID', 'Hour', 'Sepsis_label']]

# Save the extracted data into a new CSV file
extracted_df.to_csv("/home/anivesh/CSV_2/extracted_Sepsis.csv", index=False)

print("Extraction and saving completed successfully.")


import pandas as pd

# Read the merged_data.csv file
merged_df = pd.read_csv("/home/anivesh/CSV_2/merged_data.csv")

# Find the max T_Hour for each SUBJECT_ID
max_t_hour_subject_id = merged_df.groupby('SUBJECT_ID')['T_Hour'].max()

# Initialize lists to store the filtered SUBJECT_IDs
subject_ids_first_24_hours = []
subject_ids_last_24_hours = []

# Iterate through each SUBJECT_ID
for subject_id, max_t_hour in max_t_hour_subject_id.items():
    # Filter rows where Sepsis_label is equal to 0 for max T_Hour - 24
    filter_24_hours_before_max = (merged_df['SUBJECT_ID'] == subject_id) & (merged_df['T_Hour'] >= max_t_hour - 24) & (merged_df['T_Hour'] <= max_t_hour) & (merged_df['Sepsis_label'] == 0)
    sepsis_label_0_last_24_hours = merged_df[filter_24_hours_before_max]

    # Filter rows where Sepsis_label changes to 1 after max T_Hour - 24
    filter_sepsis_label_1_after_max = (merged_df['SUBJECT_ID'] == subject_id) & (merged_df['T_Hour'] > max_t_hour - 24) & (merged_df['Sepsis_label'] == 1)
    sepsis_label_1_after_max = merged_df[filter_sepsis_label_1_after_max]

    # If both conditions are met, add the SUBJECT_ID to the respective list
    if not sepsis_label_0_last_24_hours.empty and not sepsis_label_1_after_max.empty:
        subject_ids_first_24_hours.append(subject_id)
        subject_ids_last_24_hours.append(subject_id)

# Save the unique SUBJECT_IDs for the first 24 hours and last 24 hours to a new CSV file
subject_ids_first_24_hours_df = pd.DataFrame({'SUBJECT_ID': subject_ids_first_24_hours})
subject_ids_last_24_hours_df = pd.DataFrame({'SUBJECT_ID': subject_ids_last_24_hours})

# Save the unique SUBJECT_IDs into new CSV files
subject_ids_first_24_hours_df.to_csv("/home/anivesh/CSV_2/sepsis_subject_ids_first_24_hours.csv", index=False)
subject_ids_last_24_hours_df.to_csv("/home/anivesh/CSV_2/sepsis_subject_ids_last_24_hours.csv", index=False)

# Perform inner join on Patient_ID and Hour columns
merged_df = pd.merge(subject_ids_first_24_hours_df, subject_ids_last_24_hours_df, on=['SUBJECT_ID'], how='outer')

# Save the merged data into a new CSV file
merged_df.to_csv("/home/anivesh/CSV_2/merged_data_V1.csv", index=False)

print("Extraction and saving completed successfully.")


import pandas as pd

# Read the original dataset
original_df = pd.read_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL_V1.csv")

# Drop the 'N' column
original_df = original_df.drop(columns=['N'])

# Read the merged data to get the SUBJECT_IDs to remove
merged_data = pd.read_csv("/home/anivesh/CSV_2/merged_data_V1.csv")
subject_ids_to_remove = merged_data['SUBJECT_ID'].unique()

# Remove rows with SUBJECT_IDs present in the merged data
filtered_df = original_df[~original_df['SUBJECT_ID'].isin(subject_ids_to_remove)]

# Save the filtered data into a new CSV file
filtered_df.to_csv("/home/anivesh/CSV_2/MIMIC3_dataset_FINAL_V2.csv", index=False)

print("Rows with specified SUBJECT_IDs removed and saved successfully.")





