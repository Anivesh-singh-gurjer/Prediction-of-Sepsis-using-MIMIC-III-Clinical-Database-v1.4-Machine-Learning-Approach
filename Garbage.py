
import csv

def extract_rows(input_file, output_prefix, allowed_itemids, rows_per_file=1000000):
    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)

        file_number = 1
        record_count = 0

        output_csv = open(f"{output_prefix}_{file_number}.csv", 'w', newline='')
        writer = csv.writer(output_csv)
        writer.writerow(header)

        for row in reader:
            itemid_value = int(row[header.index('ITEMID')])
            if itemid_value in allowed_itemids:
                writer.writerow(row)
                record_count += 1

            if record_count == rows_per_file:
                # Create a new output file when the limit is reached
                file_number += 1
                record_count = 0
                output_csv.close()
                output_csv = open(f"{output_prefix}_{file_number}.csv", 'w', newline='')
                writer = csv.writer(output_csv)
                writer.writerow(header)

        output_csv.close()

# Usage example:
allowed_itemids = [1536, 6, 1542, 3083, 1529, 228368, 223761, 223762, 3603, 220179, 51221, 51222, 535, 220181, 220180, 1530, 543, 225312, 51237, 6701, 6702, 51, 52, 224828, 51265, 580, 581, 220228, 3655, 74, 51274, 51275, 626, 51279, 4193, 4194, 4195, 4196, 227429, 4197, 51300, 4200, 51301, 615, 618, 1127, 113, 227442, 227443, 116, 220277, 227444, 50802, 50803, 50804, 50806, 50808, 50809, 50810, 50811, 50813, 227456, 50818, 50820, 50821, 44166, 50822, 227464, 50824, 227466, 227467, 227468, 646, 654, 1162, 3725, 4753, 3726, 3724, 3736, 3737, 3740, 160, 3744, 3746, 3747, 676, 677, 3750, 678, 679, 681, 684, 50861, 50862, 8368, 3761, 3766, 50878, 50882, 50883, 50885, 198, 3784, 3785, 50889, 3789, 50893, 226512, 3792, 211, 3796, 50902, 3799, 727, 3801, 3802, 3803, 226534, 226536, 226537, 226540, 50931, 8440, 8441, 3834, 3835, 3836, 3837, 3838, 3839, 228096, 3840, 769, 770, 771, 772, 2311, 51464, 3337, 776, 777, 781, 50960, 786, 787, 788, 791, 1817, 50971, 228640, 50976, 803, 50983, 807, 811, 813, 814, 815, 816, 821, 30006, 823, 824, 825, 51002, 51003, 828, 829, 51006, 834, 836, 837, 839, 848, 228177, 849, 851, 225624, 225625, 3420, 861, 227686, 8555, 225651, 225664, 220545, 220546, 225667, 225668, 51081, 220045, 220050, 226707, 220052, 220051, 225170, 226730, 227243, 442, 443, 444, 445, 220602, 448, 450, 455, 456, 220615, 220621, 467, 470, 227287, 471, 220635, 481, 220645, 490, 491, 492, 1522, 1523, 1525, 1527, 505, 506, 1531, 1532, 1533, 1535]
extract_rows('CHARTEVENTS.csv', 'filtered_rows_V', allowed_itemids, rows_per_file=1000000)

import pandas as pd
import os
from glob import glob

# Path to the directory containing your CSV files
csv_files_path = r'D:\Python'

# Get a list of all CSV files in the directory starting with 'filtered_rows_V_'
csv_files = glob(os.path.join(csv_files_path, 'filtered_rows_V_*.csv'))

# Columns to extract
columns_to_extract = ['SUBJECT_ID', 'ITEMID', 'CHARTTIME', 'VALUENUM', 'VALUEUOM']

# Loop through each CSV file and extract columns
for csv_file in csv_files:
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Extract the specified columns
    extracted_df = df[columns_to_extract]
    
    # Save the extracted DataFrame to a new CSV file
    output_path = os.path.join(csv_files_path, f'extracted_{os.path.basename(csv_file)}')
    extracted_df.to_csv(output_path, index=False)
    
    print(f"Columns extracted and saved to {output_path}")

import pandas as pd
import glob

# Path to the directory containing your CSV files
files_path = r'D:\Python\extracted_filtered_rows_V_*.csv'

# Get a list of all CSV files in the directory
files = glob.glob(files_path)

# Initialize variables for chunking
rows_per_output = 2000000
output_file_number = 1
rows_count = 0

# Initialize an empty DataFrame to store the merged data
merged_data = pd.DataFrame()

# Loop through each CSV file and append its data to the merged_data DataFrame
for file in files:
    df = pd.read_csv(file)
    merged_data = pd.concat([merged_data, df], ignore_index=True)

    # Check if the merged_data DataFrame has reached the desired number of rows
    rows_count += len(df)
    if rows_count >= rows_per_output:
        # Save the merged data to a new CSV file
        output_file_path = f'D:\Python\MIMIC_CODE\Merged_Extract_{output_file_number}.csv'
        merged_data.to_csv(output_file_path, index=False)
        print(f"File {output_file_path} saved.")
        
        # Reset counters for the next output file
        merged_data = pd.DataFrame()
        rows_count = 0
        output_file_number += 1

# Save any remaining data to the last CSV file
if not merged_data.empty:
    output_file_path = f'D:\Python\MIMIC_CODE\Merged_Extract_{output_file_number}.csv'
    merged_data.to_csv(output_file_path, index=False)
    print(f"File {output_file_path} saved.")

    import pandas as pd
import glob

# Path to the directory containing your CSV files
files_path = r'D:\Python\extracted_filtered_rows_V_*.csv'

# Get a list of all CSV files in the directory
files = glob.glob(files_path)

# Initialize variables for chunking
rows_per_output = 40000000
output_file_number = 1
rows_count = 0

# Initialize an empty DataFrame to store the merged data
merged_data = pd.DataFrame()

# Loop through each CSV file and append its data to the merged_data DataFrame
for file in files:
    df = pd.read_csv(file)
    merged_data = pd.concat([merged_data, df], ignore_index=True)

    # Check if the merged_data DataFrame has reached the desired number of rows
    rows_count += len(df)
    if rows_count >= rows_per_output:
        # Save the merged data to a new CSV file
        output_file_path = f'D:\Python\MIMIC_CODE\Merged_Extract_{output_file_number}.csv'
        merged_data.to_csv(output_file_path, index=False)
        print(f"File {output_file_path} saved.")
        
        # Reset counters for the next output file
        merged_data = pd.DataFrame()
        rows_count = 0
        output_file_number += 1

# Save any remaining data to the last CSV file
if not merged_data.empty:
    output_file_path = f'D:\Python\MIMIC_CODE\Merged_Extract_{output_file_number}.csv'
    merged_data.to_csv(output_file_path, index=False)
    print(f"File {output_file_path} saved.")
 
import pandas as pd

# Read the merged data from the CSV file
merged_data_path = r'D:\Python\MIMIC_CODE\Merged_Extract_2.csv'
merged_data = pd.read_csv(merged_data_path)

# Convert 'CHARTTIME' column to datetime format
merged_data['CHARTTIME'] = pd.to_datetime(merged_data['CHARTTIME'])

# Find the minimum 'CHARTTIME' for each patient as the admission time
min_charttime_by_patient = merged_data.groupby('SUBJECT_ID')['CHARTTIME'].min()

# Merge the minimum CHARTTIME back into the original DataFrame
merged_data = pd.merge(merged_data, min_charttime_by_patient.reset_index(name='admission_time'), on='SUBJECT_ID')

# Calculate the hours elapsed since admission for each row
merged_data['hours_since_admission'] = (merged_data['CHARTTIME'] - merged_data['admission_time']).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
merged_data['chart_interval'] = merged_data['hours_since_admission'] // 1

# Save the modified DataFrame to a new CSV file
output_csv_path = r'D:\Python\MIMIC_CODE\Modified_CV2.csv'
merged_data.to_csv(output_csv_path, index=False)

print(f"Modified merged data saved to {output_csv_path}")

import pandas as pd

# File paths
file_path_cv1 = r'D:\Python\MIMIC_CODE\Modified_CV1.csv'
file_path_cv2 = r'D:\Python\MIMIC_CODE\Modified_CV2.csv'

# Columns to extract
columns_to_extract = ['SUBJECT_ID', 'ITEMID', 'VALUENUM', 'chart_interval']

# Read and extract columns from Modified_CV1.csv
cv1_df = pd.read_csv(file_path_cv1, usecols=columns_to_extract)

# Read and extract columns from Modified_CV2.csv
cv2_df = pd.read_csv(file_path_cv2, usecols=columns_to_extract)

# Save the extracted data to new CSV files
cv1_df.to_csv(r'D:\Python\MIMIC_CODE\Extracted_CV1.csv', index=False)
cv2_df.to_csv(r'D:\Python\MIMIC_CODE\Extracted_CV2.csv', index=False)

print("Columns extracted and saved successfully.")


import pandas as pd

# File paths
file_path_cv1 = r'D:\Python\MIMIC_CODE\Extracted_CV1.csv'
file_path_cv2 = r'D:\Python\MIMIC_CODE\Extracted_CV2.csv'

# Read Extracted_CV1.csv and Extracted_CV2.csv
cv1_df = pd.read_csv(file_path_cv1)
cv2_df = pd.read_csv(file_path_cv2)

# Merge the dataframes
merged_df = pd.concat([cv1_df, cv2_df], ignore_index=True)

# Save the merged data to a new CSV file
output_path = r'D:\Python\MIMIC_CODE\CV_Final.csv'
merged_df.to_csv(output_path, index=False)

print(f"Merged data saved at {output_path}")


import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'C:\Users\HP\OneDrive\Desktop\Python\CE_V2.csv'
ce_v2_df = pd.read_csv(ce_v2_path)

# Filter rows where itemid is 211 or 220045
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([211, 220045])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID', 'VALUEUOM']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'HR'})
filtered_df = filtered_df.rename(columns={'SUBJECT_ID': 'Patient_ID'})
filtered_df = filtered_df.rename(columns={'chart_interval': 'Hour'})

# Filter out rows with non-numeric 'HR' values
filtered_df = filtered_df[filtered_df['HR'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'HR' column to numeric
filtered_df['HR'] = pd.to_numeric(filtered_df['HR'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'HR' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['HR'].mean()

# Save the resulting DataFrame to a new CSV file
average_output_path = r'C:\Users\HP\OneDrive\Desktop\Python\MIMIC_CODE\CV_HR_V2.csv'
average_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'HR'])

print(f"New CSV file 'average_hr.csv' saved at {average_output_path}")


import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'D:\Python\MIMIC_CODE\CV_Final.csv'
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
average_output_path = r'D:\Python\MIMIC_CODE\HR.csv'
average_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'HR'])

print(f"New CSV file 'average_hr.csv' saved at {average_output_path}")

# Read the CSV file into a DataFrame
df = pd.read_csv(average_output_path)

num_rows = len(df)

print(f'The number of rows in the CSV file is: {num_rows}')

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'C:\Users\HP\OneDrive\Desktop\Python\CE_V2.csv')

# Filter rows where itemid is 220277
filtered_df = ce_v2_df[ce_v2_df['ITEMID'].isin([220277])]

# Drop unnecessary columns
columns_to_drop = ['ITEMID', 'admission_time', 'VALUEUOM']
filtered_df = filtered_df.drop(columns_to_drop, axis=1)

# Rename columns
filtered_df = filtered_df.rename(columns={'VALUENUM': 'O2Sat'})
filtered_df = filtered_df.rename(columns={'SUBJECT_ID': 'Patient_ID'})
filtered_df = filtered_df.rename(columns={'chart_interval': 'Hour'})

# Filter out rows with non-numeric 'O2Sat' values
filtered_df = filtered_df[filtered_df['O2Sat'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

# Convert 'O2Sat' column to numeric
filtered_df['O2Sat'] = pd.to_numeric(filtered_df['O2Sat'])

# Group by 'Hour' and 'Patient_ID', then calculate the average of 'O2Sat' for each group
average_df = filtered_df.groupby(['Hour', 'Patient_ID'], as_index=False)['O2Sat'].mean()

# Save the resulting DataFrame to a new CSV file
average_df.to_csv(r'C:\Users\HP\OneDrive\Desktop\Python\Extract\O2Sat.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'C:\Users\HP\OneDrive\Desktop\Python\Extract\CV_Merge.csv')
o2sat_df = pd.read_csv(r'C:\Users\HP\OneDrive\Desktop\Python\Extract\O2Sat.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'C:\Users\HP\OneDrive\Desktop\Python\Extract\Merged.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\O2Sat.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\HR.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\O2Sat.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat'])

# Read the CSV file into a DataFrame
df = pd.read_csv(merged_df)

num_rows = len(df)

print(f'The number of rows in the CSV file is: {num_rows}')

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
desired_items_df.to_csv(r'D:\Python\MIMIC_CODE\Temp_Celsius.csv', index=False)

desired_items_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Temp_Celsius.csv')

df = pd.read_csv(desired_items_df)

num_rows = len(df)

print(f'The number of rows in the CSV file is: {num_rows}')

import pandas as pd

# Read your Temp_CE.csv file and manipulate the data as per your requirements
temp_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Temp_Celsius.csv')

# Merge the DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')
merged_df = pd.merge(merged_df, average_temp_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_1.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\SBP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_1.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\SBP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_2.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\MAP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_2.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\MAP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_3.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\DBP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_3.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\DBP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_4.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\PH.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_4.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\PH.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_5.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\HCO3.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_5.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\HCO3.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_6.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Creatinine.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_6.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Creatinine.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_7.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Magnesium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_7.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Magnesium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_8.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\WBC.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_8.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\WBC.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_9.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\BUN.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_9.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\BUN.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_10.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN'])



import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Hgb.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_10.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Hgb.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_11.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Hct.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_11.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Hct.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_12.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Potassium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_12.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Potassium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_13.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Hgb.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_10.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Hgb.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_11.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Hct.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_11.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Hct.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_12.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Potassium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_12.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Potassium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_13.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium'])




import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Sodium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_13.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Sodium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_14.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Glucose.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_14.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Glucose.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_15.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Fibrinogen.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_15.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Fibrinogen.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_16.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Sodium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_13.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Sodium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_14.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Glucose.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_14.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Glucose.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_15.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Fibrinogen.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_15.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Fibrinogen.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_16.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Chloride.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_16.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Chloride.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_17.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_17.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_18.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Ion_Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_18.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Ion_Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_19.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Chloride.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_16.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Chloride.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_17.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_17.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_18.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Ion_Calcium.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_18.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Ion_Calcium.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_19.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium'])




import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Bilirubin_direct.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_19.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Bilirubin_direct.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_20.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Troponin.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_20.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Troponin.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_21.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\CRP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_21.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CRP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_22.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\PTT.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_22.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\PTT.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_23.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\INR.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_23.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\INR.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_24.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Arterial_BE.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_24.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Arterial_BE.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_25.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE'])



import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Arterial_lactate.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_25.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Arterial_lactate.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_26.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate'])

import pandas as pd

# Read the CSV file
inputevents_df = pd.read_csv('E:/MIMIC IMP/INPUTEVENTS_MV.csv')

# Extract SUBJECT_ID and PATIENTWEIGHT
selected_columns = ['SUBJECT_ID', 'PATIENTWEIGHT']
extracted_data = inputevents_df[selected_columns]

# Rename 'SUBJECT_ID' to 'Patient_ID'
extracted_data = extracted_data.rename(columns={'SUBJECT_ID': 'Patient_ID'})

# Extract unique rows based on 'Patient_ID'
unique_patient_data = extracted_data.drop_duplicates(subset='Patient_ID')

# Save the result to a new CSV file
unique_patient_data.to_csv('D:/Python/MIMIC_CODE/Patient_weight_V2.csv', index=False)

import pandas as pd

# Read the CSV file
inputevents_mv_df = pd.read_csv('E:/MIMIC IMP/INPUTEVENTS_MV.csv')

# Extract SUBJECT_ID, STARTTIME, ITEMID, AMOUNT
selected_columns = ['SUBJECT_ID', 'STARTTIME', 'ITEMID', 'AMOUNT']
extracted_data = inputevents_mv_df[selected_columns]

# Rename 'SUBJECT_ID' to 'Patient_ID'
extracted_data = extracted_data.rename(columns={'SUBJECT_ID': 'Patient_ID'})

# Save the result to a new CSV file
extracted_data.to_csv('D:/Python/MIMIC_CODE/Extracted_MV.csv', index=False)

import pandas as pd

# Read the CSV files
merged_26_df = pd.read_csv('D:/Python/MIMIC_CODE/Merged_26.csv')
patient_weight_v2_df = pd.read_csv('D:/Python/MIMIC_CODE/Patient_weight_V2.csv')

# Merge DataFrames based on 'Patient_ID', using left join
merged_data = pd.merge(merged_26_df, patient_weight_v2_df, on='Patient_ID', how='left')

# Save the result to a new CSV file
merged_data.to_csv('D:/Python/MIMIC_CODE/Merged_27.csv', index=False)

import pandas as pd

# Assuming your DataFrame is named 'modified_cv1_df'
modified_cv1_df = pd.read_csv('D:/Python/MIMIC_CODE/Modified_CV1.csv')

# Extract Patient_ID and admission_time
selected_columns = ['SUBJECT_ID', 'admission_time']
patient_admission_data = modified_cv1_df[selected_columns].drop_duplicates()

# Save the result to a new CSV file
patient_admission_data.to_csv('D:/Python/MIMIC_CODE/Admission_1.csv', index=False)

import pandas as pd

# Assuming your DataFrame is named 'modified_cv1_df'
modified_cv1_df = pd.read_csv('D:/Python/MIMIC_CODE/Modified_CV2.csv')

# Extract Patient_ID and admission_time
selected_columns = ['SUBJECT_ID', 'admission_time']
patient_admission_data = modified_cv1_df[selected_columns].drop_duplicates()

# Save the result to a new CSV file
patient_admission_data.to_csv('D:/Python/MIMIC_CODE/Admission_2.csv', index=False)

import pandas as pd

# File paths
file_path_cv1 = r'D:\Python\MIMIC_CODE\Admission_1.csv'
file_path_cv2 = r'D:\Python\MIMIC_CODE\Admission_2.csv'

# Read Extracted_CV1.csv and Extracted_CV2.csv
cv1_df = pd.read_csv(file_path_cv1)
cv2_df = pd.read_csv(file_path_cv2)

# Merge the dataframes
merged_df = pd.concat([cv1_df, cv2_df], ignore_index=True)

# Save the merged data to a new CSV file
output_path = r'D:\Python\MIMIC_CODE\Admission_Final.csv'
merged_df.to_csv(output_path, index=False)

print(f"Merged data saved at {output_path}")

import pandas as pd

# Read the CSV files
merged_26_df = pd.read_csv('D:\Python\MIMIC_CODE\Extracted_MV.csv')
patient_weight_v2_df = pd.read_csv('D:\Python\MIMIC_CODE\Admission_Final.csv')

# Merge DataFrames based on 'Patient_ID', using left join
merged_data = pd.merge(merged_26_df, patient_weight_v2_df, on='Patient_ID', how='left')

# Save the result to a new CSV file
merged_data.to_csv('D:\Python\MIMIC_CODE\Extracted_MV_1.csv', index=False)

import pandas as pd

# Read the merged data from the CSV file
merged_data_path = r'D:\Python\MIMIC_CODE\Extracted_MV_1.csv'
merged_data = pd.read_csv(merged_data_path)

# Convert 'STARTTIME' column to datetime format
merged_data['STARTTIME'] = pd.to_datetime(merged_data['STARTTIME'])

# Calculate the hours elapsed since admission for each row
merged_data['hours_since_admission'] = (merged_data['STARTTIME'] - pd.to_datetime(merged_data['admission_time'])).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
merged_data['chart_interval'] = merged_data['hours_since_admission'] // 1

# Save the modified DataFrame to a new CSV file
output_csv_path = r'D:\Python\MIMIC_CODE\Extracted_MV_2.csv'
merged_data.to_csv(output_csv_path, index=False)

print(f"Modified merged data saved to {output_csv_path}")


import pandas as pd

# File paths
file_path_cv1 = r'D:\Python\MIMIC_CODE\Extracted_MV_2.csv'


# Columns to extract
columns_to_extract = ['Patient_ID', 'ITEMID', 'AMOUNT', 'chart_interval']

# Read and extract columns from Modified_CV1.csv
cv1_df = pd.read_csv(file_path_cv1, usecols=columns_to_extract)


# Save the extracted data to new CSV files
cv1_df.to_csv(r'D:\Python\MIMIC_CODE\Extracted_MV_3.csv', index=False)

print("Columns extracted and saved successfully.")


import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'D:\Python\MIMIC_CODE\Extracted_MV_3.csv'
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
average_output_path = r'D:\Python\MIMIC_CODE\Platelets.csv'
average_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'Platelets'])

print(f"New CSV file 'Platelets.csv' saved at {average_output_path}")

import pandas as pd

# Read the CSV file
inputevents_mv_df = pd.read_csv('E:\MIMIC IMP\INPUTEVENTS_CV.csv')

# Extract SUBJECT_ID, STARTTIME, ITEMID, AMOUNT
selected_columns = ['SUBJECT_ID', 'CHARTTIME', 'ITEMID', 'AMOUNT']
extracted_data = inputevents_mv_df[selected_columns]

# Rename 'SUBJECT_ID' to 'Patient_ID'
extracted_data = extracted_data.rename(columns={'SUBJECT_ID': 'Patient_ID'})

# Save the result to a new CSV file
extracted_data.to_csv('D:/Python/MIMIC_CODE/Extracted_CV.csv', index=False)

import pandas as pd

# Read the CSV files
merged_26_df = pd.read_csv('D:\Python\MIMIC_CODE\Extracted_CV.csv')
patient_weight_v2_df = pd.read_csv('D:\Python\MIMIC_CODE\Admission_Final.csv')

# Merge DataFrames based on 'Patient_ID', using left join
merged_data = pd.merge(merged_26_df, patient_weight_v2_df, on='Patient_ID', how='left')

# Save the result to a new CSV file
merged_data.to_csv('D:\Python\MIMIC_CODE\Extracted_CV_1.csv', index=False)

import pandas as pd

# Read the merged data from the CSV file
merged_data_path = r'D:\Python\MIMIC_CODE\Extracted_CV_1.csv'
merged_data = pd.read_csv(merged_data_path)

# Convert 'STARTTIME' column to datetime format
merged_data['CHARTTIME'] = pd.to_datetime(merged_data['CHARTTIME'])

# Calculate the hours elapsed since admission for each row
merged_data['hours_since_admission'] = (merged_data['CHARTTIME'] - pd.to_datetime(merged_data['admission_time'])).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
merged_data['chart_interval'] = merged_data['hours_since_admission'] // 1

# Save the modified DataFrame to a new CSV file
output_csv_path = r'D:\Python\MIMIC_CODE\Extracted_CV_2.csv'
merged_data.to_csv(output_csv_path, index=False)

print(f"Modified merged data saved to {output_csv_path}")

import pandas as pd

# File paths
file_path_cv1 = r'D:\Python\MIMIC_CODE\Extracted_CV_2.csv'


# Columns to extract
columns_to_extract = ['Patient_ID', 'ITEMID', 'AMOUNT', 'chart_interval']

# Read and extract columns from Modified_CV1.csv
cv1_df = pd.read_csv(file_path_cv1, usecols=columns_to_extract)


# Save the extracted data to new CSV files
cv1_df.to_csv(r'D:\Python\MIMIC_CODE\Extracted_CV_3.csv', index=False)

print("Columns extracted and saved successfully.")


import pandas as pd

# Read the CE_V2.csv file
ce_v2_path = r'D:\Python\MIMIC_CODE\Extracted_CV_3.csv'
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
average_output_path = r'D:\Python\MIMIC_CODE\Platelets_1.csv'
average_df.to_csv(average_output_path, index=False, columns=['Patient_ID', 'Hour', 'Platelets'])

print(f"New CSV file 'Platelets.csv' saved at {average_output_path}")

import pandas as pd

# File paths
file_path_cv1 = r'D:\Python\MIMIC_CODE\Platelets_1.csv'
file_path_cv2 = r'D:\Python\MIMIC_CODE\Platelets_2.csv'

# Read Platelets_1.csv and Platelets_2.csv
cv1_df = pd.read_csv(file_path_cv1)
cv2_df = pd.read_csv(file_path_cv2)

# Merge the dataframes
merged_df = pd.concat([cv1_df, cv2_df], ignore_index=True)

# Filter out rows with negative 'Hour' values
merged_df = merged_df[merged_df['Hour'] >= 0]

# Save the merged data to a new CSV file
output_path = r'D:\Python\MIMIC_CODE\Platelets.csv'
merged_df.to_csv(output_path, index=False)

print(f"Merged data saved at {output_path}")

import pandas as pd

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_27.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Platelets.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_28.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\ETCO2.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_28.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\ETCO2.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_29.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\paCO2.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_29.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\paCO2.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_30.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2'])



import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\PT.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_31.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\PT.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_32.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\RBC.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_32.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\RBC.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_33.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\SVR.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_33.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\SVR.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_34.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR'])


import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\GCS.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_34.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\GCS.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_35.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\SysBP.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_35.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\SysBP.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_36.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS', 'SysBP'])

import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\Bilirubin_Total.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_36.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Bilirubin_Total.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_37.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS', 'SysBP', 'Bilirubin_Total'])




import pandas as pd

# Read your CE_V2.csv file and manipulate the data as per your requirements
ce_v2_df = pd.read_csv(r'D:\Python\MIMIC_CODE\CV_Final.csv')

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
average_df.to_csv(r'D:\Python\MIMIC_CODE\SpO2.csv', index=False)

# Read the HR and O2Sat CSV files into DataFrames
ce_hr_df = pd.read_csv(r'D:\Python\MIMIC_CODE\Merged_37.csv')
o2sat_df = pd.read_csv(r'D:\Python\MIMIC_CODE\SpO2.csv')

# Merge the two DataFrames on 'Hour' and 'Patient_ID'
merged_df = pd.merge(ce_hr_df, o2sat_df, on=['Hour', 'Patient_ID'], how='outer')

# Save the resulting DataFrame to a new CSV file
merged_df.to_csv(r'D:\Python\MIMIC_CODE\Merged_38.csv', index=False, columns=['Patient_ID', 'Hour', 'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP','PH', 'HCO3', 'Creatinine', 'Magnesium', 'WBC', 'BUN', 'Hgb', 'Hct', 'Potassium', 'Sodium', 'Glucose', 'Fibrinogen', 'Chloride', 'Calcium', 'Ion_Calcium', 'Bilirubin_direct', 'Troponin', 'CRP', 'PTT', 'INR', 'Arterial_BE', 'Arterial_lactate', 'PATIENTWEIGHT', 'Platelets', 'ETCO2', 'paCO2', 'paO2', 'PT', 'RBC','SVR', 'GCS', 'SysBP', 'Bilirubin_Total', 'SpO2'])


import pandas as pd

# Read the ADMISSIONS.csv file
admissions_df = pd.read_csv('D:\Python\ADMISSIONS.csv')

# Drop rows where 'DIAGNOSIS' column has NaN values
admissions_df = admissions_df.dropna(subset=['DIAGNOSIS'])

# Filter the DataFrame for patients diagnosed with SEPSIS
sepsis_patients = admissions_df[admissions_df['DIAGNOSIS'].str.contains('SEPSIS', case=False)]

# Save the resulting DataFrame to a new CSV file
sepsis_patients.to_csv('D:\Python\ADMISSIONS_1.csv', index=False)

import pandas as pd

# Read the modified CSV file without negative intervals
chartevents_df = pd.read_csv('D:\Python\ADMISSIONS_1.csv')

# Rename 'SUBJECT_ID' column to 'Patient_ID'
chartevents_df.rename(columns={'SUBJECT_ID': 'Patient_ID'}, inplace=True)

# Select specific columns: 'SUBJECT_ID', 'ITEMID', 'AMOUNT'
selected_columns = chartevents_df[['Patient_ID', 'ADMITTIME', 'DIAGNOSIS']]

# Save the selected columns to a new CSV file
selected_columns.to_csv('D:\Python\Sepsis.csv', index=False)

import pandas as pd

# Read the modified CSV file without negative intervals
chartevents_df = pd.read_csv('D:/Python/CE_V2.csv')

# Rename 'SUBJECT_ID' column to 'Patient_ID'
chartevents_df.rename(columns={'SUBJECT_ID': 'Patient_ID'}, inplace=True)

# Select specific columns: 'SUBJECT_ID', 'ITEMID', 'AMOUNT'
selected_columns = chartevents_df[['Patient_ID', 'admission_time']]

# Save the selected columns to a new CSV file
selected_columns.to_csv('D:/Python/admission_time.csv', index=False)

import pandas as pd

# Read the ADMISSIONS.csv file
admissions_df = pd.read_csv('D:/Python/Sepsis_V2.csv')

# Drop rows where 'DIAGNOSIS' column has NaN values
admissions_df = admissions_df.dropna(subset=['admission_time'])

# Save the resulting DataFrame to a new CSV file
admissions_df.to_csv('D:/Python/Sepsis_V3.csv', index=False)

import pandas as pd

# Read the DataFrame from the provided CSV file
chartevents_df = pd.read_csv('D:\Python\Sepsis_V3.csv')

# Convert 'admittime' and 'admission_time' columns to datetime format
chartevents_df['ADMITTIME'] = pd.to_datetime(chartevents_df['ADMITTIME'])
chartevents_df['admission_time'] = pd.to_datetime(chartevents_df['admission_time'])

# Calculate the hours elapsed since admission for each row
chartevents_df['hours_since_admission'] = (chartevents_df['ADMITTIME'] - chartevents_df['admission_time']).dt.total_seconds() / 3600

# Assign a new column 'chart_interval' based on the hours elapsed
chartevents_df['chart_interval'] = chartevents_df['hours_since_admission'].apply(lambda x: 1 if x < 0 else x // 1 + 1)

# Save the modified DataFrame to a new CSV file
chartevents_df.to_csv('D:\Python\Sepsis_V4.csv', index=False)

import pandas as pd

# Read the CSV file
sepsis_df = pd.read_csv('D:/Python/Sepsis_V4.csv')

# Extract columns 'Patient_ID', 'DIAGNOSIS', 'chart_interval' and rename 'chart_interval' to 'Hour'
selected_columns = sepsis_df[['Patient_ID', 'DIAGNOSIS', 'chart_interval']]
selected_columns.rename(columns={'chart_interval': 'Hour'}, inplace=True)

# Save the selected columns to a new CSV file
selected_columns.to_csv('D:/Python/Sepsis_V5.csv', index=False)

import pandas as pd

# Read the Sepsis_V5.csv file
sepsis_df = pd.read_csv('D:/Python/Sepsis_V5.csv')

# Fill the 'DIAGNOSIS' column with 1 where patient is diagnosed with sepsis, else NaN or empty string
sepsis_df['DIAGNOSIS'] = sepsis_df['DIAGNOSIS'].apply(lambda x: 1 if 'SEPSIS' in str(x).upper() else '')

sepsis_df.rename(columns={'DIAGNOSIS': 'Sepsis_label'}, inplace=True)

# Select specific columns: 'Patient_ID', 'ADMITTIME', 'DIAGNOSIS'
selected_columns = sepsis_df[['Patient_ID', 'Hour', 'Sepsis_label']]

# Save the modified DataFrame to a new CSV file
selected_columns.to_csv('D:/Python/Sepsis_V6.csv', index=False)

import pandas as pd

# Read the first CSV file
data1 = pd.read_csv('D:\Python\MIMIC_CODE\Merged_38.csv')

# Read the second CSV file
data2 = pd.read_csv('D:\Python\Sepsis_V6.csv')

# Merge the two DataFrames based on 'Patient_ID'
merged_data = pd.merge(data1, data2, on=['Hour', 'Patient_ID'], how='outer')

# Save the merged data to a new CSV file
merged_data.to_csv('D:\Python\MIMIC_CODE\Merged_40.csv', index=False)


import pandas as pd

# Read the DataFrame from the provided CSV file
data = pd.read_csv('D:\Python\MIMIC_CODE\Merged_40.csv')

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
data.to_csv('D:\Python\MIMIC_CODE\Merged_41.csv', index=False)




import pandas as pd

# Read the CSV file
csv_file_path = 'E:\MIMIC IMP\PATIENTS.csv'  # Replace with your CSV file path
data = pd.read_csv(csv_file_path)

# Rename 'subject_id' to 'Patient_ID'
data = data.rename(columns={'SUBJECT_ID': 'Patient_ID'})

# Extract 'Patient_ID' and 'gender' columns
data = data[['Patient_ID', 'GENDER']]

# Convert gender to numeric (assuming Male = 0, Female = 1)
data['GENDER'] = data['GENDER'].map({'M': 0, 'F': 1})

# Save the updated DataFrame to a new CSV file
data.to_csv('D:\Python\MIMIC_CODE\Gender.csv', index=False)

import pandas as pd

# Read the extracted CSV file with Patient_ID and gender
extracted_data = pd.read_csv('D:\Python\MIMIC_CODE\Gender.csv')

# Read the other DataFrame (replace 'other_data.csv' with your file name)
other_data = pd.read_csv('D:\Python\MIMIC_CODE\Merged_41.csv')

# Merge the two DataFrames on 'Patient_ID'
merged_data = pd.merge(extracted_data, other_data, on='Patient_ID', how='inner')

# Save the merged DataFrame to a new CSV file
merged_data.to_csv('D:\Python\MIMIC_CODE\Merged_42.csv', index=False)


import pandas as pd

# Read the merged CSV file
merged_df = pd.read_csv('D:/Python/MIMIC_CODE/Merged_42.csv')

# Extract unique 'Patient_ID' values with 'Hour' more than 25
filtered_patient_ids = merged_df.loc[merged_df['Hour'] > 25, 'Patient_ID'].unique()

# Create a DataFrame with the filtered 'Patient_ID' values
filtered_df = pd.DataFrame({'Patient_ID': filtered_patient_ids})

# Save the resulting DataFrame to a new CSV file
filtered_df.to_csv('D:/Python/MIMIC_CODE/Filtered_Patient_IDs.csv', index=False)


import pandas as pd

# Read the Merged_42.csv file
merged_df = pd.read_csv('D:/Python/MIMIC_CODE/Merged_42.csv')

# Read the Filtered_Patient_IDs.csv file
filtered_patient_ids_df = pd.read_csv('D:/Python/MIMIC_CODE/Filtered_Patient_IDs.csv')

# Filter rows in Merged_42.csv based on Patient_IDs in Filtered_Patient_IDs.csv
filtered_merged_df = merged_df[merged_df['Patient_ID'].isin(filtered_patient_ids_df['Patient_ID'])]

# Save the filtered DataFrame to a new CSV file
filtered_merged_df.to_csv('D:/Python/MIMIC_CODE/Merged_43.csv', index=False)


import pandas as pd

# Read the Merged_43.csv file
merged_43_df = pd.read_csv('D:/Python/MIMIC_CODE/Merged_43.csv')

# Identify Patient_IDs with Sepsis_label = 1 at hour < 10
patients_to_remove = merged_43_df.loc[
    (merged_43_df['Sepsis_label'] == 1) & (merged_43_df['Hour'] < 10),
    'Patient_ID'
].unique()

# Remove rows with identified Patient_IDs
filtered_merged_43_df = merged_43_df[~merged_43_df['Patient_ID'].isin(patients_to_remove)]

# Save the filtered DataFrame to a new CSV file
filtered_merged_43_df.to_csv('D:/Python/MIMIC_CODE/Filtered_Merged_43.csv', index=False)


import pandas as pd

# Read the Merged_43.csv file
merged_43_df = pd.read_csv('D:/Python/MIMIC_CODE/Merged_43.csv')

# Identify Patient_IDs with Sepsis_label = 1 at hour < 10
patients_to_remove = merged_43_df.loc[
    (merged_43_df['Sepsis_label'] == 1) & (merged_43_df['Hour'] < 10),
    'Patient_ID'
].unique()

# Remove rows with identified Patient_IDs
filtered_merged_43_df = merged_43_df[~merged_43_df['Patient_ID'].isin(patients_to_remove)]

# Save the filtered DataFrame to a new CSV file
filtered_merged_43_df.to_csv('D:/Python/MIMIC_CODE/Merged_44.csv', index=False)



import pandas as pd

# Read the CSV file
data = pd.read_csv('E:\MIMIC IMP\PATIENTS.csv')

# Change column name from 'subject_id' to 'Patient_ID'
data.rename(columns={'SUBJECT_ID': 'Patient_ID'}, inplace=True)

# Extract unique values of 'Patient_ID' and 'admission_time'
extracted_data = data[['Patient_ID', 'DOB']].drop_duplicates()

# Save the extracted data to a new CSV file
extracted_data.to_csv('D:/Python/MIMIC_CODE/DOB.csv', index=False)

import pandas as pd

# Read the CSV files
dob_df = pd.read_csv('D:/Python/MIMIC_CODE/DOB.csv')
admission_df = pd.read_csv('D:\Python\MIMIC_CODE\Admission_Final.csv')

# Convert 'dob' column to datetime format
dob_df['DOB'] = pd.to_datetime(dob_df['DOB'])

# Convert 'admission_time' column to datetime format with coerce to handle errors
admission_df['admission_time'] = pd.to_datetime(admission_df['admission_time'], errors='coerce')

# Print rows where 'admission_time' couldn't be parsed
print("Rows with 'admission_time' not parsed:")
print(admission_df[admission_df['admission_time'].isnull()])

# Filter out rows with NaT (Not a Time - i.e., parsing errors)
admission_df = admission_df[admission_df['admission_time'].notnull()]

# Merge data based on 'Patient_ID' for validation
merged_data = pd.merge(admission_df, dob_df, on='Patient_ID', how='left')

# Calculate age by subtracting the years from 'admission_time' to 'dob'
merged_data['Age'] = (merged_data['admission_time'].dt.year - merged_data['DOB'].dt.year)

# Print rows where 'Age' is null
print("\nRows with 'Age' as null:")
print(merged_data[merged_data['Age'].isnull()])

# Save the updated DataFrame to a new CSV file
merged_data.to_csv('D:/Python/MIMIC_CODE/DOB_V1.csv', index=False)

import pandas as pd

# Read the CSV file
data = pd.read_csv('D:\Python\MIMIC_CODE\DOB_V1.csv')

# Extract unique values of 'Patient_ID' and 'admission_time'
unique_values = data[['Patient_ID', 'Age']].drop_duplicates()

# Save the unique values to a new CSV file
unique_values.to_csv('D:\Python\MIMIC_CODE\DOB_V2.csv', index=False)


import pandas as pd

# Read the extracted CSV file with Patient_ID and gender
extracted_data = pd.read_csv('D:\Python\MIMIC_CODE\Merged_44.csv')

# Read the other DataFrame (replace 'other_data.csv' with your file name)
other_data = pd.read_csv('D:\Python\MIMIC_CODE\DOB_V2.csv')

# Merge the two DataFrames on 'Patient_ID'
merged_data = pd.merge(extracted_data, other_data, on='Patient_ID', how='inner')

# Save the merged DataFrame to a new CSV file
merged_data.to_csv('D:\Python\MIMIC_CODE\Merged_45.csv', index=False)



import pandas as pd

# Read the CSV file
data = pd.read_csv('D:\Python\MIMIC_CODE\Merged_45.csv')

# Sort the data by 'Patient_ID' and 'Hour'
data.sort_values(by=['Patient_ID', 'Hour'], inplace=True)

# Initialize the ICULOS column with zeros
data['ICULOS'] = 0

# Initialize variables to track the current patient and hour
current_patient = None
current_hour = None
icu_count = 1

# Iterate through the rows and calculate ICULOS
for index, row in data.iterrows():
    if row['Patient_ID'] == current_patient:
        if row['Hour'] - current_hour > 10:
            icu_count = 1
        else:
            icu_count += 1
    else:
        icu_count = 1
    
    data.at[index, 'ICULOS'] = icu_count
    current_patient = row['Patient_ID']
    current_hour = row['Hour']

# Save the updated DataFrame to a new CSV file
data.to_csv('D:\Python\MIMIC_CODE\Merged_46.csv', index=False)

import pandas as pd

# Read the CSV file
data = pd.read_csv('E:\MIMIC IMP\ICUSTAYS.csv')

# Filter rows based on 'first_careunit' or 'last_careunit'
filtered_data = data[data['FIRST_CAREUNIT'].isin(['SICU', 'MICU']) | data['LAST_CAREUNIT'].isin(['SICU', 'MICU'])]

# Extract required columns
extracted_data = filtered_data[['SUBJECT_ID', 'HADM_ID', 'FIRST_CAREUNIT', 'LAST_CAREUNIT']]

# Display or save the extracted data
extracted_data.to_csv('D:\Python\MIMIC_CODE\ICU.csv', index=False)

import pandas as pd

# Create a DataFrame
df = pd.read_csv('D:\Python\MIMIC_CODE\ICU.csv')

# Merge 'first_careunit' and 'last_careunit' if they are the same
df['merged_careunit'] = df.apply(lambda row: row['FIRST_CAREUNIT'] if row['FIRST_CAREUNIT'] == row['LAST_CAREUNIT'] else None, axis=1)

# Drop rows where 'first_careunit' and 'last_careunit' are not the same
df.dropna(subset=['merged_careunit'], inplace=True)

# Drop 'first_careunit', 'last_careunit', and 'merged_careunit' columns (if needed)
df.drop(['FIRST_CAREUNIT', 'LAST_CAREUNIT'], axis=1, inplace=True)

df.to_csv('D:\Python\MIMIC_CODE\ICU_V1.csv', index=False)

import pandas as pd

# Create a DataFrame
df = pd.read_csv('D:\Python\MIMIC_CODE\ICU_V1.csv')

# Change column name 'merged_careunit' to 'Unit'
df.rename(columns={'merged_careunit': 'Unit'}, inplace=True)

# Map 'MICU' to 0 and 'SICU' to 1
df['Unit'] = df['Unit'].map({'MICU': 0, 'SICU': 1})

df.to_csv('D:\Python\MIMIC_CODE\ICU_V2.csv', index=False)

import pandas as pd

# Load the CSV file into a pandas DataFrame
input_csv_path = r'D:\Python\MIMIC_CODE\MIMIC_DATASET.csv'
df = pd.read_csv(input_csv_path)

# Drop duplicate rows
df_no_duplicates = df.drop_duplicates()

# Save the resulting DataFrame to a new CSV file
output_csv_path = r'D:\Python\MIMIC_CODE\MIMIC_DATASET_no_duplicates.csv'
df_no_duplicates.to_csv(output_csv_path, index=False)

print(f"Duplicate rows removed. Result saved to: {output_csv_path}")

import pandas as pd

# Load the dataset
file_path = r'D:\Python\MIMIC_CODE\MIMIC_DATASET_V1.csv'
data = pd.read_csv(file_path)

# Extract unique Patient_IDs where Sepsis_label = 1
sepsis_patient_ids = data[data['Sepsis_label'] == 1]['Patient_ID'].unique()

# Create a DataFrame with the extracted Patient_IDs
sepsis_patients_df = pd.DataFrame(sepsis_patient_ids, columns=['Patient_ID'])

# Save the resulting csv
sepsis_patients_df.to_csv(r'D:\Python\MIMIC_CODE\sepsis_patients.csv', index=False)


import pandas as pd

# Load the sepsis_patients.csv
sepsis_patients_file_path = r'D:\Python\MIMIC_CODE\sepsis_patients.csv'
sepsis_patients_df = pd.read_csv(sepsis_patients_file_path)

# Extract unique Patient_IDs from sepsis_patients.csv
sepsis_patient_ids = sepsis_patients_df['Patient_ID'].unique()

# Load the MIMIC_DATASET_V1.csv
dataset_v1_file_path = r'D:\Python\MIMIC_CODE\MIMIC_DATASET_V1.csv'
data_v1 = pd.read_csv(dataset_v1_file_path)

# Extract all recordings for patients present in sepsis_patients.csv
patients_in_sepsis_data_v1 = data_v1[data_v1['Patient_ID'].isin(sepsis_patient_ids)]

# Save the resulting csv
patients_in_sepsis_data_v1.to_csv(r'D:\Python\MIMIC_CODE\sepsis_recording.csv', index=False)

import pandas as pd

# Load the sepsis_recording.csv
sepsis_recording_file_path = r'D:\Python\MIMIC_CODE\sepsis_recording.csv'
sepsis_recording_df = pd.read_csv(sepsis_recording_file_path)

# Group by Patient_ID and find the first occurrence of Sepsis_label = 1
first_sepsis_hour = sepsis_recording_df[sepsis_recording_df['Sepsis_label'] == 1].groupby('Patient_ID')['Hour'].first()

# Define a function to filter out recordings based on the difference in hours
def filter_recordings(patient_id):
    patient_data = sepsis_recording_df[sepsis_recording_df['Patient_ID'] == patient_id]
    if patient_id in first_sepsis_hour.index:
        first_sepsis_time = first_sepsis_hour[patient_id]
        patient_data = patient_data[~((patient_data['Sepsis_label'] == 0) & (first_sepsis_time - patient_data['Hour'] > 720))]
    return patient_data

# Apply the function to each patient ID and concatenate the results
filtered_data = pd.concat([filter_recordings(patient_id) for patient_id in sepsis_recording_df['Patient_ID'].unique()])

# Save the resulting csv
filtered_data.to_csv(r'D:\Python\MIMIC_CODE\modified_Srec.csv', index=False)


import pandas as pd

# Load the modified_Srec.csv
modified_Srec_file_path = r'D:\Python\MIMIC_CODE\modified_Srec.csv'
modified_Srec_df = pd.read_csv(modified_Srec_file_path)

# Ensure that the first recording for each Patient_ID starts with Hour 0
for patient_id, group in modified_Srec_df.groupby('Patient_ID'):
    if group['Hour'].min() != 0:
        hour_diff = group['Hour'].min()
        modified_Srec_df.loc[modified_Srec_df['Patient_ID'] == patient_id, 'Hour'] -= hour_diff

# Increment Hour column for each Patient_ID
patient_hour_mapping = modified_Srec_df.groupby('Patient_ID')['Hour'].first().to_dict()
modified_Srec_df['Hour'] += modified_Srec_df['Patient_ID'].map(patient_hour_mapping)

# Save the resulting csv
modified_Srec_df.to_csv(r'D:\Python\MIMIC_CODE\modified_Srec_V1.csv', index=False)

import pandas as pd

# Load the modified_Srec.csv
modified_Srec_file_path = r'D:\Python\MIMIC_CODE\modified_Srec_V1.csv'
modified_Srec_df = pd.read_csv(modified_Srec_file_path)

# Filter recordings where Age is more than 90 or less than 13
modified_Srec_df = modified_Srec_df[(modified_Srec_df['Age'] <= 90) & (modified_Srec_df['Age'] >= 13)]

# Save the resulting csv
modified_Srec_df.to_csv(r'D:\Python\MIMIC_CODE\modified_Srec_V2.csv', index=False)

import pandas as pd

# Step 1: Load the MIMIC_DATASET_V1.csv and sepsis_patients.csv
mimic_dataset_v1_path = r'D:\Python\MIMIC_CODE\MIMIC_DATASET_V1.csv'
sepsis_patients_path = r'D:\Python\MIMIC_CODE\sepsis_patients.csv'
mimic_dataset_v1 = pd.read_csv(mimic_dataset_v1_path)
sepsis_patients = pd.read_csv(sepsis_patients_path)

# Step 2: Remove Patient_ID's present in sepsis_patients.csv from MIMIC_DATASET_V1.csv
non_sep_df = mimic_dataset_v1[~mimic_dataset_v1['Patient_ID'].isin(sepsis_patients['Patient_ID'])]

# Step 3: Save the resulting CSV as non_sep.csv
non_sep_csv_path = r'D:\Python\MIMIC_CODE\non_sep.csv'
non_sep_df.to_csv(non_sep_csv_path, index=False)

# Step 4: Perform an outer merge between non_sep.csv and modified_Srec_V2.csv
modified_Srec_V2_path = r'D:\Python\MIMIC_CODE\modified_Srec_V2.csv'
modified_Srec_V2 = pd.read_csv(modified_Srec_V2_path)
mimic_dataset_v2 = pd.merge(non_sep_df, modified_Srec_V2, on='Patient_ID', how='outer')

# Step 5: Save the resulting CSV as MIMIC_DATASET_V2.csv
mimic_dataset_v2_path = r'D:\Python\MIMIC_CODE\MIMIC_DATASET_V2.csv'
mimic_dataset_v2.to_csv(mimic_dataset_v2_path, index=False)

