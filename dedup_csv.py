import os

import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv(os.path.abspath("./Input/generated_data1_source.csv"), sep=',', encoding_errors='ignore', na_filter=True)

print('df:', df)

# Remove duplicates
df_dup = df.duplicated(keep=False)
df_no_duplicates = df.drop_duplicates()

# Display the first few rows of the DataFrame after removing duplicates
print('df_dup: ', df_dup)
print('df_dedup: ', df_no_duplicates)