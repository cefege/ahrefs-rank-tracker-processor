# Ahrefs Rank Tracker CSV Processor

This Python script processes Ahrefs Rank Tracker CSV files and generates project reports. The script uses the following Python packages:

- collections
- datetime
- shutil
- tempfile
- zipfile
- numpy
- subprocess
- glob
- os
- time
- pandas
- functools
- tldextract
- streamlit
- random

## Getting Started

To use this script, follow these steps:

1. Clone the repository to your local machine.
2. Open a terminal or command prompt and navigate to the cloned repository.
3. Install the required Python packages by running the following command:

   ```
   pip install -r requirements.txt
   ```

4. Place your Ahrefs Rank Tracker CSV files in the `data/ahrefs/rank_tracker` folder.
5. Run the script using the following command:

   ```
   streamlit run ahrefs_rank_tracker.py
   ```

6. The script will generate project reports for each unique project name found in the CSV files. The reports will be saved in the `data/ahrefs/rank_tracker/projects` folder.
7. To view the project reports, open a web browser and go to `http://localhost:8501`.

## Script Functions

The script includes the following functions:

### `rename_csv_files`

This function renames CSV files based on the value in the `URL` column.

### `add_folder_name_to_csv`

This function adds the name of the folder containing the CSV file to a new column called `date_scraped`.

### `process_csv_files`

This function removes unnecessary columns from the CSV files and sorts them alphabetically by `Tags`, `Keyword`, and `Location`.

### `extract_project_names`

This function extracts all unique project names from the CSV files.

### `merge_csv_files`

This function merges all CSV files into one dataframe and filters the dataframe by project name.

### `calculate_days_between_dates`

This function calculates the number of days between dates in a list and removes dates that are less than a specified number of days apart.

### `important_dates_filter`

This function filters the dataframe to only include rows that match the dates specified in the `calculate_days_between_dates` function.

### `pivot_rank_tracker`

This function pivots the dataframe to display the rank for each date in a separate column.

### `create_project_reports`

This function creates a project report for each project name.

### `main_loop`

This function is the main loop of the script. It extracts the zip file, renames CSV files, adds the folder name to CSV files, processes CSV files, extracts project names, and creates project reports.

### `main`

This function sets up the Streamlit app title and description, and uses `file_uploader` to get the uploaded zip file. It then calls the `main_loop` function to process the CSV files.

## Streamlit Interface

The script includes a Streamlit interface to view the project reports. The interface displays a sidebar with a dropdown menu to select the project to view. The selected project report is then displayed in the main area of the interface.

## Conclusion

This Python script provides an easy way to process Ahrefs Rank Tracker CSV files and generate project reports. The script can be easily customized to suit your needs, and the Streamlit interface provides a user-friendly way to view the reports.
