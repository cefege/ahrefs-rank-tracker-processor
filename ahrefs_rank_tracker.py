#! /usr/bin/env python
from collections import Counter
import datetime
import shutil
import tempfile
import zipfile
import numpy as np
import subprocess
import glob
import os
import time
import pandas as pd
from functools import reduce
import tldextract
import streamlit as st
import random
from zipfile import ZipFile


def rename_csv_files(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv") and "__MACOSX" not in root:
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
                url_col = df["URL"].astype(str)  # Convert the 'URL' column to strings
                for url in url_col:
                    if "http" in url:
                        url = tldextract.extract(url).domain
                        new_file_name = url + ".csv"
                        # create a new column named 'project' and set all the values to url
                        df["project"] = url
                        df.to_csv(file_path, index=False, sep="\t", encoding="utf-16")
                        break  # Only need the first matching URL
                else:
                    continue  # No matching URL found, skip renaming
                new_file_path = os.path.join(root, new_file_name)
                if file_path != new_file_path and not os.path.exists(new_file_path):
                    os.rename(file_path, new_file_path)
                    file_path = (
                        new_file_path  # Update the file path for the second loop below
                    )

                # Append folder name to the end of the filename
                folder_name = os.path.basename(root)
                file_name = os.path.basename(file_path)
                new_file_name = (
                    file_name.replace(".csv", "") + "_" + folder_name + ".csv"
                )
                new_file_path = os.path.join(root, new_file_name)
                if file_path != new_file_path and not os.path.exists(new_file_path):
                    os.rename(file_path, new_file_path)


def add_folder_name_to_csv(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv") and "._" not in file:
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
                folder_name = os.path.basename(root)
                df["date_scraped"] = folder_name
                df.to_csv(file_path, index=False, sep="\t", encoding="utf-16")


def process_csv_files(folder_path):
    for file in glob.glob(os.path.join(folder_path, "**/*.csv"), recursive=True):
        df = pd.read_csv(file, sep="\t", encoding="utf-16")
        # try rename colum "position" to "rank"
        try:
            df = df.rename(columns={"Position": "Rank"})
        except:
            pass

        df = df[
            [
                "Keyword",
                "URL",
                "Location",
                "Volume",
                "Tags",
                "date_scraped",
                "project",
            ]
            + [col for col in df if col.startswith("Rank")]
        ]

        # sort alphabetically by keyword column 'tags' then 'keyword' then 'location'
        df = df.sort_values(by=["Tags", "Keyword", "Location"])
        # overwrite the original file with the sorted data
        df.to_csv(file, index=False, sep="\t", encoding="utf-16")


# goest to a folder path and subfolders and opens eact .csv file with pandas and extracts all values from column "project" and returns a list of all the project names
def extract_project_names(folder_path):
    project_names = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv") and "._" not in file:
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
                if "project" in df.columns:
                    project_names.extend(df["project"].unique())
    # remove duplicates
    project_names = list(set(project_names))
    return project_names


def merge_csv_files(folder, search_string):
    # Find all files in the folder and subfolders that contain .csv files
    files = glob.glob(os.path.join(folder, "**/*.csv"), recursive=True)
    # Merge all the CSV files into one dataframe
    df_list = []
    for file in files:
        df = pd.read_csv(file, sep="\t", encoding="utf-16")
        df_list.append(df)
    merged_df = pd.concat(df_list, ignore_index=True)

    # Filter the dataframe to only include rows that column "project" contains the search string
    merged_df = merged_df[merged_df["project"].str.contains(search_string, na=False)]

    # remove duplicates rows
    merged_df = merged_df.drop_duplicates()

    return merged_df


def calculate_days_between_dates(dates, max_diff):
    # Convert the list of dates to numpy.datetime64 objects
    dates = np.array(dates, dtype="datetime64[s]")

    # Convert numpy.datetime64 objects to datetime.datetime objects
    dates = [np.datetime64(date).astype(datetime.datetime) for date in dates]

    # Initialize variables
    num_days = []
    i = 0

    while i < len(dates) - 1:
        # Calculate the number of days between consecutive dates
        diff = (dates[i] - dates[i + 1]).days

        # If the difference is less than the maximum allowed difference, remove the second date and recalculate
        if diff < max_diff:
            dates = np.delete(dates, i + 1)
        else:
            num_days.append(diff)
            i += 1

    # convert datetime.datetime objects to a list of dates
    dates = [date.strftime("%Y-%m-%d") for date in dates]
    # Return the list of remaining dates
    return dates


def important_dates_filter(project_df, max_dates):
    # extract the unique "date_scraped" values
    unique_dates = project_df["date_scraped"].unique()

    #  extract the unique "date_scraped" values
    unique_dates = calculate_days_between_dates(unique_dates, 10)

    # only keep the first 3 elements in the list, if there are more than 3 elements
    if len(unique_dates) > max_dates:
        unique_dates = unique_dates[:max_dates]

    # filter the dataframe to only in column date_scraped valuest from unique_dates
    project_df = project_df[project_df["date_scraped"].isin(unique_dates)]

    # sort the dataframe by "scrapped_date" from newest to oldest
    project_df = project_df.sort_values("date_scraped", ascending=False)

    return project_df


def pivot_rank_tracker(df):
    # Convert date_scraped to datetime and format it
    df["date_scraped"] = pd.to_datetime(df.date_scraped).dt.strftime("%Y-%m-%d")

    # Pivot the dataframe on the date_scraped column and reset the index
    df = df[["Keyword", "date_scraped", "Location", "Rank", "URL", "Volume", "Tags"]]
    df = df.pivot(
        index=["Keyword", "Location", "URL", "Volume", "Tags"],
        columns="date_scraped",
        values="Rank",
    )
    df = df.reset_index()
    df.columns.name = None

    # Rename columns that start with 'Rank'
    df.columns = [
        "{}_{}".format("Rank", col) if "-" in col else col for col in df.columns
    ]

    # Rearrange the columns so that the 'Rank' columns come after the first column
    col_filtered_final = [col for col in df.columns if not "Rank" in col]
    col_rank = [col for col in df.columns if "Rank" in col]

    col_rank.reverse()
    col_filtered_final[1:1] = col_rank
    df = df[col_filtered_final]

    return df


def create_project_reports(folder_path, project_list):
    for project in project_list:
        project_df = merge_csv_files(folder_path, project)
        # print(project_df, "project_df===")
        project_df = important_dates_filter(project_df, 3)
        project_df = pivot_rank_tracker(project_df)
        # go to up a level and create a folder called "projects" if it doesn't exist
        os.makedirs(os.path.join(folder_path, "projects"), exist_ok=True)
        # save the dataframe to a csv file
        project_df.to_csv(
            os.path.join(folder_path, "projects", project + ".csv"),
            sep="\t",
            encoding="utf-16",
            index=False,
        )


@st.cache_resource
def main_loop(
    zip_file, folder_path
):  # If a zip file was uploaded, extract it in the current folder
    if zip_file is not None:
        if os.path.exists("data") and os.path.isdir("data"):
            # Remove the directory and its contents
            shutil.rmtree("data")
            print(f"{'data'} has been deleted.")
        else:
            print(f"{'data'} does not exist or is not a directory.")

        with zipfile.ZipFile(zip_file) as zip_ref:
            zip_ref.extractall("./" + data_dir)
        # Define the path to the folder containing the csv files
        # # rename to human readable names
        rename_csv_files(folder_path)
        # # add date column
        add_folder_name_to_csv(folder_path)
        # # remove unnecessary columns
        process_csv_files(folder_path)

        # for all csv files in folder_path and subfolders, extract all values from column "project" and save them to a list
        project_list = extract_project_names(folder_path)

        # show all columns  when printing
        # pd.set_option("display.max_columns", None)

        # enumerate through the list of projects and create a project report for each project dataframe
        create_project_reports(folder_path, project_list)


def main(data_dir):
    # Set up the app title and description
    st.title("Ahrefs Rank Tracker CSV Processor")
    st.markdown(
        "This app processes Ahrefs Rank Tracker CSV files and generates project reports."
    )
    # Use file_uploader to get the uploaded zip file
    zip_file = st.file_uploader("Upload a zip file", type="zip")

    main_loop(zip_file, data_dir)


if __name__ == "__main__":
    data_dir = os.path.join("data", "ahrefs", "rank_tracker")
    main(data_dir)
    # Display the project reports
    project_list = extract_project_names(data_dir)
    if project_list:
        st.sidebar.markdown("## Project Reports")
        selected_project = st.sidebar.selectbox(
            "Select a project to view", project_list
        )
        if selected_project:
            # Load the corresponding CSV file and display it
            project_df = pd.read_csv(
                os.path.join(data_dir, "projects", selected_project + ".csv"),
                sep="\t",
                encoding="utf-16",
            )
            st.header(selected_project)
            st.dataframe(project_df)
