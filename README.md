# YOUTUBE_DATA_HARVEST

**Libraries Used**:
from googleapiclient.discovery import build: Imports the build function from the googleapiclient.discovery module, which is used to create a service object for making requests to the YouTube Data API.
from pprint import pprint: Imports the pprint function from the pprint module, which is used for pretty-printing data structures.
import pymongo: Imports the pymongo library, which is a Python driver for MongoDB.
import mysql.connector: Imports the mysql.connector library, which provides Python connectivity to MySQL database servers.
import pandas as pd: Imports the pandas library, which is used for data manipulation and analysis.
import streamlit as st: Imports the streamlit library, which is used for building web applications with Python.
import dateutil: Imports the dateutil module, which provides powerful extensions to the standard datetime module.
from datetime import *: Imports all classes from the datetime module.
from time import strftime: Imports the strftime function from the time module, which is used for formatting time.

**FEATURES**: The following functions are available in the YouTube Data Harvesting and Warehousing application: 

Establishes a connection to MongoDB.
Defines Streamlit components (e.g., text inputs, buttons, etc.) for user interaction.
Provides functionality for querying and displaying data from the MySQL database based on user-selected questions.
This script essentially serves as a tool for gathering, storing, and analyzing YouTube data using the YouTube Data API, MongoDB, MySQL, and Streamlit. It provides a user-friendly interface for querying and visualizing the collected data.
