# Phonepe-Pulse-Data-Visualization-and-Exploration
                                                                   
## Introduction
PhonePe has become a leader among digital payment platforms, serving millions of users for their daily transactions. Known for its easy-to-use design, fast and secure payment processing, and creative features, PhonePe has gained praise and recognition in the industry. The PhonePe Pulse Data Visualization and Exploration project aims to gather valuable information from PhonePe's GitHub repository, process the data, and present it using an interactive dashboard that's visually appealing. This is accomplished using Python, Streamlit, and Plotly.


## About PhonePe-Pulse Data:
This data has been structured to provide details of following three sections with data cuts on Transactions, Users and Insurance of PhonePe Pulse - Explore tab.
1.	Aggregated - Aggregated values of various payment categories as shown under Categories section
2.	Map - Total values at the State and District levels.
3.	Top - Totals of top States / Districts /Pin Codes

   
# Table of Contents
•	Prerequisites
•	Installation
•	Usage
•	Features
•	Contributing
•	Contact


## Prerequisites
1.	Python -- Programming Language
2.	plotly -- Python Module that is used for data visualization and supports various graphs
3.	pandas -- Python Library for Data Visualization
4.	streamlit -- Python framework to rapidly build and share beautiful machine learning and data science web apps
5.	git.repo.base -- Python Module that helps to clone the github Repository and the store the data locally
6.	mysql.connector -- Python Library that enables Python programs to access MySQL databases
7.	json -- Python Library that helps parse JSON into a Python dictionary/list, in short open JSON files using Python
8.	os -- Python Module that provides functions for interacting with the operating system.

   
# Features

## Data Collection: 
Clone PhonePe Pulse data from the GitHub repository to your local directory for seamless access. Streamline your data collection process effortlessly. Explore a rich variety of insightful metrics and analytics, empowering you with comprehensive information. Make informed decisions with up-to-date data, ensuring precision in your analyses and strategies.


### Data Overview: 
Immerse yourself in a detailed exploration of the collected data. Gain comprehensive insights with breakdowns by states, years, quarters, transaction types, and user devices. This thorough analysis empowers you to make informed decisions based on a nuanced understanding of the dataset. Uncover trends, patterns, and correlations that drive strategic planning. Elevate your data-driven approach with a wealth of information at your fingertips.


# Migrating Data to SQL Database: 
Simplify your workflow by seamlessly converting PhonePe Pulse data from JSON to a structured DataFrame. Effortlessly store the organized data in a mysqlconnector database, ensuring optimal accessibility and efficiency. This streamlined process facilitates easier querying and analysis. Take advantage of a robust foundation for your data-driven applications and insights.


# Interactive Streamlit Interface:
Unleash the power of data exploration through our Streamlit app. The intuitive interface facilitates seamless interaction with dynamic charts, allowing users to customize visualizations and apply filters. Effortlessly zoom in or out to delve into specific nuances of your analysis. Empower your data-driven decisions with a user-friendly and adaptable platform.

# Dynamic Visualizations with Plotly:
Unlock the potential of Plotly to generate an array of charts, from dynamic line charts to insightful bar charts, scatter plots, and pie charts. Dive into your data with these visualizations, gaining a deeper understanding and effortlessly identifying patterns, trends, and correlations. Plotly's robust features empower users to create compelling visuals that enhance data exploration and analysis.

# Data Insights and Exploration: 
Embark on a dynamic analytical journey with our interactive Plotly charts and maps. Delve into nuanced insights across states, years, quarters, districts, transaction types, and user brands. Navigate seamlessly through a wealth of information, gaining a comprehensive understanding of your data landscape. Uncover patterns and trends that empower informed decision-making, making your exploration both insightful and user-friendly.

# Geo Visualization: 
Elevate your data exploration with a dynamic geo-visualization dashboard crafted using Streamlit and Plotly. Interact seamlessly with live maps, gaining real-time insights and unlocking the full potential of your geographical data. Effortlessly navigate through the interactive features to enhance your understanding and make informed decisions based on the latest information.


# Project Workflow

# Step 1 -- Installing and Importing the required Libraries

Firstly install all the required extensions/libraries/modules given above, if not installed
pip install (name of the library/module)
After installing the required libraries one need to import them in the program before one can use them.

import streamlit as st
import mysqlconnector
import requests
from streamlit_option_menu import option_menu
import pandas as pd
import plotly.express as pe
import os
import json

# Step 2 -- Data Extraction - Cloning the GitHub Repository
Now one need to clone the GitHub Repository to fetch the data from the Phonepe pulse GitHub repository.
Go to Google website -> Git Downloads -> go to "https://www.git-scm.com/downloads" -> click your choice(windows, linux/unix, macOS) -> After downloaded you may install that -> After installing, we have to check whether it is properly installed or not. -> For checking, use command prompt-> In command Prompt, type git, then it shows some messages that indicates gitbash is installed successfully -> then we give the username and email for access git -> Create a New Folder -> Open vscode terminal -> Create command prompt -> Type git clone https://github.com/PhonePe/pulse.git -> Press Enter -> cloing into 'pulse' -> After cloning will be done -> check the New Folder whether the data cloned.


# Step 3 -- Data Tranformation - JSON to Pandas DataFrame
Note : This step is performed in the .ipynb Python notebook that is in a Jupyter Notebook, because it is comparitively easy to visualize and tranform data in a .ipynb Python notebook as comapared to .py Python file. You can check the .ipynb notebook attached above for the code implementation of the process below.
After the Data Extraction part is completed one need to transform the data. The data that was extracted from the Phonepe Pulse Repository is in form of .json file, now we need to transform that data into Pandas DataFrame, so that we can visualize the data more efficiently in the form of table and if there are any null values we can do Data cleaning to handle those null values. One more advantage of tranforming the data into Pandas DataFrame is that we can further visualize the data in the form of graphs using the Plotly module in Python.
Here, we go through each JSON file in each folder using a for loop to convert them into a DataFrame that the rest of the program can read and understand. Iteratively accessing each folder's json files, extracting the necessary key and value pairs, and then combining them into a dataframe. For this procedure, I relied on the os, json, and pandas packages.


# Step 4 -- Data Insertion - Inserting the Data into MySQL Database
## Note : You can check the .ipynb notebook attched above for the code implementation of the below process.
After that one need to create a MySQL Database in there local system. Now below is the Python code to connect to that SQL Database.
hostname = "your host name goes here"
database = "your database name goes here"

   username = "your username goes here"
   pwd = "your password goes here"
   port = "your port goes here"
   mydb=mysql.connector.connect(host=hostname,user=username,password=password,database=database name)
   mycursor = mydb.cursor()

After the connection is successfully established one need to write the code to create the required tables with the required columns and insert the data that we have transformed in step-3 to the MySQL database.


# Step 5 -- To create a Streamlit Application
## Note : You can check the .py file attched above for the code implementation of this streamlit Application.

The result of this project is a live geo visualization streamlit dashboard that displays information and insights from the Phonepe pulse Github repository in an interactive and visually appealing manner.
The data is stored in a MySQL database for efficient retrieval and the dashboard is dynamically updated to reflect the latest data. Users are able to access the dashboard from a web browser and easily navigate the different visualizations and facts and figures displayed.

The dashboard provides valuable insights and information about the data in the Phonepe pulse Github repository, making it a valuable tool for data analysis and decision-making.
Overall, the result of this project is a comprehensive and user-friendly solution for extracting, transforming, and visualizing data from the Phonepe pulse Github repository.
The user-friendly interface is made with Streamlit, and the data is visualized using Plotly's in-built functionalities.

To Run the Application one can run the command mentioned below.
streamlit run phone_pe.py


