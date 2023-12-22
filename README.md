# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit

# Problem Statement:
Create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
Option to store the data in a MongoDB database as a data lake.
Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.


# Approach: 
**1)Set up a Streamlit app:** Create a simple UI using streamlit where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.
**2)Connect to the YouTube API:** Use the Google API client library for Python to make requests to the API to retreive data from YouTube
**3)Store data in a MongoDB data lake:** Store retrieved data MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
**4)Migrate data to a SQL data warehouse:** Once necessary channel's data are store in datalake, migrate it to a MySQL data warehouse.
**4)Query the SQL data warehouse:** Use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.
**5)Display data in the Streamlit app:** Finally, display the retrieved data in the Streamlit app. 


# Results:
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

