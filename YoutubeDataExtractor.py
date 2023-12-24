#pip install google-api-python-client
#pip install pymongo
#pip install mysql
#pip install streamlit

#import necessary Libraries
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import json
from streamlit_option_menu import option_menu
from datetime import datetime
from isodate import parse_duration

#Few_Channel_IDs for testing: 
# UCHlNU7kIZhRgSbhHvFoy72w - Hugging Face , 
# UCCktnahuRFYIBtNnKT5IYyg - Intellipaat ,
# UCV8e2g4IWQqK71bbzGDEI4Q - Data Professor , 
# UC2UXDak6o7rBm23k3Vv5dww - Tina Huang , 
# UCCezIgC97PvUuR4_gbFUs5g - Corey Schafer, 
# UCu4mLxrAkMEcjK9N1yJOxyA -Imtiaz Ahmad

#Create an API service object
def get_api_service():
    api_key ='AIzaSyAjH7N3yughTAAmhHjfcqtUeqJ3vwWzcww'
    return build('youtube','v3',developerKey=api_key)

#Establish connection to MYSQL:
mydb = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password = "Shilpapraj234",
)

 # Function to Retrieve the channel information for the given channel id
def get_channel_data(channel_id):
    channel_info = youtube.channels().list(
    part='snippet,statistics,contentDetails',
    id=channel_id
    ).execute()
    return (channel_info)

 # Function to Retrieve the playlist information for the given playlist id
def get_playlistitem_data(playlist_id,nextPageToken):
    playlist_items_request= youtube.playlistItems().list(
        playlistId=playlist_id,
        part='snippet,contentDetails',
        maxResults=50,
        pageToken= nextPageToken
    )
    playlist_items_response = playlist_items_request.execute()
    return(playlist_items_response)

# Function to Retrieve the video information for the given video id
def get_video_data(video_id):
    video_data_request = youtube.videos().list(
    part="snippet,contentDetails,statistics,id",
    id=video_id
    )
    video_data__response = video_data_request.execute()
    return(video_data__response)

# Function to Retrieve the comment information for the given video id
def get_comment_data(video_id,nextPageToken):
    comment_thread_request = youtube.commentThreads().list(
    part="snippet,replies",
    videoId=video_id,
    textFormat="plainText",
    maxResults=500,
    pageToken= nextPageToken
    )
    comment_thread__response = comment_thread_request.execute()
    return(comment_thread__response)

# Function to create a dictionary ready for MongoDB datalake
def api_extraction(channel_id_input):
    try:
        channels={}
        channel_id=channel_id_input
        channel_data=get_channel_data(channel_id)
        playlist_id=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        channel_name = channel_data['items'][0]['snippet']['title']
        channel_information= {
            'channel_name' : channel_data['items'][0]['snippet']['title'],
            'Channel_Id': channel_data['items'][0]['id'],
            'Subscription_Count': channel_data['items'][0]['statistics']['subscriberCount'],
            'Channel_Views' : channel_data['items'][0]['statistics']['viewCount'],
            'channel_description' : channel_data['items'][0]['snippet']['description'],
            'playlist_id' : channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            }
        channel_df=pd.DataFrame(channel_information,[0])
    
        if playlist_id:
            nextPageToken = None
            while True:
                playlist_item_response =get_playlistitem_data(playlist_id,nextPageToken)
                for video in playlist_item_response['items']:
                    video_id = video['snippet']['resourceId']['videoId']
                    video_data_response = get_video_data(video_id)
                    video_information={
                        'playlist_id':playlist_id,
                        'video_id':video_id,
                        'Video_Name':video_data_response['items'][0]['snippet']['title'],
                        'Video_Description':video_data_response['items'][0]['snippet']['description'],
                        'PublishedAt': video_data_response['items'][0]['snippet']['publishedAt'],
                        'View_Count': video_data_response['items'][0]['statistics']['viewCount'],
                        'Like_Count': video_data_response['items'][0]['statistics']['likeCount'],
                        'Favorite_Count':video_data_response['items'][0]['statistics']['favoriteCount'],
                        'Comment_Count': video_data_response['items'][0]['statistics']['commentCount'],
                        'Duration': video_data_response['items'][0]['contentDetails']['duration'],
                        'Thumbnail': video_data_response['items'][0]['snippet']['thumbnails']['default']['url'],
                        'Caption_Status': video_data_response['items'][0]['contentDetails']['caption'],
                        }
                    if video_id:
                        nextPageToken = None
                        while True:
                            comment_thread_response=get_comment_data(video_id,nextPageToken)
                            for item in comment_thread_response["items"]:
                                comment = item["snippet"]["topLevelComment"]
                                comment_information={
                                "video_id":video_id,   
                                "Comment_Id":comment['id'],
                                "Comment_Author":comment["snippet"]["authorDisplayName"],
                                "Comment_Text":comment["snippet"]["textDisplay"],
                                "Comment_PublishedAt": comment['snippet']['publishedAt']
                                }
                                #video_information['comment_information'] = comment_information
                                #channel_information['video_information'] = video_information
                                #channels['channel_information']=channel_information
                                video_information[comment_information["Comment_Id"]] = comment_information
                                channel_information[video_information["video_id"]] = video_information
                                channels[channel_name]=channel_information
                            nextPageToken = comment_thread_response.get('nextPageToken')
                            if not nextPageToken:
                                break
                    else:
                        print('There is no comments for this video')
                nextPageToken = playlist_item_response.get('nextPageToken')
                if not nextPageToken:
                    break
        else:
            print('There is no uploaded videos playlist for this user.')
    except HttpError as e:
        print (f"An HTTP error {e.resp.status} occurred:\n{e.content}")
    return(channel_df,channels)  

#Function to upload data into MongoDB
def MongoDB_update(final_data):
    client=MongoClient("mongodb+srv://shilpa587:Guvi1234@cluster0.re3zowa.mongodb.net/?retryWrites=true&w=majority") #Establish Mongoclient connection
    db=client["Youtube"]
    collection = db["data"]
    collection.insert_one(final_data)
    client.close()
    st.write("Data has been successfully inserted into MongoDB Atlas!")

#Function to create a list of channel names for dropdown options in streamlit
def MongoDB_channels():
    client=MongoClient("mongodb+srv://shilpa587:Guvi1234@cluster0.re3zowa.mongodb.net/?retryWrites=true&w=majority")
    db=client["Youtube"]
    cursor = db.data.find()
    names = []
    for doc in cursor:
        for index,key in enumerate(doc):
            if index==1:
                names.append(doc[key]['channel_name'])
    return names,db

#Function to create MySQL Table   
def create_table():
    mycursor = mydb.cursor()
    mycursor.execute("CREATE DATABASE IF NOT EXISTS yt")
    mycursor.execute("USE yt")
    mycursor.execute('''
    CREATE TABLE IF NOT EXISTS Channel (
    channel_id VARCHAR(255) PRIMARY KEY, 
    channel_name VARCHAR(255),
    subscription_Count INTEGER, 
    channel_Views INTEGER, 
    channel_description TEXT,
    playlist_id VARCHAR(255)
    )''')

    mycursor.execute('''     
    CREATE TABLE IF NOT EXISTS Playlist (
    playlist_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(255) REFERENCES Channel(channel_id)
    )''')
    
    mycursor.execute('''
    CREATE TABLE IF NOT EXISTS Video (
    video_id VARCHAR(255) PRIMARY KEY, 
    playlist_id VARCHAR(255) REFERENCES Playlist(playlist_id),
    video_name VARCHAR(255), 
    video_description TEXT, 
    published_date DATETIME, 
    view_count INTEGER, 
    like_count INTEGER, 
    favorite_count INTEGER, 
    comment_count INTEGER, 
    duration INTEGER, 
    thumbnail VARCHAR(255), 
    caption_status VARCHAR(255)
    )''')
    
    mycursor.execute( '''
    CREATE TABLE IF NOT EXISTS Comment (
    comment_id VARCHAR(255) PRIMARY KEY, 
    video_id VARCHAR(255) REFERENCES Videos(video_id), 
    comment_text TEXT, 
    comment_author TEXT, 
    comment_published_at DATETIME
    )''')


#Streamlit deployment page setup
if __name__ == '__main__':
    youtube = get_api_service()
st.set_page_config(page_title='Youtube Data Extractor',layout="wide")
with st.sidebar:                                     
    selected = option_menu(    menu_title='Youtube Data Extractor',
                               options=["View Channel Details and upload to MongoDB",'SQL Migration','Analyse Channel Data'],
                               default_index=0,
                           )
if selected == "View Channel Details and upload to MongoDB":
    st.title('View Channel Details')
    channel_id_ip=st.text_input("Please enter channel ID")
    if channel_id_ip and st.button("GET DOCUMENT"):
        channel_info,channel_data=api_extraction(channel_id_ip)
        st.write(channel_info)
    if st.button("Upload to MongoDb", key="2"):
        channel_info,channel_data=api_extraction(channel_id_ip)
        MongoDB_update(channel_data)
elif selected == 'SQL Migration':
    st.title('SQL Migration')
    channel_dropdowns,database=MongoDB_channels()
    mongo_documents=database.data.find()
    Channel_name=st.selectbox("Select channel ID",channel_dropdowns)
    create_table()
    mycursor = mydb.cursor()
    mycursor.execute("USE yt")
    if st.button("Migrate to SQL", key="3"):
        for doc in (mongo_documents):
            for item in doc:
                if item==Channel_name:
                    sql="INSERT INTO Channel(channel_id,channel_name,subscription_Count,channel_Views,channel_description,playlist_id) VALUES(%s,%s,%s,%s,%s,%s)"
                    val=(doc[item]['Channel_Id'],doc[item]['channel_name'],doc[item]['Subscription_Count'],doc[item]['Channel_Views'],doc[item]['channel_description'],doc[item]['playlist_id'])
                    mycursor.execute(sql, val)
                    mycursor.execute("INSERT INTO Playlist(playlist_id,channel_id) VALUES(%s,%s)",(doc[item]['playlist_id'],doc[item]['Channel_Id']))
                    for key in doc[item]:
                        if key not in ['channel_name','Channel_Id','Subscription_Count','Channel_Views','channel_description','playlist_id']:
                            mycursor.execute("INSERT INTO Video(video_id,playlist_id,video_name,video_description,published_date,view_count,like_count,favorite_count,comment_count,duration,thumbnail,caption_status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(doc[item][key]['video_id'],doc[item][key]['playlist_id'],doc[item][key]['Video_Name'],doc[item][key]['Video_Description'],datetime.strptime(doc[item][key]['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ'),doc[item][key]['View_Count'],doc[item][key]['Like_Count'],doc[item][key]['Favorite_Count'],doc[item][key]['Comment_Count'],int(parse_duration(doc[item][key]['Duration']).total_seconds()),doc[item][key]['Thumbnail'],doc[item][key]['Caption_Status']))                        #if doc[item][key]['playlist_id']:
                            for each in doc[item][key]:
                                if each not in ['video_id','playlist_id','Video_Name','Video_Description','PublishedAt','View_Count','Like_Count','Favorite_Count','Comment_Count','Duration','Thumbnail','Caption_Status']:     
                                    mycursor.execute("INSERT INTO Comment(comment_id,video_id,comment_text,comment_author,comment_published_at) VALUES(%s,%s,%s,%s,%s)",(doc[item][key][each]['Comment_Id'],doc[item][key][each]['video_id'],doc[item][key][each]['Comment_Author'],doc[item][key][each]['Comment_Text'],datetime.strptime(doc[item][key][each]['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')))
                    st.write("Data has been successfully Migrated to MySql!")                                    
    mycursor.execute("commit;")
elif selected=="Analyse Channel Data":
    mycursor = mydb.cursor()
    mycursor.execute("USE yt")
    questions = ["What are the Names of all the videos and their corresponding channels?",
                   "Which channels have the most number of videos, and how many videos do they have?",
                   "What are the top 10 most viewed videos and their respective channels ?",
                   "How many comments were made on each video, and what are their corresponding video names?",
                   "Which videos have the highest number of likes, and what are their corresponding channel names?",
                   "What is the total number of likes and views for each video, and what are  their corresponding video names?",
                   "What is the total number of views for each channel, and what are their corresponding channel names?",
                   "What are the names of all the channels that have published videos in the year 2022?",
                   "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                   "Which videos have the highest number of comments, and what are their corresponding channel names?"]
    Query = st.selectbox('Select Question', questions)
    if Query == "What are the Names of all the videos and their corresponding channels?":
        if st.button("ANSWER"):
            SQL_query = "select video.video_name,channel.channel_name from channel inner join video on channel.playlist_id = video.playlist_id order by channel.channel_name"
            mycursor.execute(SQL_query)
            data = [i for i in mycursor.fetchall()]
            st.dataframe(
                    pd.DataFrame(data, columns=["Video", "Channel"], index=range(1, len(data) + 1)))
    elif Query == "Which channels have the most number of videos, and how many videos do they have?":
            if st.button("ANSWER"):
                SQL_query = "select channel_name,COUNT(*) as video_count from video inner join channel on channel.playlist_id = video.playlist_id group by video.playlist_id,channel_name ORDER BY video_count desc limit 1"
                mycursor.execute(SQL_query)
                st.write("Channels with most number of videos ")
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Channel Name", "Total Videos"], index=range(1, len(data) + 1)))
    elif Query == "What are the top 10 most viewed videos and their respective channels ?":
            if st.button("ANSWER"):
                SQL_query = "select video_name,view_count,channel_name from channel inner join video on channel.playlist_id = video.playlist_id order by view_count desc LIMIT 10"
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Video", "View Count","Channel Name"], index=range(1, len(data) + 1)))
    elif Query == "How many comments were made on each video, and what are their corresponding video names?":
            if st.button("ANSWER"):
                SQL_query="select video_name,comment_count from video order by comment_count desc"
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Video", "comment_count"], index=range(1, len(data) + 1)))
    elif Query == "Which videos have the highest number of likes, and what are their corresponding channel names?":
            if st.button("ANSWER"):
                SQL_query="select video_name,like_count from video order by like_count desc"
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Video", "Like Count"], index=range(1, len(data) + 1)))
    elif Query == "What is the total number of likes and views for each video, and what are  their corresponding video names?":
            if st.button("ANSWER"):
                SQL_query="select video_name,like_count,view_count from video order by like_count desc"   
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Video", "Like Count","View Count"], index=range(1, len(data) + 1)))
    elif Query == "What is the total number of views for each channel, and what are their corresponding channel names?":
            if st.button("ANSWER"):
                SQL_query="select channel_name,channel_Views from channel order by channel_Views desc"   
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Channel","Channel Views"], index=range(1, len(data) + 1)))
    elif Query == "What are the names of all the channels that have published videos in the year 2022?":
            if st.button("ANSWER"):
                SQL_query="select channel_name,published_date from channel inner join video on channel.playlist_id=video.playlist_id where published_date like '2022%'"   
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Channel Name","Published Date"], index=range(1, len(data) + 1)))
    elif Query == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            if st.button("ANSWER"):
                SQL_query="select channel_name,avg(duration) from channel inner join video on channel.playlist_id=video.playlist_id group by channel_name order by avg(duration) desc"   
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Channel Name","Avg Duration"], index=range(1, len(data) + 1)))
    elif Query == "Which videos have the highest number of comments, and what are their corresponding channel names?":
            if st.button("ANSWER"):
                SQL_query="select video_name,comment_count,channel_name from channel inner join video on channel.playlist_id=video.playlist_id order by comment_count desc"   
                mycursor.execute(SQL_query)
                data = [i for i in mycursor.fetchall()]
                st.dataframe(
                        pd.DataFrame(data, columns=["Video Name","Comment Count","Channel Name"], index=range(1, len(data) + 1)))                