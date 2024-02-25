from googleapiclient.discovery import build
from pprint import pprint
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import dateutil
from datetime import *
from time import strftime

def Apiconnection():
    Api_Id="AIzaSyD5qcbpDTrQuZMn8PCWzbY0GLOIG7A-l9o"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube
youtube = Apiconnection()

def channeldetails(channelid):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channelid
    )
    response = request.execute()
    for i in response['items']:
        data=dict(
            channel_Name = i['snippet']['title'],
            channel_Id = i['id'],
            channel_desc = i['snippet']['description'],
            channel_published = i['snippet']['publishedAt'],
            channel_subcount = i['statistics']['subscriberCount'],
            channel_videocount = i['statistics']['videoCount'],
            channel_viewcount = i['statistics']['viewCount'],
            channel_playlistId = i['contentDetails']['relatedPlaylists']['uploads']
        )
    return data

def videosids(channelid):
    videos_ids = []
    response = youtube.channels().list(id=channelid, part='contentDetails').execute()
    playlistid = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1=youtube.playlistItems().list(
            part = 'snippet',
            playlistId = playlistid,
            maxResults = 50,
            pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            videos_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return videos_ids

def videosdetails(videosids):
    videos_data = []
    for videoid in videosids:
        request = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = videoid
        )
        response = request.execute()
        for i in response["items"]:
            data = dict(
                channel_Name = i['snippet']['channelTitle'],
                channel_Id = i['snippet']['channelId'],
                video_Id = i['id'],
                video_title = i['snippet']['title'],
                video_tags = i['snippet'].get('tags'),
                video_thumbnail = i['snippet']['thumbnails']['default']['url'],
                video_desc = i['snippet'].get('description'),
                video_publisheddate = i['snippet']['publishedAt'],
                video_duration = i['contentDetails']['duration'],
                video_views = i['statistics'].get('viewCount'),
                video_likes = i['statistics'].get('likeCount'),
                video_comments = i['statistics'].get('commentCount'),
                video_favoritecount = i['statistics']['favoriteCount'],
                video_definition = i['contentDetails']['definition'],
                video_caption = i['contentDetails']['caption']
                )
        videos_data.append(data)
    return videos_data

def commentsdetails(videoids):
    comments_data = []
    try:
        for videoid in videoids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = videoid,
                maxResults = 50
            )
            response = request.execute()
            for i in response['items']:
                data = dict(
                    comment_id = i['snippet']['topLevelComment']['id'],
                    video_id = i['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_Author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_publishedat = i['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comments_data.append(data)
    except:
        pass
    return comments_data

def playlistdetails(channelid):
    next_page_token = None
    playlist_data = []
    while True:
        request = youtube.playlists().list(
            part = 'snippet, contentDetails',
            channelId = channelid,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()
        for i in response['items']:
            data = dict(
                Playlist_Id = i['id'],
                Playlist_Title = i['snippet']['title'],
                channel_id = i['snippet']['channelId'],
                channel_name = i['snippet']['channelTitle'],
                playlist_published = i['snippet']['publishedAt'],
                playlist_videocount = i['contentDetails']['itemCount']
            )
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data


# dbconnection = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
# db = dbconnection['Youtube_Data']
dbconnection = pymongo.MongoClient("mongodb+srv://trivenisyamala:triveni@cluster0.aaqjse5.mongodb.net/")
db = dbconnection["Youtube_datalist"]



#data inertion to mongodb and mysql
def channel_details(channelid):
    ch_info = channeldetails(channelid)
    pl_info = playlistdetails(channelid)
    vi_ids = videosids(channelid)
    vi_info = videosdetails(vi_ids)
    com_info = commentsdetails(vi_ids)
    collection1 = db['Channel_Details']
    collection1.insert_one({
        'Channel_Information':ch_info,
        'Playlist_Informatiion':pl_info,
        'Video_Information':vi_info,
        'Comment_Information':com_info})
    return 'Data uploaded successfully'

def channels_table():
    sqlconnection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Syamala@9666',
        database='Youtube_datalist'
    )
    cursor = sqlconnection.cursor()
    
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    sqlconnection.commit()
    
    try:
        query = """create table if not exists channels (channel_Name varchar(100),
                                                       channel_Id varchar(100) primary key,
                                                       channel_desc text,
                                                       channel_subcount bigint,
                                                       channel_videocount int,
                                                       channel_viewcount bigint,
                                                       channel_playlistId varchar(100))"""
        cursor.execute(query)
        sqlconnection.commit()
    
    except:
        print("channels table already created")
    
    ch_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for ch_data in collection1.find({},{'_id':0, 'Channel_Information':1}):
        ch_list.append(ch_data['Channel_Information'])
    df = pd.DataFrame(ch_list)
    
    for index,row in df.iterrows():
        query = """INSERT into channels(channel_Name,
                                        channel_Id,
                                        channel_desc,
                                        channel_subcount,
                                        channel_videocount,
                                        channel_viewcount,
                                        channel_playlistId)
                                VALUES(%s,%s,%s,%s,%s,%s,%s)"""
        values = (row['channel_Name'],
                  row['channel_Id'],
                  row['channel_desc'],
                  row['channel_subcount'],
                  row['channel_videocount'],
                  row['channel_viewcount'],
                  row['channel_playlistId'])
        try:
            cursor.execute(query,values)
            sqlconnection.commit()
        except:
            print("Channel values are inserted already")

def playlists_table():
    sqlconnection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Syamala@9666',
        database='Youtube_datalist'
    )
    cursor = sqlconnection.cursor()
    
    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    sqlconnection.commit()
    
    try:
        query = """create table if not exists playlists (Playlist_Id varchar(100) primary key,
                                                       Playlist_Title varchar(300),
                                                       channel_id varchar(100),
                                                       channel_name varchar(100),
                                                       playlist_videocount int
                                                       )"""
        cursor.execute(query)
        sqlconnection.commit()
    
    except:
        print("playlists table already created")

    pl_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for pl_data in collection1.find({},{'_id':0, 'Playlist_Informatiion':1}):
        for i in range(len(pl_data['Playlist_Informatiion'])):
            pl_list.append(pl_data['Playlist_Informatiion'][i])
    df1 = pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        query = """INSERT into playlists(Playlist_Id,
                                        Playlist_Title,
                                        channel_id,
                                        channel_name,
                                        playlist_videocount)
                                VALUES(%s,%s,%s,%s,%s)"""
        values = (row['Playlist_Id'],
                  row['Playlist_Title'],
                  row['channel_id'],
                  row['channel_name'],
                  row['playlist_videocount'])
        try:
            cursor.execute(query,values)
            sqlconnection.commit()
        except:
            print("Playlist values are inserted already")

def videos_table():
    sqlconnection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Syamala@9666',
        database='Youtube_datalist'
    )
    cursor = sqlconnection.cursor()
    
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    sqlconnection.commit()
    try:
        query = """create table if not exists videos (channel_Name varchar(100),
                                                    channel_Id varchar(100),
                                                    video_Id varchar(50) primary key,
                                                    video_title varchar(300),
                                                    video_thumbnail varchar(200),
                                                    video_desc text,
                                                    video_views bigint,
                                                    video_likes bigint,
                                                    video_comments int,
                                                    video_duration time,
                                                    video_favoritecount int,
                                                    video_definition varchar(20),
                                                    video_caption varchar(20),
                                                    video_publisheddate DATETIME
                                                    )"""
        cursor.execute(query)
        sqlconnection.commit()
    except:
        print("videos table already created")
        
    vi_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for vi_data in collection1.find({},{'_id':0, 'Video_Information':1}):
        for i in range(len(vi_data['Video_Information'])):
            vi_list.append(vi_data['Video_Information'][i])
    df2 = pd.DataFrame(vi_list)
    
    for index,row in df2.iterrows():
        query = """INSERT into videos(channel_Name,
                                        channel_Id,
                                        video_Id,
                                        video_title,
                                        video_thumbnail,
                                        video_desc,
                                        video_views,
                                        video_likes,
                                        video_comments,
                                        video_duration,
                                        video_favoritecount,
                                        video_definition,
                                        video_caption,
                                        video_publisheddate
                                        )
                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        
        from dateutil import parser

        d = parser.parse(row['video_publisheddate']).astimezone(timezone.utc)

        
        # d = datetime.fromisoformat(row['video_publisheddate']).astimezone(timezone.utc)
        row['video_publisheddate'] = d.strftime('%Y-%m-%d %H:%M:%S')
        d1 = dateutil.parser.parse(row['video_duration'][2:])
        row['video_duration'] = d1.strftime('%H:%M:%S')
        
        values = (row['channel_Name'],
                  row['channel_Id'],
                  row['video_Id'],
                  row['video_title'],
                  row['video_thumbnail'],
                  row['video_desc'],
                  row['video_views'],
                  row['video_likes'],
                  row['video_comments'],
                  row['video_duration'],
                  row['video_favoritecount'],
                  row['video_definition'],
                  row['video_caption'],
                 row['video_publisheddate'])
        try:
            cursor.execute(query,values)
            sqlconnection.commit()
        except:
            print("videos values are inserted already")

def comments_table():
    sqlconnection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Syamala@9666',
        database='Youtube_datalist'
    )
    cursor = sqlconnection.cursor()
    
    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    sqlconnection.commit()
    
    try:
        query = """create table if not exists comments (comment_id varchar(100) primary key,
                                                        video_id varchar(50),
                                                        comment_text text,
                                                        comment_Author varchar(200)
                                                       )"""
        cursor.execute(query)
        sqlconnection.commit()
    
    except:
        print("comments table already created")
        
    com_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for com_data in collection1.find({},{'_id':0, 'Comment_Information':1}):
        for i in range(len(com_data['Comment_Information'])):
            com_list.append(com_data['Comment_Information'][i])
    df3 = pd.DataFrame(com_list)
    
    for index,row in df3.iterrows():
        query = """INSERT into comments(comment_id,
                                        video_id,
                                        comment_text,
                                        comment_Author
                                        )
                                VALUES(%s,%s,%s,%s)"""
        values = (row['comment_id'],
                  row['video_id'],
                  row['comment_text'],
                  row['comment_Author']
                  )
        try:
            cursor.execute(query,values)
            sqlconnection.commit()
        except:
            print("comments values are inserted already")

def datatables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables created successfully"

def view_channels_table():
    ch_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for ch_data in collection1.find({},{'_id':0, 'Channel_Information':1}):
        ch_list.append(ch_data['Channel_Information'])
    df = st.dataframe(ch_list)
    return df

def view_playlists_table():
    pl_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for pl_data in collection1.find({},{'_id':0, 'Playlist_Informatiion':1}):
        for i in range(len(pl_data['Playlist_Informatiion'])):
            pl_list.append(pl_data['Playlist_Informatiion'][i])
    df1 = st.dataframe(pl_list)
    return df1

def view_videos_table():
    vi_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for vi_data in collection1.find({},{'_id':0, 'Video_Information':1}):
        for i in range(len(vi_data['Video_Information'])):
            vi_list.append(vi_data['Video_Information'][i])
    df2 = st.dataframe(vi_list)
    return df2

def view_comments_table():
    com_list = []
    db = dbconnection['Youtube_datalist']
    collection1 = db['Channel_Details']
    for com_data in collection1.find({},{'_id':0, 'Comment_Information':1}):
        for i in range(len(com_data['Comment_Information'])):
            com_list.append(com_data['Comment_Information'][i])
    df3 = st.dataframe(com_list)
    return df3

with st.sidebar:
    st.title(":blue[Youtube Data Harvesting and Warehousing]")
    st.header("Contents")
    st.caption("Python")
    st.caption("Data collection")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and MySQL")

channel_id = st.text_input("Enter Channel ID")

if st.button("collect and store data"):
    ch_ids = []
    db = dbconnection["Youtube_datalist"]
    collection1 = db["Channel_Details"]
    for ch_data in collection1.find({},{"_id":0, "Channel_Information":1}):
        ch_ids.append(ch_data["Channel_Information"]["channel_Id"])
    if channel_id in ch_ids:
        st.success("Channel details of the given channel id already present")
    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Insert to MySQL"):
    Tables = datatables()
    st.success(Tables)

select_table = st.radio("Select Table",("Channels","Playlists","Videos","Comments"))

if select_table == "Channels":
    view_channels_table()
elif select_table == "Playlists":
    view_playlists_table()
elif select_table == "Videos":
    view_videos_table()
elif select_table == "Comments":
    view_comments_table()

sqlconnection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Syamala@9666',
    database='Youtube_datalist'
)
cursor = sqlconnection.cursor(buffered=True)

question = st.selectbox("Choose your question", ("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question == "1. What are the names of all the videos and their corresponding channels?":
    question1 = """select video_title as videos,channel_Name as channelname from videos"""
    cursor.execute(question1)
    sqlconnection.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns = ["video title", "channel name"])
    st.write(df)

elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
    question2 = """select channel_Name as channelname, channel_videocount as total_videocount from channels
                    order by channel_videocount desc"""
    cursor.execute(question2)
    sqlconnection.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns = ["Channel Name", "Total videos count"])
    st.write(df2)

elif question == "3. What are the top 10 most viewed videos and their respective channels?":
    question3 = """select video_views as viewscount, channel_Name as channelname, video_title as videotitle from videos
                    where video_views is not null
                    order by video_views desc
                    limit 10"""
    cursor.execute(question3)
    sqlconnection.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3,columns = ["Views Count", "Channel Name", "Video Title"])
    st.write(df3)

elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
    question4 = """select video_comments as commentcount, video_title as videotitle from videos
                    where video_comments is not null"""
    cursor.execute(question4)
    sqlconnection.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4,columns = ["Comments Count", "Video Title"])
    st.write(df4)

elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    question5 = """select video_title as videotitle, channel_Name as channelname, video_likes as likecount from videos
                    where video_likes is not null
                    order by video_likes desc"""
    cursor.execute(question5)
    sqlconnection.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5,columns = ["Video Title", "Channel Name", "Likes count"])
    st.write(df5)

elif question == "6. What is the total number of likes for each video, and what are their corresponding video names?":
    question6 = """select video_likes as likecount, video_title as videotitle from videos"""
    cursor.execute(question6)
    sqlconnection.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6,columns = ["Likes count", "Video Title"])
    st.write(df6)

elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    question7 = """select channel_viewcount as viewcount, channel_Name as channelname from channels"""
    cursor.execute(question7)
    sqlconnection.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7,columns = ["Views Count", "Channel Name"])
    st.write(df7)

elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
    question8 = """select channel_Name as channelname, video_title as videotitle, video_publisheddate as publishedat from videos
                    where extract(year from video_publisheddate)=2022"""
    cursor.execute(question8)
    sqlconnection.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8,columns = ["Channel Name", "Video Title", "Published Date"])
    st.write(df8)
    
elif question == "9. What is the average duration of all videos in each channel and what are their corresponding channel names?":
    question9 = """select channel_Name as channelname, AVG(video_duration) as videoduration from videos
                    group by channel_Name"""
    cursor.execute(question9)
    sqlconnection.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9,columns = ["Channel Name", "Average Duration"])
    
    T9 = []
    for index,row in df9.iterrows():
        channel_Name = row["Channel Name"]
        average_duration = row["Average Duration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channelname=channel_Name, avg_duration = average_duration_str))
        dfavg=pd.DataFrame(T9)
    st.write(dfavg)
    
elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    question10 = """select video_title as videotitle, channel_Name as channelname, video_comments as commentscount from videos
                    where video_comments is not null 
                    order by video_comments desc"""
    cursor.execute(question10)
    sqlconnection.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10,columns = ["Video Title", "Channel Name", "Number of comments"])
    st.write(df10)