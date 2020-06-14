import mysql.connector
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from dateutil import parser
import time
global HOST
global USER
global PASSWD
global DATABASE
from textblob import TextBlob
def setdb(HOSTval,USERval,PASSWDval,DATABASEval):
    global HOST
    global USER
    global PASSWD
    global DATABASE
    HOST = HOSTval
    USER = USERval
    PASSWD = PASSWDval
    DATABASE = DATABASEval
def store_data(created_at, text, screen_name, tweet_id,sentiment):
    db=mysql.connector.connect(user=USER, password=PASSWD, host=HOST, database=DATABASE, port=3306)
    cursor = db.cursor(buffered=True)
    cursor.execute("select * from information_schema.tables where table_name=%s", ('tweet',))
    if(bool(cursor.rowcount)):
        print('inserting new tweet')
        insert_query = "INSERT INTO tweet (tweet_id, screen_name, created_at, tweet,sentiment) VALUES (%s, %s, %s, %s,%s)"
        print((tweet_id, screen_name, created_at, text,str(sentiment)))
        cursor.execute(insert_query, (tweet_id, screen_name, created_at, text,str(sentiment)))
        print('data is posted')
        db.commit()
        cursor.close()
        db.close()
        return
    else:
        cursor.execute("CREATE TABLE tweet (postid serial PRIMARY KEY NOT NULL , tweet_id varchar(200) NOT NULL , screen_name text NOT NULL, created_at text NOT NULL, tweet text NOT NULL, sentiment text NOT NULL);")
        insert_query = "INSERT INTO tweet (tweet_id, screen_name, created_at, tweet,sentiment) VALUES (%s, %s, %s, %s,%s)"
        cursor.execute(insert_query, (tweet_id, screen_name, created_at, text,str(sentiment)))
        print('data is posted')
        db.commit()
        cursor.close()
        db.close()
        return
class StdOutListener(StreamListener):
    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
        super(StdOutListener, self).__init__()
    def on_data(self, data):
        self.running = False
        # try:
        # Decode the JSON from Twitter
        datajson = json.loads((data))
        print('1')
        print(datajson)
        #grab the wanted data from the Tweet
        text = datajson['text']
        screen_name = datajson['user']['screen_name']
        tweet_id = datajson['id']
        created_at = parser.parse(datajson['created_at'])
        sentiment = TextBlob(text)
        sentiment = sentiment.sentiment.polarity
        # print(datajson)
        #print out a message to the screen that we have collected a tweet
        print("Tweet collected at " + str(created_at))
        #insert the data into the MySQL database
        store_data(created_at, text, screen_name, tweet_id,sentiment)
        # except Exception as e:
        #     print('error e',e)
    def on_error(self, status):
        print ('error status',status)
def server(consumer_key,consumer_secret,access_token,access_token_secret,values):
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener(1)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=values)


setdb("158.177.144.172","admin",".7475443aA","database")
server("bs0FEPgZigZmxkXhQtf7mdyPJ","bpTWMggY4OtZyq3ZbZ5ZZ0YVeNNcOsouQcV2rNPLalcs5SyVfb","3546809597-GxrmPZBEB8zfi2tZgqw3im2TAgY6AODwoibodNL","44nkAsQx7EOSXDdxzEUtiwtJQU4CWX0GTIgm6ks6GPaq0",['#blm'])
