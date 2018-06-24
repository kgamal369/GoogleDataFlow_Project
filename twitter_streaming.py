#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
access_token = "328076065-rlsM7Hd5Gs4I2nnn4CUS99pWwPuQMCKcowQbWtUN"
access_token_secret = "6H3inYJ0Zq04HSfeb2TDi5izM8JpszFJuBvmWu2yiMnrC"
consumer_key = "u5WXQd7fK8mJb7JDUE6Gfz91x"
consumer_secret = "6CgBKoUcvJJQO4oxIpw9bqxqgItbqA8BtxUFvJXP34iYoSLRPP"
owner= "EngKarim369"
owner_ID="328076065"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['python', 'javascript', 'ruby'])