from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


#consumer key, consumer secret, access token, access secret.
ckey="dyGcHiPx2U4l232B2n7EF3zHL"
csecret="TG3VABJjXh8CCJtoZPukj5SUfBItTDE3xdWtGui6NebNVu8feo"
atoken="948534702188716033-f91py9OPFYhXhTkys3R4AyLE047N6xI"
asecret="IpSXQzbO6O53KR9tAZFIY9sgNiBQ0ev3Rin9otEAoWcWv"

class listener(StreamListener):

    def on_data(self, data):
        print(data)
        return(True)

    def on_error(self, status):
        print (status)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["car"])