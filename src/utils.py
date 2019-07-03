from time import strptime

# Wed Apr 17 23:59:55 +0000 2019
def get_tweet_month(tweet):
  date = tweet['created_at']

  slices = date.split(' ')
  month = slices[1]


  return strptime(month, '%b').tm_mon
