from fuzzywuzzy import fuzz
import html
import requests
import json
import tweepy
import boto3
import os
import random
# import pygsheets
from random import randint
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from datetime import datetime,timedelta,date
from decimal import Decimal

rules = [['i m ', "i'm "],['i ll ', "i'll "],
         ['c mon ', "c'mon "],
         ['it s ', "it's "],
         ['ain t ', "ain't "],
         ['didn t ', "didn't "],
         ['don t ', "don't "],[' don t', " don't"],
         ['doesn t ', "doesn't "],
         ['qu ', "qu'"], ['O Clock', "O'Clock"], [' re ', "'re "],
         ['can t ', "can't "], ['i ve ', "i've "], [' s ', "'s "], [" l a", " l'a"], [" l e", " l'e"],
         [" s e", " s'e"], [" d e", " d'e"], [" n e", " n'e"], [" l i", " l'i"], [" j i", " j'i"],
         [" j a", " j'a"], [" l o", " l'o"], [" l u", " l'u"], [" t a", " t'a"], [" t i", " t'i"],
         [" t e", " t'e"], [" m a", " m'a"], [" m e", " m'e"],
         [" m i", " m'i"], [" c e", " c'e"],[" d a", " d'a"],[" d un", " d'un"],[" j e", " j'e"],[" j i", " j'i"],[" j p", " j'p"],[" j h", " j'h"],
        ]


SP_client_id = os.getenv('SP_client_id')
SP_client_secret = os.getenv('SP_client_secret')
refresh_token = os.getenv('refresh_token')
TWITTER_APP_KEY = os.getenv('TWITTER_APP_KEY')
TWITTER_APP_SECRET = os.getenv('TWITTER_APP_SECRET')
playlistname=os.getenv('playlistname')
playlistmax=int(os.getenv('playlistmax'))
gscripturl=os.getenv('gscripturl')

##define dynamo table used to store tweets
dynamodb = boto3.resource('dynamodb', region_name='eu-west-1', endpoint_url="https://dynamodb.eu-west-1.amazonaws.com")
tweets_table = dynamodb.Table('FipDB')

##find last tweet stored in db
def getlastdbtweet():
    for i in range(15):
        d=int((datetime.today()-timedelta(days=i)).strftime('%Y%m%d'))
        q=tweets_table.query(KeyConditionExpression=Key('date').eq(d),ScanIndexForward=False,Limit=1)['Items']
        if q: return q[0]['id']
    return 1

##get new tweets
def getnewtweets(lastdbtweetid):
    auth = tweepy.AppAuthHandler(TWITTER_APP_KEY,TWITTER_APP_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    new_tweets=[]
    for status in tweepy.Cursor(api.user_timeline,id='2211149702',since_id=lastdbtweetid,tweet_mode='extended').items(5 if lastdbtweetid==1 else 0):
        if  'nowplaying' in status.full_text and 'Fip Actualite' not in status.full_text:
	        new_tweets.append({'id':status.id,'text':status.full_text,'tweet_time':status.created_at,'date':int(status.created_at.date().strftime('%Y%m%d')),'time':int(status.created_at.time().strftime('%H%M%S'))})
    return list(reversed(new_tweets))

def save_new_tweet(t):
        try:
                t2=t.copy()
                t2={k:Decimal(str(v)) if isinstance(v, float) else v for k,v in t.items()}
                for k in ['tweet_time','tweet_song','tweet_album','tweet_artist']:t2.pop(k, None)
                
                tweets_table.put_item(
                    Item=t2,
                    ConditionExpression='attribute_not_exists(id)'
                    )
        except ClientError as e:
                print(e.response['Error']['Message'])

## add song attributes to tweet objects
def cleaner(s):
    if s is not None:
        s = (' ' + s).lower()
        for r in rules:
            s = s.lower().replace(r[0], r[1])
        return html.unescape(s.strip())
    else:
        return None

def texttosong(tweet):
    a = str(tweet['text']).split("nowplaying ")[1].split(' http')[0].strip()
    if a[-5:-1].isdigit():
        b = [[i, c] for i, c in enumerate(a) if c in ('(', ')')]
        limit = [b[i]
                 for i in range(-2, -len(b) - 1, -2) if b[i][1] == '('][0][0]
        album = a[limit:][1:]
    elif a.count('(') > a.count(')'):
        b = [[i, c] for i, c in enumerate(a) if c in ('(', ')')]
        limit = [b[i]
                 for i in range(-1, -len(b) - 1, -2) if b[i][1] == '('][0][0]
        album = a[limit:][1:]
    else:
        album = None
        limit = len(a)
    sa = a[:limit].strip()
    song = sa.split(' - ')[0]
    artist = sa.split(' - ')[1] if ' - ' in sa else None

    #second cleaning
    song = song.split('(')[0] if song[0] != '(' else song

    if album is not None:
        for i in ['bof / ','b.o.f./','bof tv /','bof/','b.o.f. /','bo /','bo']:
          if album.lower().startswith(i):album=album[len(i):]
        album = album.split(' - ')[0].split(' (')[0].split('/')[0].strip()
    if artist is not None:
        for i in ['bo/','bof /','bof/']:
          if artist.lower().startswith(i):artist=artist[len(i):]
        artist = artist.split(' - ')[0].split(' (')[0].split('/')[0].strip()
    tweet['tweet_song']=cleaner(song)
    tweet['tweet_artist']=cleaner(artist)
    tweet['tweet_album']=cleaner(album)

## spotify functions
def spotifyconnect():
    spotifyurl="https://accounts.spotify.com/api/token"
    spotifydata = {'grant_type': 'refresh_token', 'refresh_token':refresh_token}
    r = requests.post(spotifyurl, data=spotifydata, auth=(SP_client_id, SP_client_secret))
    if r.status_code!=200: print('spotifyconnect error: '+ r.text.rstrip('\n'))

    token=r.json()['access_token']
    headers = {"Authorization":"Bearer "+token}
    return headers

def spotify_search(params):
    r=requests.get(
    'https://api.spotify.com/v1/search',
    headers=headers,
    params={ 'q': params, 'type': 'track' }
    )
    matches=[]
    if not r.json().get('tracks'):print(r.text.rstrip('\n'))
    for i in r.json()['tracks']['items']:
        m={'songid': i['id'],'uri': i['uri'], 'song': i['name'], 'songpop': i['popularity'], 'artist': ','.join([a['name'] for a in i['artists']]), 'artistid': ','.join([a['id'] for a in i['artists']]), 'album': i['album']['name'], 'albumid': i['album']['id'],'albumdate': i['album']['release_date']}
        if 'karaoke' not in (m['album']+m['artist']+m['song']).lower():matches.append(m)
    return matches

def scored(tweet,search):
    for i in search:i['score'] = [fuzz.WRatio(tweet['tweet_song'], i['song']),fuzz.WRatio(tweet['tweet_artist'], i['artist']),fuzz.WRatio(tweet['tweet_album'], i['album']),i['songpop'],len(search)]
    search = sorted(search, key=lambda i: (i['score'][0],i['score'][1], i['score'][2], i['songpop']),reverse=True)
    search[0]['score']='/'.join(str(e) for e in search[0]['score'])
    return search[0]

def spotify_match(tweet):
    search=spotify_search(f"track:{tweet['tweet_song']}" + f" artist:{tweet['tweet_artist']}" if tweet['tweet_artist'] else '' + f" album:{tweet['tweet_album']}" if tweet['tweet_album'] else '')
    if len(search)>0:
        return scored(tweet,search)
    if tweet['tweet_album'] and tweet['tweet_artist'] and not search:
        search=spotify_search(f"track:{tweet['tweet_song']}" + f" album:{tweet['tweet_album']}" )
        if len(search)>0:
            return scored(tweet,search)
    u=str(tweet['text']).split("nowplaying ")[1].split(' http')[0].strip()
    print(f"unmatched: {u}")
    return{}

def spotify_audiofeatures(tweet):
    r=requests.get(
    f"https://api.spotify.com/v1/audio-features/{tweet['songid']}",
    headers=headers,
    )
    r=r.json()
    [r.pop(k, None) for k in ("type","id",'track_href','uri','analysis_url') ]
    return r

def get_spotify_playlist(name):
    r=requests.get(
    'https://api.spotify.com/v1/users/zecharlatan/playlists',
    headers=headers
    )
    ps=r.json()['items']
    return [{'name':p['name']
             ,'id':p['id']
             ,'uri':p['uri']
             ,'tracks':p['tracks']['total']
             ,'snapshot_id':p['snapshot_id']}
             for p in ps if p['name']==name][0]

def sendtoplaylist(list_uris,playlist):
    payload = {'uris': list_uris}
    url=f"https://api.spotify.com/v1/users/zecharlatan/playlists/{playlist['id']}/tracks"
    r=requests.post(url, headers=headers, data=json.dumps(payload))
    playlist['snapshot_id']=r.json().get('snapshot_id')
    return r.status_code

def removesongsplaylist(n,playlist):
    payload = {'positions': list(range(n)),'snapshot_id':playlist['snapshot_id']}
    url=f"https://api.spotify.com/v1/users/zecharlatan/playlists/{playlist['id']}/tracks"
    r=requests.delete(url, headers=headers, data=json.dumps(payload))
    return r.status_code

def getatweet():
    d=int((date(2018, 1, 1)+timedelta(days=randint(0, 50))).strftime('%Y%m%d'))
    global headers
    q=tweets_table.query(KeyConditionExpression=Key('date').eq(d))['Items']
    q2=[i for i in q if not i.get('songid')]
    if q2:
        tweet=random.choice(q2)
        print(tweet)
        texttosong(tweet)
        headers=spotifyconnect()
        tweet = {**tweet, **spotify_match(tweet)}
        if tweet.get('uri'):
            tweet = {**tweet, **spotify_audiofeatures(tweet)}
        else:
            tweet['songid']=0
        t2={k:Decimal(str(v)) if isinstance(v, float) else v for k,v in tweet.items()}
        for k in ['tweet_time','tweet_song','tweet_album','tweet_artist']:t2.pop(k, None)
        print(t2)
        tweets_table.put_item(Item=t2)








import timeit
def elapsed(ev=''):
    if hasattr(elapsed, 'start_time'):
        tp=(ev+': '+"%.2f" % (timeit.default_timer() - elapsed.start_time))
        print(tp)
        elapsed.start_time = timeit.default_timer()
        return(tp)
    else:
        elapsed.start_time = timeit.default_timer()

def lambda_handler(event, context):
    lastdbtweetid=getlastdbtweet()
    new_tweets=getnewtweets(lastdbtweetid)
    global headers
    if len(new_tweets)>0:
        for t in new_tweets: texttosong(t)
        headers=spotifyconnect()
        uri_list=[]
        tweetsgdrive=[]
        for tweet in new_tweets:
            tweet = {**tweet, **spotify_match(tweet)}
            if tweet.get('uri'):
                tweet = {**tweet, **spotify_audiofeatures(tweet)}
                uri_list.append(tweet['uri'])
            else:tweet['songid']=0
            save_new_tweet(tweet)
            tweetsgdrive.append(tweet)

        if uri_list:
            playlist=get_spotify_playlist(playlistname)
            sendtoplaylist(uri_list,playlist)
            if (playlist['tracks']+len(uri_list))>playlistmax:
                n=playlist['tracks']+len(uri_list)-playlistmax
                removesongsplaylist(n,playlist)
        elapsed()

        for tweet in tweetsgdrive:
            l=[i.strftime('%d-%m-%Y %H:%M:%S') if isinstance(i, (datetime)) else i for i in list(tweet.values())]
            r=requests.get(gscripturl,params={ 'q': l})
            print('gtime insert: ' + str(elapsed()))
    else:
        getatweet()
