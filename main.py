import requests
import asyncio
from dotenv import set_key, load_dotenv
import os
import re
import yfinance as yf
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import datetime

class RedditScraper:
    def __init__(self):
        load_dotenv()

        self.client = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Authorization': f'bearer {os.environ.get("REDDIT_API")}'
        }
        self.pattern = r'\b[A-Z]{1,5}(?:\.[A-Z])?\b'

        self.invalid_tickers = {"AI"}
        
    def generate_secret(self) -> str:
        
        print("Generating new token")

        link = "https://www.reddit.com/api/v1/access_token"
        data = {
            "grant_type": "password",
            "username": os.environ.get("REDDIT_USERNAME"),
            "password": os.environ.get("REDDIT_PASSWORD")
        }

        auth = (os.environ.get("REDDIT_APP"), os.environ.get("REDDIT_APP_SECRET"))
        access_token = requests.post(link, data=data, headers=self.headers,auth=auth).json()['access_token']
        
        
        set_key(".env", "REDDIT_API", access_token)
        
        return access_token

    def fetch_top_posts_day(self, subreddit:str, top:int) -> list:
        
        print("Fetching top posts")

        link = f"https://oauth.reddit.com/r/{subreddit}/top?t=day&limit={top}"

        res = self.client.get(link, headers=self.headers)
        if res.status_code == 401:
            print("Token expired")

            secret = self.generate_secret()
            self.headers["Authorization"] = secret

            res = self.client.get(link, headers=self.headers)
            
            
        result = self.clean_data(res.json()['data']['children'])

        return result

    def clean_data(self, arr) -> list:
        
        cleaned_data = []
        for i in arr:
            data = i['data']

            if data["selftext"] == "" or data['ups'] <= 1:
                continue

            text = {
                "title": data["title"],
                "text": re.sub(r'\s+', ' ', re.sub(r'[^A-Za-z0-9 ]', '', data["selftext"])).strip(),    
                "upvotes": data["ups"],
                "link": f'reddit.com{data["permalink"]}'
            }
            cleaned_data.append(text)

        return cleaned_data
        
    def valid_posts(self, arr: list) -> list:
        print("Validating posts")   
        valid_posts = []

        for i in arr:
            match_title = re.findall(self.pattern, i['title'])
            match_text = re.findall(self.pattern, i['text'])    
            match = match_title[0] if match_title else match_text[0] if match_text else ""    

            if match == "" or self.validate_ticker(match) == False:
                continue
            
            i["ticker"] = match
            valid_posts.append(i)

        return valid_posts

    def validate_ticker(self, ticker) -> bool:
        if ticker in self.invalid_tickers:
            return False    
        
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if info['currentPrice']:
                return True
            else:
                self.invalid_tickers.add(ticker)
                return False
        except:
            self.invalid_tickers.add(ticker)
            return False

class SentimentalAnalysis:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()

    
    def anaylze(self, text: str) -> float:
        return self.analyzer.polarity_scores(text)['compound']
    
    def analyze_posts(self, posts: list) -> list:
        print("Analyzing posts")
        for i in posts:
            i["sentiment"] = self.anaylze(i["text"])
        return posts

class Trading:
    def __init__(self):
        database_url = os.environ.get("DB_URL")
        try:
            self.client = MongoClient(
                database_url, 
                server_api=ServerApi('1'))['quant_data']
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            raise

    def weighted_sentiment(self, posts: list) -> dict:
        hashmap = {}
        for i in posts:
            if i['ticker'] in hashmap:
                hashmap[i['ticker']]['links'].append(i['link'])
                hashmap[i['ticker']]['sentiment'] += i['sentiment'] * i['upvotes']
                hashmap[i['ticker']]['upvotes'] += i['upvotes']
            else:
                hashmap[i['ticker']] = {
                    'links': [i['link']],
                    'sentiment': i['sentiment'] * i['upvotes'],
                    'upvotes': i['upvotes']
                }

        for i in hashmap:
            hashmap[i]['sentiment'] = hashmap[i]['sentiment'] / hashmap[i]['upvotes']
            hashmap[i]['action'] = (
                'buy' if hashmap[i]['sentiment'] > 0 else
                'short' if hashmap[i]['sentiment'] < 0 else
                'hold'
            )

        sorted_hashmap = dict(sorted(hashmap.items(), key=lambda x: x[1]['upvotes'], reverse=True))
        return sorted_hashmap

    def create_voo(self, string: str) -> None:
        voo = {
            '_id': 'VOO',
            'shares': 0,
            'average_price': 0,
            'total_position': 0
        }

        self.client[string].insert_one(voo)  

    def database(self, stocks: dict, database: str, top:int) -> None: 
        print("Updating database")
        try:
            full_database_name = f'{database}_top{top}'
            collection = self.client[full_database_name]
            actions = self.client[f'{full_database_name}_actions']

            if (self.client[f'{full_database_name}_voo'].find_one({'_id': 'VOO'}) == None):
                self.create_voo(f'{full_database_name}_voo') 
            
            voo = self.client[f'{full_database_name}_voo']

        except Exception as e:
            print(f"Error accessing the database: {e}")

        update = {}

        for k, v in stocks.items():
            if (v['action'] == 'hold'):
                continue

            existing_ticker = collection.find_one({'_id': k})
            stock_price = self.get_ticker_price(k)
            money_spent = round(v['upvotes'] * stock_price, 2)  
            if existing_ticker != None:
                if (v['action'] == 'short'):
                    total_shares = existing_ticker['shares'] - v['upvotes']
                    if (total_shares < 0):
                        total_shares = 0
                    
                    update = {
                        'shares': total_shares,
                        'average_price': existing_ticker['average_price'],
                        'total_position': total_shares * existing_ticker['average_price'],
                    }


                else:
                    total_shares = existing_ticker['shares'] + v['upvotes'] 
                    new_average_price = (existing_ticker['average_price'] * existing_ticker['shares'] + money_spent) / total_shares

                    update = {
                        'shares': total_shares,
                        'average_price': new_average_price,
                        'total_position': total_shares * new_average_price,
                    }   
            else:
                if (v['action'] == 'short'):
                    continue
                update = {
                    '_id': k,
                    'shares': v['upvotes'],
                    'average_price': money_spent / v['upvotes'],
                    'total_position': money_spent,
                }

            collection.update_one({'_id': k}, {'$set': update}, upsert=True)

            action = {
                'ticker': k,
                'action': v['action'],
                'links': v['links'],
                'timestamp': datetime.date.today().strftime("%d-%m-%Y"),
                'sentiment': v['sentiment'],    
                'price': stock_price,
                'shares': v['upvotes'],     

            }

            actions.insert_one(action)      

            existing_voo = voo.find_one({'_id': 'VOO'})
            new_voo_shares = money_spent / self.get_ticker_price('VOO')
            
            if (v['action'] == 'short'):    
                update_voo = {
                    '_id': 'VOO',
                    'shares': existing_voo['shares'] - new_voo_shares,
                    'average_price': existing_voo['average_price'],
                    'total_position': (existing_voo['shares'] - new_voo_shares) * existing_voo['average_price']
                }   
            else:
                update_voo = {
                    '_id': 'VOO',
                    'shares': existing_voo['shares'] + new_voo_shares,
                    'average_price': (existing_voo['total_position'] + money_spent) / (existing_voo['shares'] + new_voo_shares),
                    'total_position': existing_voo['total_position'] + money_spent
                }

            voo.update_one({'_id': 'VOO'}, {'$set': update_voo}, upsert=True)   

    def get_ticker_price(self, ticker: str) -> float:
        stock = yf.Ticker(ticker)
        if (ticker == 'VOO'):
            print(stock.info)
            return stock.info['regularMarketOpen']

        return stock.info['currentPrice']
    
    def test(self):
        for i in self.client.list_collection_names():
            self.client[i].drop()


if __name__ == "__main__":

    subreddits = ["investing", "stocks", "wallstreetbets"]
    top_posts = 25

    scraper = RedditScraper()
    sentiment = SentimentalAnalysis()
    trading = Trading()

    for sub in subreddits:
    
        posts = scraper.fetch_top_posts_day(sub, top_posts)
        valid_posts = scraper.valid_posts(posts)

        analyzed_posts = sentiment.analyze_posts(valid_posts)
        
        weighted_sentiment = trading.weighted_sentiment(analyzed_posts)
        print(sub)
        for k, v in weighted_sentiment.items():    
            print(k, v['sentiment'], v['upvotes'], v['action'])
        
        print("")
        trading.database(weighted_sentiment, sub, top_posts)

    #print(Trading().get_ticker_price('VOO'))
    

