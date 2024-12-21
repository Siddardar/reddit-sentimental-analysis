
    scraper = RedditScraper()
    posts = scraper.fetch_top_posts_day("wallstreetbets")
    valid_posts = scraper.valid_posts(posts)

    sentiment = SentimentalAnalysis()
    analyzed_posts = sentiment.analyze_posts(valid_posts)    
    