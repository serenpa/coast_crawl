# Python crawler for MongoDB
	
This simple crawler starts with a seed page and will continuously crawl pages linked to from this seed page as long as the pages are within the same top level domain. It writes the output to a Mongo database.
	
To use:
```python
import coast_crawl
coast_crawl.crawl(pages, db_url, db_client)
```

The seed_page variable is the url you want to begin crawling (for example, when crawling a blog, you'd probably want to use the sites 'archive' page), the optional db_url variable is the url to your mongo instance (defaulted to mongodb://localhost:27017) and the optional db_client variable is the name of the database within mongo that you want to use (defaulted to 'crawler_results').
	
Because state is persisted to a DB, any crash can be recovered from by simply just starting the server up again.
		
**WARNING: This crawler does not take into account the robots.txt file.**
	
## Future work/additions: 
* currently if a 404 response is given, the crawler moves onto the next link and continues. This isn't ideal if, for example, the internet connection temporarily drops. Some handling of this would be nice.
