import os
import time
import requests
from bs4 import BeautifulSoup
import html2text
from azure.cognitiveservices.search.websearch import WebSearchClient
from msrest.authentication import CognitiveServicesCredentials
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, Column, String, Text, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Initialize FastAPI instance
app = FastAPI()

# Database configuration
DATABASE_URL = "postgresql://localhost:Thakur@2001@localhost/WebCrawler"

# Initialize database connection
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define a table for storing extracted data
class ExtractedData(Base):
    __tablename__ = "extracted_data"

    id = Column(String, primary_key=True, index=True)
    title = Column(String)
    url = Column(String, unique=True, index=True)
    description = Column(Text)
    keywords = Column(Text)
    text_content = Column(Text)
    links = Column(Text)
    images = Column(Text)

Base.metadata.create_all(bind=engine)

# Azure Bing Search API credentials
subscription_key = "bdfedf9934d842d3aa95446942d878b3"
assert subscription_key
client = WebSearchClient(endpoint="https://api.bing.microsoft.com", credentials=CognitiveServicesCredentials(subscription_key))
client.config.base_url = "{Endpoint}/v7.0"

# Function to perform Bing search and return URLs as a list
def search_and_save_urls(query, num_results=5):
    web_data = client.web.search(query=query, count=num_results)
    print("\r\nSearched for Query: {}".format(query))

    urls = []
    if hasattr(web_data.web_pages, 'value'):
        print("\r\nWebpage Results#{}".format(len(web_data.web_pages.value)))
        for i in range(len(web_data.web_pages.value)):
            print("Name: {} ".format(web_data.web_pages.value[i].name))
            print("URL: {} ".format(web_data.web_pages.value[i].url))
            print("\r\n")
            urls.append(web_data.web_pages.value[i].url)
    else:
        print("Didn't find any web pages...")

    return urls

# Function to fetch data from a website
def get_data_from_website(url):
    max_retries = 3
    retries = 0

    while retries < max_retries:
        try:
            # Get response from the server with timeout set to 10 seconds
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            break  # Exit loop if successful
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return None, None, None, None
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Error connecting to the server: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Request error: {req_err}")

        retries += 1
        print(f"Retrying ({retries}/{max_retries})...")
        time.sleep(2 ** retries)  # Exponential backoff before retrying

    else:
        print(f"Failed to fetch {url} after {max_retries} retries")
        return None, None, None, None

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Removing script and style tags
    for script in soup(["script", "style"]):
        script.extract()

    # Extract text in markdown format
    html = str(soup)
    html2text_instance = html2text.HTML2Text()
    html2text_instance.images_to_alt = True
    html2text_instance.body_width = 0
    html2text_instance.single_line_break = True
    text = html2text_instance.handle(html)

    # Extract links (href attributes of <a> tags)
    links = [link.get('href') for link in soup.find_all('a')]

    # Extract images (src attributes of <img> tags)
    images = [img.get('src') for img in soup.find_all('img')]

    # Extract page metadata
    try:
        page_title = soup.title.string.strip()
    except AttributeError:
        page_title = url  # Use URL if title tag is not found

    meta_description = soup.find("meta", attrs={"name": "description"})
    description = meta_description.get("content") if meta_description else page_title

    meta_keywords = soup.find("meta", attrs={"name": "keywords"})
    keywords = meta_keywords.get("content") if meta_keywords else ""

    metadata = {'title': page_title,
                'url': url,
                'description': description,
                'keywords': keywords}

    return text, links, images, metadata

# Function to save extracted data to the PostgreSQL database
def save_to_db(text, links, images, metadata):
    db = SessionLocal()
    db_data = ExtractedData(
        id=str(time.time()),
        title=metadata['title'],
        url=metadata['url'],
        description=metadata['description'],
        keywords=metadata['keywords'],
        text_content=text,
        links="\n".join(links),
        images="\n".join(images)
    )
    db.add(db_data)
    db.commit()
    db.refresh(db_data)
    db.close()

# FastAPI endpoints

@app.get("/search")
async def search_endpoint(query: str):
    urls = search_and_save_urls(query)
    return {"urls": urls}

@app.get("/scrape")
async def scrape_endpoint(url: str = Query(..., description="URL of the website to scrape")):
    text, links, images, metadata = get_data_from_website(url)
    if text and metadata:
        save_to_db(text, links, images, metadata)
        return {"message": f"Data from {url} has been saved to the database"}
    else:
        return {"error": "Failed to scrape data from the provided URL"}

# If this script is run directly, start FastAPI server
if _name_ == "_main_":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)