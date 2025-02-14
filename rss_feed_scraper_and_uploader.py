import html
import http
import json
import pandas as pd
import os
import re
import requests
import textwrap
import feedparser
import schedule
import sys
import threading
import time
import wikibaseintegrator.models as models
import wikibaseintegrator.wbi_helpers
from datetime import datetime
from wikibaseintegrator import wbi_login, WikibaseIntegrator, wbi_helpers
from wikibaseintegrator.datatypes import ExternalID, Item, Time, URL, MonolingualText, Quantity
from wikibaseintegrator.wbi_config import config
from wikibaseintegrator.wbi_enums import ActionIfExists
from dateutil import parser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Define Wikibase API URL and credentials
WIKIBASE_URL = ""
USERNAME = ""
PASSWORD = ""

# Property IDs
PROP_INSTANCE_OF = ""
PROP_URL = ""
PROP_DATE_PUBLISHED = ""
PROP_ARCHIVED_URL = ""
PROP_ARCHIVED_DATE = ""
PROP_TITLE = ""
PROP_DOI = ""
PROP_PMID = ""
PROP_PMCID = ""

# Internet Archive URL for querying
WAYBACK_API = "http://archive.org/wayback/available"

# User-Agent header to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# JSON file to track processed articles
MAPPING_FILE_BASE = "wikibase_mapping"
JSON_FILE_BASE = "news"
NEWS_SITES_FILE = "sites.json"
UNMATCHED_FILE_BASE = "unmatched_articles"

# Function to manage chunked JSON files
def get_chunked_filename(base_name, extension="json", max_size=50 * 1024 * 1024):
    index = 1
    while True:
        filename = f"{base_name}_{index}.{extension}"
        if not os.path.exists(filename) or os.path.getsize(filename) < max_size:
            return filename
        index += 1

MAPPING_FILE = get_chunked_filename(MAPPING_FILE_BASE)
JSON_FILE = get_chunked_filename(JSON_FILE_BASE)
UNMATCHED_FILE = get_chunked_filename(UNMATCHED_FILE_BASE)

# Load existing mappings
if os.path.exists(MAPPING_FILE):
    with open(MAPPING_FILE, "r") as f:
        mappings = json.load(f)
else:
    mappings = {}

# Load unmatched articles
if os.path.exists(UNMATCHED_FILE):
    with open(UNMATCHED_FILE, "r") as f:
        unmatched_articles = json.load(f)
else:
    unmatched_articles = []

# Configure WikibaseIntegrator
config['MEDIAWIKI_API_URL'] = WIKIBASE_URL
login_instance = wbi_login.Login(user=USERNAME, password=PASSWORD)
wbi = WikibaseIntegrator(login=login_instance)

# List of news sites with RSS feeds
# Load existing mappings
if os.path.exists(NEWS_SITES_FILE):
    with open(NEWS_SITES_FILE, "r") as f:
        NEWS_SITES = json.load(f)
else:
    NEWS_SITES = {}

def format_date(date_str, is_archival=False):
    if pd.isna(date_str):  # Check for NaN values
        return None
    if isinstance(date_str, float):
        date_str = int(date_str)
    
    date_str = str(date_str)  # Ensure it's a string
    
    if is_archival:
        if not date_str.isdigit() or len(date_str) != 14:
            raise ValueError(f"Invalid archival date format: {date_str}")
        return f"+{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T00:00:00Z"
    
    # Remove timezone abbreviation if present
    date_str_cleaned = re.sub(r'\s[A-Z]{3}$', '', date_str)
    
    try:
        dt = datetime.strptime(date_str_cleaned, "%B %d %Y %I:%M %p")
    except ValueError:
        try:
            dt = parser.parse(date_str_cleaned)
        except ValueError:
            raise ValueError(f"Date format does not match expected pattern: {date_str}")
    
    return dt.strftime("+%Y-%m-%dT00:00:00Z")

def resolve_google_news_link(google_rss_url):
    """
    Uses Selenium to resolve the final redirected URL from a Google News RSS link.
    """
    options = Options()
    options.headless = True  # Run in headless mode
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)  # Use Chrome, or replace with Firefox

    try:
        driver.get(google_rss_url)
        time.sleep(3)  # Give it time to process the redirection
        resolved_url = driver.current_url
    except Exception as e:
        print(f"Error resolving URL {google_rss_url}: {e}")
        resolved_url = google_rss_url  # Fallback to original link
    finally:
        driver.quit()

    return resolved_url

def extract_original_google_news_url(rss_entry):
    """
    Extracts the original news source URL from a Google News RSS entry.
    """
    if 'source' in rss_entry and 'href' in rss_entry.source:
        return rss_entry.source['href']  # This is the actual news source URL
    return rss_entry.link  # Fallback to the provided Google News link

def get_archive_link(url):
    try:
        response = requests.get(WAYBACK_API, params={"url": url}, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        if "archived_snapshots" in data and "closest" in data["archived_snapshots"]:
            archive_info = data["archived_snapshots"]["closest"]
            return archive_info.get("url", ""), archive_info.get("timestamp", "")
    except Exception as e:
        print(f"Error fetching archive info for {url}: {e}")
    return "", ""

def user_input_with_timeout(prompt, timeout=120, default=""):  
    result = [default]

    def get_input():
        result[0] = input(prompt).strip()

    thread = threading.Thread(target=get_input)
    thread.daemon = True
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        print(f"\nTimeout reached, defaulting to '{default}'")
        return default
    return result[0]

def search_or_create_entity(title, url, doi=None, pmid=None, pmcid=None, dc_source=None, google_rss_url=None):

    title = (html.unescape(title.replace("‘", "'").replace("’", "'").replace("“", '"').replace("”", '"'))).replace("‚Äô", "'")
    shortened_title = None

    if ',Äô' in title:
        print(title)
        exit()

    alias = None
    if google_rss_url:
        if 'google.com' in google_rss_url:
            alias = title
            split_title = title.rsplit(' - ')
            title = split_title[0]

    if len(title) > 250:
        print(f"Title ({title}) is longer than 250 characters. Shortening for search...")
        shortened_title = title[:247] + "..."
        search_results = wikibaseintegrator.wbi_helpers.search_entities(shortened_title, mediawiki_api_url=WIKIBASE_URL)
    else:
        search_results = wikibaseintegrator.wbi_helpers.search_entities(title, mediawiki_api_url=WIKIBASE_URL)
    if search_results:
        for result in search_results:
            entity_data = wbi.item.get(result)
            url_claim_data = entity_data.claims.get(PROP_URL)
            existing_urls = []
            if len(url_claim_data) > 0:
                for url_claim in url_claim_data:
                    existing_urls.append(url_claim.mainsnak.datavalue['value'])
            if url in existing_urls:
                print(f"Automatically matched based on URL: {title} (ID: {result})")
                return result
            else:
                if doi or pmid or pmcid:
                    doi_claim_data = [(doi_claim.mainsnak.datavalue['value']).lower() for doi_claim in entity_data.claims.get(PROP_DOI)]
                    if doi in doi_claim_data:
                        print(f"Automatically matched based on DOI: {title} (ID: {result})")
                        return result

                    pmid_claim_data = [pmid_claim.mainsnak.datavalue['value'] for pmid_claim in entity_data.claims.get(PROP_PMID)]
                    if pmid in pmid_claim_data:
                        print(f"Automatically matched based on PMID: {title} (ID: {result})")
                        return result

                    pmcid_claim_data = [pmcid_claim.mainsnak.datavalue['value'] for pmcid_claim in entity_data.claims.get(PROP_PMCID)]
                    if pmcid in pmcid_claim_data:
                        print(f"Automatically matched based on PMCID: {title} (ID: {result})")
                        return result
                    
                confirmation = user_input_with_timeout(f"Match found: {wbi.item.get(result).labels.get('en').value} (ID: {result}). Is this correct? (y/n): ", default="n")
                if confirmation.lower() == 'y':
                    return str(result)
    
    confirmation = user_input_with_timeout(f"No automatic match found for '{title}'. Do you want to create a new entity? (y/n): ", default="y", timeout=20)
    if confirmation.lower() == 'y':
        item = wbi.item.new()
        if len(title) > 250:
            item.labels.set("en", shortened_title)
        else:
            item.labels.set("en", title)
        if "pubmed.ncbi.nlm.nih.gov" not in url:
            item.claims.add(Item(prop_nr=PROP_INSTANCE_OF, value=""), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        else:
            if dc_source:
                if "journal" in dc_source.lower():
                    item.claims.add(Item(prop_nr=PROP_INSTANCE_OF, value=""), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        if len(url) <= 500:
            if 'pubmed' not in url:
                item.claims.add(URL(prop_nr=PROP_URL, value=url), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        else:
            print(f"URL ({url}) is over 500 characters. URL must be shortened. Exiting...")
            exit()
        if google_rss_url:
            if isinstance(google_rss_url, list):
                for google_url in google_rss_url:
                    if (len(google_url) <= 500) and 'google.com' in google_url: # wikibaseintegrator.wbi_exceptions.ModificationFailed: 'Must be no more than 500 characters long'
                        qualifiers = models.Qualifiers()
                        qualifiers.add(Item(prop_nr="", value="")) # Adds RSS URL as a deprecated due to being a redirect
                        item.claims.add(URL(prop_nr=PROP_URL, value=google_url, qualifiers=qualifiers, rank="deprecated"), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        if len(title) > 400:
            chunks = split_text(title)
            for counter, chunk in enumerate(chunks, start=1):
                qualifiers = models.Qualifiers()
                qualifiers.add(Quantity(prop_nr="", amount=counter))
                item.claims.add(MonolingualText(prop_nr=PROP_TITLE, text=chunk, language='en'), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        else:
            item.claims.add(MonolingualText(prop_nr=PROP_TITLE, text=title, language='en'), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        if alias:
            item.aliases.set('en', alias)
            item.claims.add(MonolingualText(prop_nr=PROP_TITLE, text=alias, language='en', rank="deprecated"), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        item.write()
        return item.id
    else:
        unmatched_articles.append({"Title": title, "URL": url})
        with open(UNMATCHED_FILE, "w") as f:
            json.dump(unmatched_articles, f)
        return None
    
def split_text(text, max_length=400):
    words = text.split()
    chunks = []
    current_chunk = []

    for word in words:
        if sum(len(w) for w in current_chunk) + len(current_chunk) + len(word) <= max_length:
            current_chunk.append(word)
        else:
            chunks.append(" ".join(current_chunk))
            current_chunk = [word]

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks

def update_or_create_article(article):
    title = article["Title"]
    url = article["Article URL"]
    
    entity_id = search_or_create_entity(title, url, article["DOI"], article["PMID"], article["PMCID"], article["dc:source"], article["Google RSS URL"])
    if not entity_id:
        return
    
    print(f"Updating article entity: {title}")
    archive_url, archive_date = get_archive_link(url)
    item = wbi.item.get(entity_id)
    if article["Date Published"] != "" and article["Date Published"] != " ":
        item.claims.add(Time(prop_nr=PROP_DATE_PUBLISHED, time=format_date(article["Date Published"]), precision=11), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
    if archive_url:
        qualifiers = models.Qualifiers()
        qualifiers.add(Time(prop_nr=PROP_ARCHIVED_DATE, time=format_date(archive_date, is_archival=True), precision=11))
        item.claims.add(URL(prop_nr=PROP_ARCHIVED_URL, value=archive_url, qualifiers=qualifiers), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)

    if article["DOI"] and article["DOI"] != "":
        mappings[article["DOI"]] = entity_id
        item.claims.add(ExternalID(prop_nr=PROP_DOI, value=article["DOI"]), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
    if article["PMID"] and article["PMID"] != "":
        mappings[article["PMID"]] = entity_id
        item.claims.add(ExternalID(prop_nr=PROP_PMID, value=article["PMID"]), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
    if article["PMCID"] and article["PMCID"] != "":
        mappings[article["PMCID"]] = entity_id
        item.claims.add(ExternalID(prop_nr=PROP_PMCID, value=article["PMCID"]), action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)

    item.write()
    
    mappings[url] = entity_id
    if 'Google RSS URL' in article:
        if article["Google RSS URL"] != "" and article["Google RSS URL"]:
            for google_url in article["Google RSS URL"]:
                mappings[google_url] = entity_id
    with open(MAPPING_FILE, "w") as f:
        json.dump(mappings, f, indent=4)

def fetch_rss_articles(site, rss_url):
    articles = []
    
    try:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            try:
                article_dict = {key: entry.get(key, "") for key in entry.keys()}
                article_dict["Title"] = " ".join(str(entry.title).splitlines())
                article_dict["DOI"] = None
                article_dict["PMID"] = None
                article_dict["PMCID"] = None
                article_dict["Google RSS URL"] = None
                article_dict["dc:source"] = None

                if "google.com" in entry.link:
                    article_dict["Google RSS URL"] = [entry.link]
                    first_resolved_link = resolve_google_news_link(entry.link)
                    if "google.com" in first_resolved_link:
                        article_dict["Google RSS URL"].append(first_resolved_link)
                        second_resolved_link = resolve_google_news_link(first_resolved_link)
                        article_dict["Article URL"] = second_resolved_link
                    else:
                        article_dict["Article URL"] = first_resolved_link               
                    
                elif "pubmed.ncbi.nlm.nih.gov" in entry.link:
                    article_dict["Google RSS URL"] = [entry.link]

                    dc_identifier = entry.get("dc_identifier", "") # Ex: doi:10.1136/bmj.n998
                    id_identifier = entry.get("id", "") # Ex: pubmed:33879524
                    dc_source = entry.get("dc_source", "")
                    if dc_identifier != "":
                        article_dict["DOI"] = dc_identifier.split(':')[1]
                    if id_identifier != "":
                        article_dict["PMID"] = id_identifier.split(':')[1]
                    if dc_source != "":
                        article_dict["dc:source"] = dc_source

                    article_dict["Article URL"] = 'https://pubmed.ncbi.nlm.nih.gov/' + article_dict["PMID"] + '/'

                else:
                    article_dict["Article URL"] = entry.link

                article_dict["Date Published"] = entry.get("published", "")
                if article_dict["Title"] != "" and article_dict["Title"] != " ":
                    articles.append(article_dict)
            except AttributeError:
                pass
    except http.client.RemoteDisconnected:
        print("Remote end closed connection without response for %s. Skipping for now..." % str(site))
    
    return articles

def process_articles():
    existing_data = json.load(open(JSON_FILE, "r")) if os.path.exists(JSON_FILE) else []

    for site, rss_url in NEWS_SITES.items():
        articles = fetch_rss_articles(site, rss_url)
        for article in articles:
            if article["link"] not in mappings:
                update_or_create_article(article)
                article["Wikibase ID"] = mappings[article["link"]]
                existing_data.append(article)
                time.sleep(20)
    
    with open(JSON_FILE, "w") as f:
        json.dump(existing_data, f, indent=4)

def run_daily():
    login_instance = wbi_login.Login(user=USERNAME, password=PASSWORD)
    wbi = WikibaseIntegrator(login=login_instance)
    process_articles()
    print("Daily run completed.")

if __name__ == "__main__":
    process_articles()  # Run immediately upon start
    schedule.every().day.at("00:00").do(run_daily)
    while True:
        schedule.run_pending()
        time.sleep(300)
