import azure.functions as func
import hashlib
import json
import logging
import os
import re
import requests
import traceback
import validators
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

CHUNK_SIZE = 2000  # characters per chunk, tuned for AI Search
DEFAULT_MAX_PAGES = int(os.environ.get("CRAWL_MAX_PAGES", "100"))
CRAWL_NESTED_SITEMAPS = os.environ.get("CRAWL_NESTED_SITEMAPS", "false").lower() == "true"


@app.route(route="search_site", methods=["POST"])
def search_site(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url = req.params.get('url')
    max_pages = None
    if not url:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            url = req_body.get('url')
            max_pages = req_body.get('max_pages')

    if url:
        if validators.url(url):
            if _is_sitemap_candidate(url):
                result = crawl_sitemap(url, max_pages)
            else:
                result = orchestrator_function(url)
            return func.HttpResponse(
                json.dumps(result, indent=2),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "The URL was invalid."}),
                status_code=400,
                mimetype="application/json"
            )
    else:
        return func.HttpResponse(
            json.dumps({"error": "No URL was passed. Please input a URL."}),
            status_code=400,
            mimetype="application/json"
        )


def orchestrator_function(url):
    try:
        data = crawl_site(url)

        title = get_page_title(data)
        description = get_meta_tag(data)
        content = get_text_content(data)
        links = get_all_urls(data)

        # Split content into chunks for AI Search indexing.
        chunks = chunk_text(content, CHUNK_SIZE)

        documents = []
        for i, chunk in enumerate(chunks):
            doc_id = generate_doc_id(url, i)
            document = {
                "id": doc_id,
                "url": url,
                "title": title or "",
                "chunk_index": i,
                "total_chunks": len(chunks),
                "content": chunk,
                "metadata_description": description or "",
                "links": links,
                "crawled_at": datetime.now(timezone.utc).isoformat(),
            }
            documents.append(document)

        upload_to_blob_storage(url, documents)

        return {
            "url": url,
            "title": title,
            "total_chunks": len(chunks),
            "documents_uploaded": len(documents),
        }
    except Exception as error:
        logging.error(f"Error while crawling the site: {error}")
        logging.error(traceback.format_exc())
        return {"error": str(error)}


def _is_sitemap_candidate(url):
    """Check if the URL is a domain root or an explicit sitemap URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    return path == "" or path.endswith("/sitemap.xml") or path.endswith(".xml")


def _build_sitemap_url(url):
    """Build the sitemap URL from a domain root URL."""
    parsed = urlparse(url)
    if parsed.path.rstrip("/").endswith(".xml"):
        return url
    return urlunparse((parsed.scheme, parsed.netloc, "/sitemap.xml", "", "", ""))


def fetch_sitemap(url):
    """Fetch and parse a sitemap, returning a list of page URLs."""
    sitemap_url = _build_sitemap_url(url)
    logging.info(f"Fetching sitemap from: {sitemap_url}")

    response = requests.get(sitemap_url, allow_redirects=True, timeout=15)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    # Strip XML namespace for easier tag matching
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    # Check if this is a sitemap index
    sitemap_entries = root.findall(f"{ns}sitemap")
    if sitemap_entries:
        if CRAWL_NESTED_SITEMAPS:
            logging.info(f"Sitemap index found with {len(sitemap_entries)} sub-sitemaps, following nested sitemaps.")
            urls = []
            for sitemap_el in sitemap_entries:
                loc = sitemap_el.find(f"{ns}loc")
                if loc is not None and loc.text:
                    try:
                        urls.extend(fetch_sitemap(loc.text.strip()))
                    except Exception as e:
                        logging.warning(f"Failed to fetch sub-sitemap {loc.text}: {e}")
            return urls
        else:
            logging.warning(
                f"Sitemap index found with {len(sitemap_entries)} sub-sitemaps, "
                "but CRAWL_NESTED_SITEMAPS is disabled. Skipping nested sitemaps."
            )
            return []

    # Standard sitemap with <url><loc> entries
    url_entries = root.findall(f"{ns}url")
    urls = []
    for url_el in url_entries:
        loc = url_el.find(f"{ns}loc")
        if loc is not None and loc.text:
            urls.append(loc.text.strip())
    logging.info(f"Found {len(urls)} URLs in sitemap.")
    return urls


def crawl_sitemap(domain_url, max_pages=None):
    """Crawl all pages discovered from a sitemap."""
    if max_pages is None:
        max_pages = DEFAULT_MAX_PAGES

    try:
        page_urls = fetch_sitemap(domain_url)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logging.info("No sitemap found, falling back to single-page crawl.")
            return orchestrator_function(domain_url)
        raise
    except Exception as e:
        logging.warning(f"Failed to fetch sitemap: {e}. Falling back to single-page crawl.")
        return orchestrator_function(domain_url)

    if not page_urls:
        logging.info("Sitemap was empty or had no URLs. Falling back to single-page crawl.")
        return orchestrator_function(domain_url)

    total_found = len(page_urls)
    page_urls = page_urls[:max_pages]

    results = []
    errors = []
    for page_url in page_urls:
        try:
            result = orchestrator_function(page_url)
            if "error" in result:
                errors.append({"url": page_url, "error": result["error"]})
            else:
                results.append(result)
        except Exception as e:
            logging.error(f"Error crawling {page_url}: {e}")
            errors.append({"url": page_url, "error": str(e)})

    return {
        "sitemap_url": _build_sitemap_url(domain_url),
        "total_urls_found": total_found,
        "max_pages": max_pages,
        "pages_crawled": len(results),
        "pages_failed": len(errors),
        "results": results,
        "errors": errors,
    }


def crawl_site(url):
    response = requests.get(url, allow_redirects=True, timeout=15)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")


def get_page_title(data):
    try:
        return data.title.string.strip() if data.title and data.title.string else None
    except Exception as error:
        logging.error(f"Error retrieving the site title: {error}")
        return None


def get_text_content(data):
    try:
        body = data.find("body")
        if not body:
            return ""
        for tag in body(["script", "style", "noscript", "nav", "footer", "header"]):
            tag.decompose()
        lines = (line.strip() for line in body.get_text(separator="\n").splitlines())
        return "\n".join(line for line in lines if line)
    except Exception as error:
        logging.error(f"Error retrieving text content: {error}")
        return ""


def get_all_urls(data):
    try:
        urls = []
        for el in data.select("a[href]"):
            href = el['href']
            if href.startswith("https://") or href.startswith("http://"):
                urls.append(href)
        return urls
    except Exception as error:
        logging.error(f"Error retrieving URLs: {error}")
        return []


def get_meta_tag(data):
    try:
        meta_tag = data.find("meta", attrs={'name': 'description'})
        return meta_tag["content"] if meta_tag else None
    except Exception as error:
        logging.error(f"Error retrieving meta description: {error}")
        return None


def chunk_text(text, chunk_size):
    """Split text into chunks, breaking at paragraph boundaries when possible."""
    if not text:
        return [""]

    paragraphs = text.split("\n")
    chunks = []
    current_chunk = ""

    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 1 > chunk_size and current_chunk:
            chunks.append(current_chunk.strip())
            current_chunk = paragraph
        else:
            current_chunk = current_chunk + "\n" + paragraph if current_chunk else paragraph

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks if chunks else [""]


def generate_doc_id(url, chunk_index):
    """Create a deterministic, URL-safe document ID for AI Search."""
    raw = f"{url}::chunk::{chunk_index}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def upload_to_blob_storage(url, documents):
    try:
        account_url = os.environ["STORAGE_ACCOUNT_URL"]
        container_name = os.environ["STORAGE_CONTAINER_NAME"]
        client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")

        if client_id:
            credential = ManagedIdentityCredential(client_id=client_id)
        else:
            credential = DefaultAzureCredential()

        blob_service_client = BlobServiceClient(account_url, credential=credential)
        container_client = blob_service_client.get_container_client(container_name)

        if not container_client.exists():
            container_client.create_container()

        parsed = urlparse(url)
        folder = re.sub(r"[^a-zA-Z0-9_-]", "_", parsed.netloc + parsed.path)[:120]

        for doc in documents:
            blob_name = f"{folder}/{doc['id']}.json"
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(json.dumps(doc, indent=2), overwrite=True)

        logging.info(f"Uploaded {len(documents)} documents for {url}")
    except Exception as error:
        logging.error(f"Error uploading to blob storage: {error}")
        logging.error(traceback.format_exc())