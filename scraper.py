import os
import re
import json
import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import aiohttp
import requests
import torch
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client
from PIL import Image
from io import BytesIO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://yqawmzggcgpeyaaynrjk.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4")

import ssl
import certifi

ssl_context = ssl.create_default_context(cafile=certifi.where())

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

CATEGORIES = [
    {"url": "https://waxlondon.com/collections/all-clothing", "name": "All Clothing"},
    {"url": "https://waxlondon.com/collections/archive-sale", "name": "Archive Sale"},
    {"url": "https://waxlondon.com/collections/footwear", "name": "Footwear"},
    {"url": "https://waxlondon.com/collections/accessories", "name": "Accessories"},
]

semaphore = asyncio.Semaphore(5)


def generate_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def parse_price(price_str: str, currency: str = "CZK") -> tuple[float, str]:
    clean_price = re.sub(r"[^\d.,]", "", price_str)
    clean_price = clean_price.replace(",", ".")
    try:
        price = float(clean_price)
    except ValueError:
        price = 0.0
    return price, currency


def parse_category(category_str: str) -> str:
    if not category_str:
        return ""
    categories = [c.strip() for c in re.split(r"[,&]", category_str)]
    return ", ".join(categories)


async def fetch_session(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status == 200:
                return await response.text()
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
    return None


def extract_product_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    
    product_elements = soup.select('a.ProductItem__ImageWrapper')
    for elem in product_elements:
        href = elem.get("href")
        if href:
            full_url = urljoin(base_url, href)
            if full_url not in links:
                links.append(full_url)
    
    if not links:
        product_elements = soup.select('div.ProductItem a[href*="/products/"]')
        for elem in product_elements:
            href = elem.get("href")
            if href:
                full_url = urljoin(base_url, href)
                if full_url not in links:
                    links.append(full_url)
    
    if not links:
        link_elements = soup.select('a[href*="/products/"]')
        for elem in link_elements:
            href = elem.get("href")
            if href and "/products/" in href:
                full_url = urljoin(base_url, href)
                if full_url not in links:
                    links.append(full_url)
    
    return links


def has_products(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    product_elements = soup.select('div.ProductItem, a.ProductItem__ImageWrapper, a[href*="/products/"]')
    return len(product_elements) > 0


async def scrape_category(session: aiohttp.ClientSession, category: dict) -> list[str]:
    all_product_urls = []
    page = 1
    
    while True:
        if page == 1:
            url = category["url"]
        else:
            url = f"{category['url']}?page={page}"
        
        logger.info(f"Scraping category: {category['name']} - Page {page}")
        html = await fetch_session(session, url)
        
        if not html:
            break
        
        if not has_products(html):
            break
        
        product_urls = extract_product_links(html, url)
        if not product_urls:
            break
        
        all_product_urls.extend(product_urls)
        logger.info(f"  Found {len(product_urls)} products on page {page}, total: {len(all_product_urls)}")
        
        page += 1
        await asyncio.sleep(1)
    
    return all_product_urls


def extract_json_ld(soup: BeautifulSoup) -> dict:
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                if data.get("@type") == "Product" or "Product" in str(data):
                    return data
                if data.get("@graph"):
                    for item in data.get("@graph", []):
                        if item.get("@type") == "Product":
                            return item
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and (item.get("@type") == "Product" or "Product" in str(item)):
                        return item
        except (json.JSONDecodeError, TypeError):
            continue
    return {}


def get_all_images(soup: BeautifulSoup, base_url: str) -> list[str]:
    images = []
    
    json_ld_scripts = soup.find_all("script", type="application/ld+json")
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                image_data = data.get("image")
                if isinstance(image_data, str):
                    if image_data.startswith("http"):
                        images.append(image_data)
                elif isinstance(image_data, dict):
                    url = image_data.get("url", "")
                    if url and url.startswith("http"):
                        images.append(url)
                elif isinstance(image_data, list):
                    for img in image_data:
                        if isinstance(img, str) and img.startswith("http"):
                            images.append(img)
                        elif isinstance(img, dict):
                            url = img.get("url", "")
                            if url and url.startswith("http"):
                                images.append(url)
        except (json.JSONDecodeError, TypeError, ImportError):
            continue
    
    img_elements = soup.select('img[src*="/cdn/shop/files/"]')
    for img in img_elements:
        src = img.get("src", "")
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = "https://waxlondon.com" + src
        if src and src.startswith("http") and src not in images:
            src_1024 = src.replace("_160x", "_1024x").replace("_320x", "_1024x").replace("_480x", "_1024x").replace("_640x", "_1024x").replace("_800x", "_1024x")
            if src_1024 not in images:
                images.append(src_1024)
    
    return list(dict.fromkeys(images))


def scrape_product(html: str, product_url: str, category_name: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    
    product_data = {
        "id": generate_id(product_url),
        "source": "scraper-waxlondon",
        "brand": "Wax London",
        "product_url": product_url,
        "gender": "man",
        "second_hand": False,
        "category": category_name,
        "title": "",
        "description": "",
        "price": "",
        "sale": None,
        "image_url": "",
        "additional_images": "",
        "metadata": {},
        "created_at": datetime.utcnow().isoformat(),
    }
    
    json_ld = extract_json_ld(soup)
    
    if json_ld:
        product_data["title"] = json_ld.get("name", "")
        
        if isinstance(json_ld.get("description"), str):
            product_data["description"] = json_ld.get("description", "")
        
        offers = json_ld.get("offers", [])
        if isinstance(offers, list):
            if offers:
                first_offer = offers[0]
                price = first_offer.get("price", 0)
                currency = first_offer.get("priceCurrency", "CZK")
                product_data["price"] = f"{price}{currency}"
                
                all_prices = []
                for offer in offers:
                    p = offer.get("price", 0)
                    c = offer.get("priceCurrency", "CZK")
                    all_prices.append(f"{p}{c}")
                if len(all_prices) > 1:
                    product_data["price"] = ", ".join(all_prices)
        elif isinstance(offers, dict):
            price = offers.get("price", 0)
            currency = offers.get("priceCurrency", "CZK")
            product_data["price"] = f"{price}{currency}"
        
        image_data = json_ld.get("image")
        if isinstance(image_data, str):
            product_data["image_url"] = image_data
        elif isinstance(image_data, dict):
            product_data["image_url"] = image_data.get("url", "")
        elif isinstance(image_data, list) and image_data:
            product_data["image_url"] = image_data[0] if isinstance(image_data[0], str) else image_data[0].get("url", "")
        
        category = json_ld.get("category", "")
        if category:
            product_data["category"] = parse_category(category)
    
    if not product_data["title"]:
        title_elem = soup.select_one("h1.Product__Title")
        if title_elem:
            product_data["title"] = title_elem.get_text(strip=True)
    
    if not product_data["title"]:
        title_elem = soup.select_one("h1")
        if title_elem:
            product_data["title"] = title_elem.get_text(strip=True)
    
    if not product_data["price"]:
        price_elem = soup.select_one("span.ProductMeta__Price, span.Price, div[data-price]")
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            product_data["price"] = price_text
    
    sale_elem = soup.select_one("span.Price.Price--highlight, span.ProductMeta__Price.Price--highlight, s.Price__CompareAt")
    if sale_elem:
        sale_text = sale_elem.get_text(strip=True)
        if sale_text:
            product_data["sale"] = sale_text
    
    images = get_all_images(soup, product_url)
    if images:
        product_data["image_url"] = images[0]
        if len(images) > 1:
            product_data["additional_images"] = " , ".join(images[1:])
    
    size_elem = soup.select("select.ProductForm__OptionSelector option, div.ProductForm__Option option")
    sizes = []
    for size in size_elem:
        text = size.get_text(strip=True)
        if text and text not in sizes:
            sizes.append(text)
    
    color_elem = soup.select("div.ProductForm__SelectedValue")
    colors = []
    for color in color_elem:
        text = color.get_text(strip=True)
        if text and text not in colors:
            colors.append(text)
    
    product_data["metadata"] = json.dumps({
        "title": product_data["title"],
        "description": product_data["description"],
        "price": product_data["price"],
        "sale": product_data["sale"],
        "category": product_data["category"],
        "sizes": sizes,
        "colors": colors,
        "url": product_url,
    })
    
    if category_name == "All" and not product_data["category"]:
        product_data["category"] = "All Clothing"
    
    return product_data


async def scrape_products_batch(session: aiohttp.ClientSession, product_urls: list[str], category_name: str) -> list[dict]:
    products = []
    
    for i, url in enumerate(product_urls):
        async with semaphore:
            logger.info(f"Scraping product {i+1}/{len(product_urls)}: {url}")
            html = await fetch_session(session, url)
            
            if html:
                product = scrape_product(html, url, category_name)
                products.append(product)
            
            await asyncio.sleep(0.5)
    
    return products


def get_text_embedding(text: str, model, processor, device) -> list[float]:
    if not text:
        return [0.0] * 768
    
    try:
        inputs = processor(text=text, return_tensors="pt").to(device)
        with torch.no_grad():
            outputs = model.get_text_features(**inputs)
        if hasattr(outputs, 'pooler_output'):
            embedding = outputs.pooler_output[0].cpu().numpy().tolist()
        else:
            embedding = outputs[0].cpu().numpy()[0].tolist()
        return embedding
    except Exception as e:
        logger.info(f"Error generating text embedding: {e}")
        return [0.0] * 768


def get_image_embedding(image_url: str, model, processor, device) -> list[float]:
    if not image_url:
        return [0.0] * 768
    
    try:
        response = requests.get(image_url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            logger.info(f"Failed to download image {image_url}: HTTP {response.status_code}")
            return [0.0] * 768
        image = Image.open(BytesIO(response.content)).convert("RGB")
        
        inputs = processor(images=image, return_tensors="pt").to(device)
        with torch.no_grad():
            outputs = model.get_image_features(**inputs)
        if hasattr(outputs, 'pooler_output'):
            embedding = outputs.pooler_output[0].cpu().numpy().tolist()
        else:
            embedding = outputs[0].cpu().numpy()[0].tolist()
        return embedding
    except Exception as e:
        logger.info(f"Error generating image embedding for {image_url}: {e}")
        return [0.0] * 768


def upload_to_supabase(products: list[dict], supabase: Client) -> int:
    uploaded = 0
    errors = 0
    seen_ids = set()
    
    for product in products:
        try:
            product_id = product["id"]
            
            if product_id in seen_ids:
                logger.info(f"Skipping duplicate: {product['title']}")
                continue
            seen_ids.add(product_id)
            
            data = {
                "id": product["id"],
                "source": product["source"],
                "product_url": product["product_url"],
                "brand": product["brand"],
                "title": product["title"],
                "description": product["description"],
                "category": product["category"],
                "gender": product["gender"],
                "second_hand": product["second_hand"],
                "price": product["price"],
                "sale": product["sale"],
                "image_url": product["image_url"],
                "additional_images": product["additional_images"],
                "metadata": product["metadata"],
                "created_at": product["created_at"],
                "image_embedding": product.get("image_embedding"),
                "info_embedding": product.get("info_embedding"),
            }
            
            supabase.table("products").upsert(data, on_conflict="id").execute()
            uploaded += 1
            logger.info(f"Uploaded {uploaded}: {product['title'][:50]}...")
        except Exception as e:
            errors += 1
            logger.info(f"Error uploading {product.get('title', 'unknown')}: {e}")
    
    logger.info(f"Upload complete: {uploaded} uploaded, {errors} errors")
    return uploaded


async def main():
    logger.info("Starting Wax London scraper...")
    
    logger.info("Loading SigLIP model for embeddings...")
    from transformers import AutoProcessor, AutoModel
    import torch
    
    model_name = "google/siglip-base-patch16-384"
    device = "cuda" if torch.cuda.is_available() else "cpu"
    
    model = AutoModel.from_pretrained(model_name).to(device)
    processor = AutoProcessor.from_pretrained(model_name)
    model.eval()
    
    logger.info(f"Model loaded on {device}")
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        all_product_urls = {}
        
        for category in CATEGORIES:
            logger.info(f"\nProcessing category: {category['name']}")
            urls = await scrape_category(session, category)
            all_product_urls[category["name"]] = urls
            logger.info(f"Found {len(urls)} products in {category['name']}")
        
        logger.info(f"\nTotal products to scrape: {sum(len(v) for v in all_product_urls.values())}")
        
        all_urls_flat = []
        seen_urls = set()
        for urls in all_product_urls.values():
            for url in urls:
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_urls_flat.append(url)
        
        logger.info(f"Unique products: {len(all_urls_flat)}")
        
        all_products = []
        products = await scrape_products_batch(session, all_urls_flat, "All")
        all_products.extend(products)
        
        logger.info(f"\nScraped {len(all_products)} products")
        
        logger.info("\nGenerating embeddings...")
        for i, product in enumerate(all_products):
            logger.info(f"Processing embedding {i+1}/{len(all_products)}: {product['title'][:50]}...")
            
            image_url = product.get("image_url", "")
            if image_url:
                product["image_embedding"] = get_image_embedding(image_url, model, processor, device)
            
            info_text = f"{product['title']} {product['description']} {product['category']} {product['gender']} {product['price']}"
            product["info_embedding"] = get_text_embedding(info_text, model, processor, device)
        
        logger.info("\nUploading to Supabase...")
        uploaded = upload_to_supabase(all_products, supabase)
        logger.info(f"\nSuccessfully uploaded {uploaded} products to Supabase")


if __name__ == "__main__":
    asyncio.run(main())