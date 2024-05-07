import asyncio
import aiohttp
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import schedule
import time as tm

# Database setup
engine = create_engine('sqlite:///fetch_nike_shoes.db')
Base = declarative_base()

class Shoes(Base):
    __tablename__ = 'shoes'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    description = Column(String)
    price = Column(String)
    size_type = Column(String)
    product_size = Column(String)
    product_details = Column(String)
    images_links = Column(String)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
db_session = Session()  # Global session instance

# Create a single Chrome driver instance
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=chrome_options)

async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            try:
                return await response.text()
            except Exception as e:
                print('fetch', e)
                return await response.text()

async def get_product_links(url):
    max_retries = 3
    retry_delay = 60  # seconds
    retries = 0
    while retries < max_retries:
        try:
            driver.get(url)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, "hit")))
            products = driver.find_elements(By.CLASS_NAME, "hit")
            href_list = [product.find_element(By.TAG_NAME, "a").get_attribute("href") for product in products]
            valid_links = []
            for href in href_list:
                try:
                    response = await fetch(href)
                    if response:
                        valid_links.append(href)
                except Exception as e:
                    print(f"Error fetching link {href}: {e}")
            
            return valid_links
            
        except Exception as e:
            print(f"Error fetching product links from {url}: {e}. Retrying...")
            retries += 1
            tm.sleep(retry_delay)
    print(f"Failed to fetch product links from {url} after {max_retries} retries. Moving to the next URL...")
    return []


async def store_data(url):
    try:
        product_links = await get_product_links(url)
        for product_link in product_links:
            try:
                html = await fetch(product_link)
                try:
                    new_soup = BeautifulSoup(html, "html.parser")
                except Exception as e:
                    print("new_soup", e)
                if new_soup:
                    title = await fetch_title(new_soup)
                    description = await fetch_description(new_soup)
                    price = await fetch_price(new_soup)
                    size_type = await fetch_size_type(new_soup)
                    product_size = await fetch_product_size(new_soup, product_link)
                    product_details = await fetch_product_details(new_soup)
                    images_links = await fetch_image_links(new_soup)
                    insert_data(title, description, price, size_type, product_size, product_details, images_links)
            except Exception as e:
                print(f"Error processing {product_link}: {e}")
    except Exception as e:
        print(f"Error fetching product links from {url}: {e}")
        print("Pausing for a minute before trying again...")
        await asyncio.sleep(60)  # Pause for a minute before trying again

def insert_data(title, description, price, size_type, product_size, product_details, images_links):
    data = Shoes(title=title, description=description, price=price, size_type=size_type, product_size=product_size,
                 product_details=product_details, images_links=images_links)
    db_session.add(data)
    db_session.commit()

def check_exist(title):
    return db_session.query(Shoes).filter_by(title=title).first() is not None

async def fetch_title(new_soup):
    try:
        title = new_soup.find('h1', class_='product-area__details__title product-detail__gap-sm h2').text.strip()
    except:
        title = None
    return title

async def fetch_description(new_soup):
    descr = new_soup.find('div', attrs={"class": "product-detail__tab-container product-detail__gap-lg"})
    descr = str(descr)
    soup = BeautifulSoup(descr, 'html.parser')
    if soup:
        try:
            para = soup.find('p')
            description = para.text
        except:
            description="no description"

    return description

async def fetch_price(new_soup):
    try:
        price = new_soup.find('span', attrs={'class': 'current-price theme-money'}).text.strip()
        return price
    except:
        price = None
    return price

async def fetch_size_type(new_soup):
    try:
        size_type = new_soup.find('div', attrs={'class': 'pdpOptionValues'}).text
    except:
        size_type = None
    return size_type

async def fetch_product_size(new_soup, product_link):
    try:
        size = new_soup.find_all('div', attrs={"onclick": "handleVariantClick(event)"})
        if size:
            shoes_price = []
            for i in range(0, 26):
                try:
                    value = size[i].get("data-variant-id")
                    shoes_sizelink = product_link + "?variant=" + value
                    html = await fetch(shoes_sizelink)
                    shoes_size_soup = BeautifulSoup(html, 'html.parser')
                    shoe_price = shoes_size_soup.find('span', attrs={'class': 'current-price theme-money'}).text.strip()
                    shoes_price.append(shoe_price)
                except:
                    shoe_price = None
            shoe_price = create_size_chart(shoes_price)
        else:
            product_size = None
            return product_size
    except:
        product_size = None
        return product_size
    product_size = json.dumps(shoe_price)
    return product_size

async def fetch_product_details(new_soup):
    try:
        info = new_soup.find('div', attrs={'class': 'cc-tabs__tab__panel rte'}).find_all('span')
        if info:
            product_dict = {}
            for i in range(0, 5):
                try:
                    product_dict["MOdel_NO"] = info[0].text.strip()
                except:
                    product_dict["MOdel_NO"] = None
                try:
                    product_dict['RElEASE_DATE'] = info[1].text.strip()
                except:
                    product_dict["RElEASE_DATE"] = None
                try:
                    product_dict['SERIES'] = info[2].text.strip()
                except:
                    product_dict['SERIES'] = None
                try:
                    product_dict["NICKNAME"] = info[3].text.strip()
                except:
                    product_dict['NICKNAME'] = None
                try:
                    product_dict["COLOR_WAY"] = info[4].text.strip()
                except:
                    product_dict['COLOR_WAY'] = None
            product_details = json.dumps(product_dict)
            return product_details
        else:
            product_details = None
            return product_details
    except:
        product_details = None
        return product_details

async def fetch_image_links(new_soup):
    def image_src(image):
        html_snippet = str(image)
        soup = BeautifulSoup(html_snippet, 'html.parser')
        if soup:
            img_tag = soup.find('img')
            if img_tag:
                src_link = img_tag['src']
                return "https:" + src_link
            else:
                return None
        return None

    try:
        image = new_soup.find_all('div', attrs={'class': 'product-media product-media--image'})
        if image:
            image_links = []
            for i in image:
                image_link = image_src(i)
                if image_link:
                    image_links.append(image_link)
            images_link = json.dumps(image_links)
            return images_link
        else:
            image_links = None
            return image_links
    except:
        image_link = None
        return image_link

def create_size_chart(values):
    size_chart = [
        (3.5, 5, 3, 35.5, 22.5),
        (4, 5.5, 3.5, 36, 23),
        (4.5, 6, 4, 36.5, 23.5),
        (5, 6.5, 4.5, 37.5, 23.5),
        (5.5, 7, 5, 38, 24),
        (6, 7.5, 5.5, 38.5, 24),
        (6.5, 8, 6, 39, 24.5),
        (7, 8.5, 6, 40, 25),
        (7.5, 9, 6.5, 40.5, 25.5),
        (8, 9.5, 7, 41, 26),
        (8.5, 10, 7.5, 42, 26.5),
        (9, 10.5, 8, 42.5, 27),
        (9.5, 11, 8.5, 43, 27.5),
        (10, 11.5, 9, 44, 28),
        (10.5, 12, 9.5, 44.5, 28.5),
        (11, 12.5, 10, 45, 29),
        (11.5, 13, 10.5, 45.5, 29.5),
        (12, 13.5, 11, 46, 30),
        (12.5, 14, 11.5, 47, 30.5),
        (13, 14.5, 12, 47.5, 31),
        (13.5, 15, 12.5, 48, 31.5),
        (14, 15.5, 13, 48.5, 32),
        (15, 16.5, 14, 49.5, 33),
        (16, 17.5, 15, 50.5, 34),
        (17, 18.5, 16, 51.5, 35),
        (18, 19.5, 17, 52.5, 36)
    ]

    dict1 = {}

    for index, (usmen, uswomen, uk, eu, cm) in enumerate(size_chart):
        if index < len(values):
            if values[index] == "":
                dict1[f"us(m){usmen}/us(w){uswomen}/uk{uk}/eu{eu}/cm{cm}"] = "out of stock"
            else:
                dict1[f"us(m){usmen}/us(w){uswomen}/uk{uk}/eu{eu}/cm{cm}"] = values[index]
        else:
            break

    return dict1

async def main():
    chunk_size = 5
    total_pages = 314
    tasks = []
    semaphore = asyncio.Semaphore(chunk_size)

    for i in range(1, total_pages + 1, chunk_size):
        chunk_urls = [f"https://www.kickscrew.com/collections/nike?page={j}" for j in range(i, min(i + chunk_size, total_pages + 1))]
        print(f"Fetching data for chunk {i} - {i + chunk_size - 1}")

        async with semaphore:
            chunk_tasks = [store_data(url) for url in chunk_urls]
            await asyncio.gather(*chunk_tasks)

        # Introduce a delay of 1 second before starting the next chunk
        await asyncio.sleep(1)

def job():
    asyncio.run(main())

schedule.every().day.at("14:25").do(job)

while True:
    schedule.run_pending()
    tm.sleep(60)
