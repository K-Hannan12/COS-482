

from bs4 import BeautifulSoup
import requests
import selenium
from selenium.webdriver.chrome.service import Service
from selenium import webdriver

service = Service(executable_path="C:/Users/kaleb/COS-482/chromedriver.exe") # type: ignore
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--incognito')

driver = webdriver.Chrome(service= service,options= options)



page = 0
driver.get("https://scholar.google.com/scholar?start="+str(page)+"&as_ylo=2022&q=machine+learning&hl=en&as_sdt=0,20")
url = "https://scholar.google.com/scholar?as_ylo=2024&q=machine+learning&hl=en&as_sdt=0,20"
response = requests.get(url)
#print(response)

html = response.content
#print(html)

soup = BeautifulSoup(html, "lxml")
#print(soup)

all_articles = soup.find_all("div", class_="gs_ri")
print(all_articles)
for article in all_articles:
    title = article.find("h3", class_="gs_rt")
    authors_venue_year = article.find("div", class_="gs_a")
    #print(title.get_text(strip=False))
    #print(authors_venue_year.get_text(strip=False))

print(soup.prettify())

all_articles = soup.select("#gs_res_ccl_mid div.gs_ri")
for article in all_articles:
    title_selector = "h3.gs_rt"
    authors_venue_year_selector = "div.gs_a"
    title = article.select_one(title_selector)
    authors_venue_year = article.select_one(authors_venue_year_selector)
    #print(title.text)
    #print(authors_venue_year.text)