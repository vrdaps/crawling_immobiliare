# Import required libraries
from bs4 import BeautifulSoup, SoupStrainer
from googletrans import Translator
import concurrent.futures
import requests
import sqlite3
import random
import time
import os

import pandas as pd
import regex as re
import numpy as np

pd.set_option("display.max_rows", None, "display.max_columns", None,'display.max_colwidth',1)


#Scraper function
def soup(url,strainer):
    # Set the User-Agent to mimic a web browser
    
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    ]
    

    # Use a session object to maintain a persistent connection
    session = requests.Session()

    headers = {'User-Agent': random.choice(user_agents)}

    session.headers.update(headers)

    # Make a GET request and parse the HTML with Beautiful Soup
    r = session.get(url, headers=headers, timeout = 20)

    #Extract specific part of the website
    strain = SoupStrainer(strainer)

    # Extract the data you need from the HTML
    s=BeautifulSoup(r.content, 'html.parser', parse_only=strain)

    # Add a random delay between 1 to 3 seconds between requests to avoid detection    
    time.sleep(random.randint(1, 5))

    return s      



urls = ['https://www.immobiliare.it/#map-list',
        'https://www.immobiliare.it/aste-immobiliari/#map-list',
        'https://www.immobiliare.it/vendita-estero/#map-list',
        'https://www.immobiliare.it/vendita-terreni/#map-list',
        'https://www.immobiliare.it/affitto-estero/#map-list',
        'https://www.immobiliare.it/affitto-magazzini/#map-list',
        'https://www.immobiliare.it/vendita-capannoni/#map-list',
        'https://www.immobiliare.it/affitto-case/#map-list',
        'https://www.immobiliare.it/vendita-palazzi/#map-list',
        'https://www.immobiliare.it/vendita-uffici/#map-list',
        'https://www.immobiliare.it/affitto-attivita/#map-list',
        'https://www.immobiliare.it/affitto-terreni/#map-list',
        'https://www.immobiliare.it/vendita-magazzini/#map-list',
        'https://www.immobiliare.it/affitto-negozi/#map-list',
        'https://www.immobiliare.it/affitto-uffici/#map-list',
        'https://www.immobiliare.it/affitto-palazzi/#map-list',
        'https://www.immobiliare.it/affitto-capannoni/#map-list',
        'https://www.immobiliare.it/affitto-garage/#map-list',
        'https://www.immobiliare.it/vendita-attivita/#map-list',
        'https://www.immobiliare.it/vendita-negozi/#map-list',
        'https://www.immobiliare.it/nuove-costruzioni/#map-list',
        'https://www.immobiliare.it/vendita-seconde-case/#map-list',
        'https://www.immobiliare.it/vendita-garage/#map-list',
        'https://www.immobiliarei.it/affitto-stanze/#map-list']



def futures(s, url_in, workers=len(urls)):
    with concurrent.futures.ThreadPoolExecutor(max_workers = workers) as executor:
        future = [executor.map(s, url_in, timeout=100)]



def translate(col):
    t = Translator()
    t_col = col.apply(lambda x: t.translate(x, src='it', dest='en', timeout=100).text)
    print(t_col)
    return t_col



def transform(url_in):
    df_csv = pd.read_csv('scraped_csv.csv', on_bad_lines='warn', dtype=str)
    
    while url_in not in df_csv['URL'].values:

        prop_strainer = "div", {"class":"in-titleBlock__content"}
        prop_content = soup(url_in, prop_strainer).find_all("h1",{"class":"in-titleBlock__title"})
        if prop_content:
            prop_text = [txt.text.strip() for txt in prop_content]
            print(f"Property: {prop_text}\tOrigin: {url_in}")
        else:
            prop_text = []
    
        loc_strainer = "div", {"class":"in-titleBlock__content"}
        loc_content = soup(url_in, loc_strainer).find_all("span",{"class":"in-location"})
        if loc_content:
            loc_text = [txt.text.strip() for txt in loc_content]
            loc_text2 = [loc_text.remove(d) for d in loc_text if not d]
            print(f"Location: {loc_text}\tOrigin: {url_in}")
        else:
            loc_text = []
    
        desc_strainer = "div", {"class":"in-main in-landingDetail__main"}
        desc_content = soup(url_in, desc_strainer).find_all("div",{"class":"in-readAll"})
        if desc_content:
            desc_text = [txt.text.strip() for txt in desc_content]
            print(f"Descrition: {desc_text}\tOrigin: {url_in}")
        else:
            desc_text = []
    
        sell_strainer = "div", {"class":"in-side"}
        try:
            sell_content = soup(url_in, sell_strainer).find("div", {"class":"in-lead__content--albatross in-lead__contentData"})
            sell_content2 = sell_content.find_all("p")
            sell_text = [txt.text.strip() for txt in sell_content2]
            sell_text2 = [sell_text.remove(d) for d in sell_text if not d]
            print(f"Seller: {sell_text}\tOrigin: {url_in}")
    
        except AttributeError:
            sell_content = soup(url_in, sell_strainer).find_all("div", {"class":"in-lead__content--albatross in-lead__contentData"})
            sell_text = [txt.text.strip() for txt in sell_content]
    
        try:
            img_strainer = "div",{"class":"nd-slideshow__item"}
            img_content = soup(url_in, img_strainer).find("div",{"class":"nd-slideshow__content"})
    
            if img_content:
                img_text = [txt.find('img').get('src') for txt in img_content]
                print(f"Image: {img_text}\tOrigin: {url_in}")
    
            else:
                img_strainer = "div",{"class":"nd-figure__image nd-ratio nd-ratio--standard"}
                img_content = soup(url_in, img_strainer).find("figure",{"class":"nd-figure in-photo in-mosaic__photo"})
                img_text = [txt.find('img').get('src') for txt in img_content]
                print(f"Image: {img_text}\tOrigin: {url_in}")
    
        except TypeError:
            img_text = []
    
        url_strainer = "div", {"class":"in-titleBlock__content"}
        url_content = soup(url_in, url_strainer).find_all("h1",{"class":"in-titleBlock__title"})
        url_text = [txt.text.strip() for txt in url_content]
        url = [url_in]
        url_merge = [{a:b for a,b in zip(url_text, url)}]
        url_merge2 = [url_merge.remove(d) for d in url_merge if not d]
        url_merge = [d for d in url_merge for d in d.values()]
        print(f"URL: {url_merge}\tOrigin: {url_in}")
        
        id_text = [re.findall('\d+', s) for s in url_merge][0]
        print(f"ID: {id_text}\tOrigin: {url_in}")
        
        kv_strainer = "dl", {"class":"in-realEstateFeatures__list"}
        k = soup(url_in, kv_strainer).find_all("dt")
        v = soup(url_in, kv_strainer).find_all("dd")
        ks = [i.text.strip().upper() for i in k]
        vs = [i.text.strip() for i in v]
        kv_merge = {a:b for a,b in zip(ks, vs)}
        kv_merge2 = [kv_merge.remove(d) for d in kv_merge if not d]
        print(f"Details: {kv_merge}\tOrigin: {url_in}")
    
        webscrape_data = {'ID' : id_text, 'PROPERTY': prop_text, 'LOCATION': loc_text,'DESCRIPTION':desc_text, 'SELLER':sell_text, 'IMAGES': img_text,'URL':url_merge,'DETAILS':kv_merge}
        #results.append(webscrape_data)
    
        print(f"webscrape_data:{[webscrape_data]}")
        df = pd.DataFrame([webscrape_data])
        print(f"{url_in} : Result Converted to DataFrame")
    
        columns = ['ID','PROPERTY', 'LOCATION', 'DESCRIPTION', 'SELLER','IMAGES','URL']
        for col in columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].str.strip('[]')
            print(f"{url_in} : {col}")
    
        df['URL'] = df['URL'].str.strip("''")
        df['ID'] = df['ID'].str.strip("''")
    
        df = df.replace('', np.nan)
        df = df.dropna(subset=['URL'])
        df = df.reset_index(drop=True)
    
        new_df = df['DETAILS'].apply(pd.Series)
    
        t = Translator()
        new_df.columns = [t.translate(col, src='it', dest='en', timeout=10).text for col in new_df.columns]
    
        df_merge = df.join(new_df, how='inner')
        df_merge = df_merge.drop(columns = 'DETAILS')
        #df_merge = df_merge.drop_duplicates(ignore_index=True)
        
  
        with concurrent.futures.ThreadPoolExecutor() as executor:
            translated_cols = list(executor.map(translate, [df_merge[col] for col in df_merge.columns]))
        for i, col in enumerate(df_merge.columns):
            df_merge[col] = translated_cols[i]
        print(f"{url_in} : Translated")
        
        time.sleep(5)
        df_csv = pd.read_csv('scraped_csv.csv', on_bad_lines='warn', dtype=str)
        concatenated_df = pd.concat([df_csv, df_merge], ignore_index=True, sort=False)
        concatenated_df = concatenated_df.dropna(how='all')
        print(f"{url_in} : Concatenated")
        
        time.sleep(5)
        conn = sqlite3.connect('scraped_db.db')
        df_csv = pd.read_csv('scraped_csv.csv', on_bad_lines='warn', dtype=str)
        if  concatenated_df.shape[1] > df_csv.shape[1]:
            concatenated_df.to_csv('scraped_csv.csv',index=False)
            print(f"[{url_in}] OVERWRITTEN scraped_csv.csv")
            
            concatenated_df.to_sql('immobiliare', conn, if_exists='replace', index=False)
            conn.close()
            print(f"[{url_in}] REPLACED immobiliare table")
            
        else:
            concatenated_df.iloc[-1:].to_csv('scraped_csv.csv',index=False, mode='a', header=False)       
            print(f"[{url_in}] APPENDED scraped_csv.csv")
            
            concatenated_df.iloc[-1:].to_sql('immobiliare', conn, if_exists='append', index=False)
            conn.close()
            print(f"[{url_in}] APPENDED immobiliare table")
            
        print(f"[{url_in}]Total Scraped URLs: {len(df_csv)}")
            
        return



u = []
u1 = []
u2 = []
def scraper(url_in):
    strainer = "ul", {"class":"nd-listMeta nd-listMeta--columns"}
    a = soup(url_in, strainer).find_all("a", {"class":"nd-listMeta__link"})
    url = [links.get('href') for links in a]
    u.extend(url)

    print(f"1ST SCRAPED: [{url_in}]\t TXT FILE LENGTH: {len(u)}")

    for l in url:              
        strainer1 = "li", {"class":"nd-listMeta__item"}
        a = soup(l, strainer1).find_all("a", {"class":"nd-listMeta__link"})
        if a:
            url = [links.get('href') for links in a]
            u1.extend(url)
            
            print(f"2ND SCRAPED APPENDING: [{l}][{url_in}]\t TOTAL SCRAPED: {len(u1)}")
               
        else:
            strainer = "div",{"class":"in-pagination in-searchList__pagination"}
            a = soup(l, strainer).find_all("div",{"class":"in-pagination__list"})
            if a:
                for n in a:
                    n = n.text
                    if len(n) > 14:
                        for j in range(1, int(n[-3:])+1):
                            strainer = "ul", {"class":"nd-list in-realEstateResults"}
                            a = soup(l+f"?pag={j}", strainer).find_all("a", {"class":"in-card__title"})
                            url1 = [links.get('href') for links in a]
                            u2.extend(url1)
                            futures(transform, url1, workers=5)
                            print(f"3RD SCRAPED: [{l}?pag={j}][{l}]\t TOTAL SCRAPED: {len(u2)}")

                            for t in url1:
                                #transform(t)
                                print(f"{j}/{int(n[-3:])} [{l}][{t}] SAVED TO 'df_csv.csv'")                                

                    elif len(n) > 9:
                        for j in range(1, int(n[-2:])+1):
                            strainer = "ul", {"class":"nd-list in-realEstateResults"}
                            a = soup(l+f"?pag={j}", strainer).find_all("a", {"class":"in-card__title"})
                            url2 = [links.get('href') for links in a]
                            u2.extend(url2)
                            futures(transform, url2, workers=5)
                            print(f"3RD SCRAPED: [{l}?pag={j}][{l}]\t TOTAL SCRAPED: {len(u2)}")
                            
                            for t in url2:
                                #transform(t)
                                print(f"{j}/{int(n[-2:])} [{l}][{t}] SAVED TO 'df_csv.csv'") 
                                
                     
                    else:
                        for j in range(1, int(n[-1])+1):
                            strainer = "ul", {"class":"nd-list in-realEstateResults"}
                            a = soup(l+f"?pag={j}", strainer).find_all("a", {"class":"in-card__title"})
                            url3 = [links.get('href') for links in a]
                            u2.extend(url3)
                            futures(transform, url3, workers=5)
                            print(f"3RD SCRAPED: [{l}?pag={j}][{l}]\t TOTAL SCRAPED: {len(u2)}")

                            for t in url3:
                                #transform(t)
                                print(f"{j}/{int(n[-1:])} [{l}][{t}] SAVED TO 'df_csv.csv'") 
       
                                
            else:
                strainer2 = "ul", {"class":"nd-list in-realEstateResults"} 
                a = soup(l, strainer2).find_all("a", {"class":"in-card__title"})
                url4 = [links.get('href') for links in a]
                u2.extend(url4)
                futures(transform, url4, workers=5)
         
                for t in url4:
                    #transform(t)
                    print(f"[{l}][{t}] SAVED TO 'df_csv.csv'") 
                
            print(f"3RD SCRAPED WRITING: [{l}][{url_in}]\t TOTAL SCRAPED: {len(u2)}")



        for r in url:
            strainer = "div",{"class":"in-pagination in-searchList__pagination"}
            a = soup(r, strainer).find_all("div",{"class":"in-pagination__list"})
            if a:
                for n in a:
                    n = n.text
                    if len(n) > 14:
                        for j in range(1, int(n[-3:])+1):
                            strainer = "ul", {"class":"nd-list in-realEstateResults"}
                            a = soup(r+f"?pag={j}", strainer).find_all("a", {"class":"in-card__title"})
                            url1 = [links.get('href') for links in a]
                            u2.extend(url1)
                            futures(transform, url1, workers=5)
                            print(f"3RD SCRAPED: [{l}?pag={j}][{l}]\t TOTAL SCRAPED: {len(u2)}")
                            
                            for t in url1:
                                #transform(t)
                                print(f"{j}/{int(n[-3:])} [{r}][{t}] SAVED TO 'df_csv.csv'") 
                                

                    elif len(n) > 9:
                        for j in range(1, int(n[-2:])+1):
                            strainer = "ul", {"class":"nd-list in-realEstateResults"}
                            a = soup(r+f"?pag={j}", strainer).find_all("a", {"class":"in-card__title"})
                            url2 = [links.get('href') for links in a]
                            u2.extend(url2)
                            futures(transform, url2, workers=5)
                            print(f"3RD SCRAPED: [{l}?pag={j}][{l}]\t TOTAL SCRAPED: {len(u2)}")
                            
                            for t in url2:
                                #transform(t)
                                print(f"{j}/{int(n[-2:])} [{r}][{t}] SAVED TO 'df_csv.csv'") 
                                

                    else:
                        for j in range(1, int(n[-1])+1):
                            strainer = "ul", {"class":"nd-list in-realEstateResults"}
                            a = soup(r+f"?pag={j}", strainer).find_all("a", {"class":"in-card__title"})
                            url3 = [links.get('href') for links in a]
                            u2.extend(url3)
                            futures(transform, url3, workers=5)
                            print(f"3RD SCRAPED: [{l}?pag={j}][{l}]\t TOTAL SCRAPED: {len(u2)}")
                            
                            for t in url3:
                                #transform(t)
                                print(f"{j}/{int(n[-1:])} [{r}][{t}] SAVED TO 'df_csv.csv'") 
                                
                                
            else:
                strainer2 = "ul", {"class":"nd-list in-realEstateResults"} 
                a = soup(r, strainer2).find_all("a", {"class":"in-card__title"})
                url4 = [links.get('href') for links in a]
                u2.extend(url4)
                futures(transform, url4, workers=5)

                for t in url4:
                    #transform(t)
                    print(f"[{r}][{t}] SAVED TO 'df_csv.csv'") 
            
            print(f"3RD SCRAPED WRITING: [{l}][{url_in}]\t TOTAL SCRAPED: {len(u2)}")

        
if __name__ == '__main__':
    futures(scraper,urls)


