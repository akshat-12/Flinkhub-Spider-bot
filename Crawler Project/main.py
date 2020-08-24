import requests
from bs4 import BeautifulSoup
import traceback
import urllib
import datetime
import pymongo
from cfg import config
from logger import logger
import os
import time
import math
import random
import string
import concurrent


def crawler():
    # Establishing connection with mongodb
    client = pymongo.MongoClient(config["localhost"], config["port_num"])
    db = client[config['database_name']]
    col = db[config['collection_name']]

    # starting scraping
    if col.count_documents({}) == 0:  # if collection is empty : scrape flinkhub.com
        links_list1 = []
        headers1 = {
            'User-Agent': config['user_agent'],
        }
        try:  # send request
            logger.debug("Making HTTP GET request: " + config['host_name'])
            r1 = requests.get(config['host_name'], headers=headers1)
            res1 = r1.text
            logger.debug("Got HTML source, content length = " + str(len(res1)))

        except:  # if cannot request url
            logger.exception("Failed to get HTML source from " + config['host_name'])
            traceback.print_exc()
            return links_list1

        logger.debug("Extracting links from the HTML")
        soup1 = BeautifulSoup(res1, 'html.parser')  # converting request to soup object

        # saving html content to a .txt file
        try:
            file_name1 = ''.join(random.choices(string.ascii_uppercase +
                                                string.digits, k=16))
            file_name1 = file_name1 + '.txt'
            file_path1 = os.path.join(os.getcwd(), config["file_dir"], file_name1)
            text_file1 = open(file_path1, "w")
            n1 = text_file1.write(str(soup1))
            text_file1.close()
        except:
            logger.exception("Cannot write link in a file.")

        if 'Content-Length' in r1.headers:
            new_doc = {
                "link": config["host_name"],
                "source_link": None,
                "is_crawled": True,
                "last_crawl_date": datetime.datetime.utcnow(),
                "response_status": r1.status_code,
                "content_type": r1.headers['Content-Type'],
                "con_length": r1.headers['Content-Length'],
                "file_path": file_path1,
                "created_at": datetime.datetime.utcnow(),
            }
        else:
            new_doc = {
                "link": config["host_name"],
                "source_link": None,
                "is_crawled": True,
                "last_crawl_date": datetime.datetime.utcnow(),
                "response_status": r1.status_code,
                "content_type": r1.headers['Content-Type'],
                "con_length": len(r1.content),
                "file_path": file_path1,
                "created_at": datetime.datetime.utcnow(),
            }
        col.insert_one(new_doc)  # inserting original link to a document

        links1 = soup1.find_all("a")  # finding all a tags

        for link1 in links1:  # iterating over all the links
            temp1 = link1.get('href')  # getting url
            if temp1 not in links_list1:  # if link was not scraped in the same cycle
                links_list1.append(temp1)

                # checking validity of link
                temp_parse1 = urllib.parse.urlparse(temp1)
                netloc_bool1 = bool(temp_parse1.netloc)
                scheme_bool1 = bool(temp_parse1.scheme)
                if netloc_bool1:
                    if scheme_bool1:
                        # if link is valid and absolute url
                        actual_link1 = temp1
                        query1 = {"link": actual_link1}
                        myq1 = col.find(query1)
                        if myq1.count == 0:
                            temp_doc1 = {
                                "link": actual_link1,
                                "source_link": config['host_name'],
                                "is_crawled": False,
                                "last_crawl_date": None,
                                "response_status": None,
                                "content_type": None,
                                "con_length": None,
                                "file_path": None,
                                "created_at": datetime.datetime.utcnow()
                            }
                            col.insert_one(temp_doc1)  # adding link to a document
                        else:
                            print(temp1 + " already exists in database.")  # if link already exists in database
                    else:
                        print(temp1 + " link not valid")  # if link is not valid
                else:
                    # if link is a relative url
                    actual_link2 = urllib.parse.urljoin(config['host_name'], temp1)
                    netloc_bool2 = bool(urllib.parse.urlparse(actual_link2))
                    scheme_bool2 = bool(urllib.parse.urlparse(actual_link2))
                    if netloc_bool2 and scheme_bool2:  # if relative url is valid
                        query2 = {"link": actual_link2}
                        if col.count_documents(query2) == 0:  # if link doesn't exist in collection already
                            temp_doc1 = {
                                "link": actual_link2,
                                "source_link": config['host_name'],
                                "is_crawled": False,
                                "last_crawl_date": None,
                                "response_status": None,
                                "content_type": None,
                                "con_length": None,
                                "file_path": None,
                                "created_at": datetime.datetime.utcnow()
                            }
                            col.insert_one(temp_doc1)  # inserting link to collection
                        else:
                            print(str(actual_link2) + " already exists.")  # if link already exists in collection
                    else:
                        print(actual_link2 + " not valid")  # if link is not valid
            else:
                print(temp1 + " Link already scraped")  # if link is already scraped in the same cycle
        return links1

    else:  # if there exist some links in the collection already
        if col.count_documents({"is_crawled": False}) > 0:  # if there exist some documents which are not crawled

            # picking a random link from the collection to be scraped
            num1 = col.count_documents({"is_crawled": False})
            random1 = math.floor(random.random() * num1)
            cursor_doc = col.find({"is_crawled": False}).limit(1).skip(random1)
            for curs in cursor_doc:
                doc = curs
            links_list2 = []
            og_link = doc['link']
            headers2 = {
                'User-Agent': config['user_agent'],
            }
            try:  # requesting link
                logger.debug("Making HTTP GET request: " + og_link)
                r2 = requests.get(og_link, headers=headers2)
                res2 = r2.text
                logger.debug("Got HTML source, content length = " + str(len(res2)))
            except:
                logger.exception("Failed to get HTML source from " + og_link)
                traceback.print_exc()
                return links_list2

            logger.debug("Extracting links from the HTML")
            soup2 = BeautifulSoup(res2, 'html.parser')  # converting request to a soup object
            # saving html content to a file
            try:
                file_name2 = ''.join(random.choices(string.ascii_uppercase +
                                                    string.digits, k=16))
                file_name2 = file_name2 + '.txt'
                file_path_2 = os.path.join(os.getcwd(), config['file_dir'], file_name2)
                text_file2 = open(file_path_2, "w")
                n2 = text_file2.write(str(soup2))
                text_file2.close()
            except:
                logger.exception("Cannot write link in a file.")

            if 'Content-Length' in r2.headers:
                updated_doc = {
                    "is_crawled": True,
                    "last_crawl_date": datetime.datetime.utcnow(),
                    "response_status": r2.status_code,
                    "content_type": r2.headers['Content-Type'],
                    "con_length": r2.headers['Content-Length'],
                    "file_path": file_path_2,
                }
            else:
                updated_doc = {
                    "is_crawled": True,
                    "last_crawl_date": datetime.datetime.utcnow(),
                    "response_status": r2.status_code,
                    "content_type": r2.headers['Content-Type'],
                    "con_length": len(r2.content),
                    "file_path": file_path_2,
                }

            col.update_one(doc, {"$set": updated_doc})    # updating link which was just scraped

            links2 = soup2.find_all("a")    # converting request to a soup object
            for link2 in links2:
                temp2 = link2.get('href')    # getting link from a tag
                if temp2 not in links_list2:    # itertaing through links
                    links_list2.append(temp2)

                    # checking validity of links
                    temp_parse3 = urllib.parse.urlparse(temp2)
                    netloc_bool3 = bool(temp_parse3.netloc)
                    scheme_bool3 = bool(temp_parse3.scheme)
                    if netloc_bool3:
                        if scheme_bool3:
                            # valid absolute link
                            actual_link3 = temp2
                            query3 = {"link": actual_link3}
                            if col.count_documents(query3) == 0:
                                temp_doc = {
                                    "link": actual_link3,
                                    "source_link": og_link,
                                    "is_crawled": False,
                                    "last_crawl_date": None,
                                    "response_status": None,
                                    "content_type": None,
                                    "con_length": None,
                                    "file_path": None,
                                    "created_at": datetime.datetime.utcnow()
                                }
                                col.insert_one(temp_doc)   # adding link to the collection
                            else:
                                print(temp2 + " already exists.")    # if link already exists in collection
                        else:   # if link is not valid
                            print(temp2 + " link not valid")
                    else:
                        # link is a relative link
                        actual_link4 = urllib.parse.urljoin(og_link, temp2)
                        netloc_bool4 = bool(urllib.parse.urlparse(actual_link4))
                        scheme_bool4 = bool(urllib.parse.urlparse(actual_link4))
                        if netloc_bool4 and scheme_bool4:
                            # valid relative link
                            query4 = {"link": actual_link4}
                            if col.count_documents(query4) == 0:    # checking for existence of link in collection
                                temp_doc = {
                                    "link": actual_link4,
                                    "source_link": og_link,
                                    "is_crawled": False,
                                    "last_crawl_date": None,
                                    "response_status": None,
                                    "content_type": None,
                                    "con_length": None,
                                    "file_path": None,
                                    "created_at": datetime.datetime.utcnow()
                                }
                                col.insert_one(temp_doc)    # adding link to the collection
                            else:
                                print(actual_link4 + " already exists.")    # link already exists in collection
                        else:
                            print(actual_link4 + " not valid")  # link is not valid
            return links2   # return list of links found

        else:   # if there are no links which are not crawled yet
            valid_docs = col.find({})
            # finding links which were not crawled in last 24 hours
            time_dif = datetime.timedelta(days=1)
            greater_than_24_docs = []
            for single_doc in valid_docs:
                if single_doc["last_crawl_date"] > time_dif:
                    greater_than_24_docs.append(single_doc)
            num2 = len(greater_than_24_docs)
            # picking a random link out of those links which were not crawled in last 24 hours
            random2 = random.randint(0, num2 - 1)
            doc = greater_than_24_docs[random2]
            links_list2 = []
            og_link = doc.link
            headers2 = {
                'User-Agent': config['user_agent'],
            }
            # making a https request
            try:
                logger.debug("Making HTTP GET request: " + og_link)
                r2 = requests.get(og_link, headers=headers2)
                res2 = r2.text
                logger.debug("Got HTML source, content length = " + str(len(res2)))
            except:
                logger.exception("Failed to get HTML source from " + og_link)
                traceback.print_exc()
                return links_list2

            logger.debug("Extracting links from the HTML")
            soup2 = BeautifulSoup(res2, 'html.parser')  # turning request into soup object

            try:
                # writing html content to a txt file
                file_name2 = ''.join(random.choices(string.ascii_uppercase +
                                                    string.digits, k=16))
                file_name2 = file_name2 + '.txt'
                file_path2 = os.path.join(os.getcwd(), config['file_dir'], file_name2)
                text_file2 = open(file_path2, "w")
                n2 = text_file2.write(str(soup2))
                text_file2.close()
            except:
                logger.exception("Cannot write link in a file.")

            if 'Content-Length' in r2.headers:
                updated_doc = {
                    "is_crawled": True,
                    "last_crawl_date": datetime.date.today(),
                    "response_status": r2.status_code,
                    "content_type": r2.headers['Content-Type'],
                    "con_length": r2.headers['Content-Length'],
                    "file_path": file_path2,
                }

            else:

                updated_doc = {
                    "is_crawled": True,
                    "last_crawl_date": datetime.date.today(),
                    "response_status": r2.status_code,
                    "content_type": r2.headers['Content-Type'],
                    "con_length": len(r2.content),
                    "file_path": file_path2,
                }

            col.update_one(doc, {"$set": updated_doc})  # updating the recently crawled link document

            links2 = soup2.find_all("a")    # finding all anchor tags
            for link2 in links2:    # iterating through a tags
                temp2 = link2.get('href')   # geting the link from a tag
                if temp2 not in links_list2:    # if link wasn't found in this cycle
                    links_list2.append(temp2)
                    # checking for validity of link
                    temp_parse3 = urllib.parse.urlparse(temp2)
                    netloc_bool3 = bool(temp_parse3.netloc)
                    scheme_bool3 = bool(temp_parse3.scheme)
                    if netloc_bool3:
                        if scheme_bool3:
                            # link is absolute url and valid
                            actual_link3 = temp2
                            query3 = {"link": actual_link3}
                            if col.count_documents(query3) == 0:
                                temp_doc = {
                                    "link": actual_link3,
                                    "source_link": og_link,
                                    "is_crawled": False,
                                    "last_crawl_date": None,
                                    "response_status": None,
                                    "content_type": None,
                                    "con_length": None,
                                    "file_path": None,
                                    "created_at": datetime.datetime.utcnow()
                                }
                                col.insert_one(temp_doc)    # adding link to the collection
                            else:
                                print(temp2 + " already exists.")   # if link already exists in the collection
                        else:
                            print(temp2 + " link not valid")    # link is not valid
                    else:
                        # link is a relative link
                        actual_link4 = urllib.parse.urljoin(og_link, temp2)
                        netloc_bool4 = bool(urllib.parse.urlparse(actual_link4))
                        scheme_bool4 = bool(urllib.parse.urlparse(actual_link4))
                        if netloc_bool4 and scheme_bool4:
                            # link is relative and valid
                            query4 = {"link": actual_link4}
                            if col.count_documents(query4) == 0:
                                temp_doc = {
                                    "link": actual_link4,
                                    "source_link": og_link,
                                    "is_crawled": False,
                                    "last_crawl_date": None,
                                    "response_status": None,
                                    "content_type": None,
                                    "con_length": None,
                                    "file_path": None,
                                    "created_at": datetime.datetime.utcnow()
                                }
                                col.insert_one(temp_doc)    # adding link document to collection
                            else:
                                print(actual_link4 + " already exists.")    # link already exists in collection
                        else:
                            print(actual_link4 + " not valid")  # link is not valid
            return link2


def thread_crawler():   # function to make 5 parallel threads
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            threads = [executor.submit(crawler) for i in range(5)]
            results = [thread.result() for thread in threads]
            return results  # list of lists returned by 5 threads
    except:
        logger.debug("Threads couldn't be made.")


if __name__ == "__main__":
    while True:
        logger.debug('Starting process')
        scraped_links = thread_crawler()    # starting process
        for result in scraped_links:
            try:
                logger.debug("Extracted " + str(len(result)) + " links from HTML")
            except:
                logger.debug("Nothing returned in this cycle.")

        main_client = pymongo.MongoClient(config['localhost'], config['port_num'])
        database = main_client[config['database_name']]
        collection = database[config['collection_name']]
        if collection.count_documents({}) >= config['max_limit']:  # check if we have scraped max tweets yet
            logger.debug(str(config['max_limit']) + " links scraped. Ending process!!!")
            break
        time.sleep(5.0)
