# Standard libraries
import os
from pathlib import Path
import re
import time
import datetime
import sys

# Third Parties
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import unquote, urlparse
import sqlite3
import pandas as pd
import dateutil.parser
# from pyspark import SparkContext, SparkSession

start = time.process_time()

# Internal
SRC_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent
sys.path.append(str(SRC_DIR)+'/')

class cc_reader:
    """
        Common Crawl reader class. Designed to read common crawl archives from the `archive` directory
        And write output to the database connection `conn`
    """


    def __init__(
        self,
        archives,
        ):
        """
        Args:
            conn {db connection}: Database that cc_reader dumps into
            archives {list[str]}: list of warc files that cc_reader will read from
        """        
        self.archives = archives
        self.output = pd.DataFrame({
            'link_origin': [], 
            'link_destination': [], 
            'source_archive': [], 
            'is_valid': []})



    def read_warc_archives(self):
        df = self.output
        for archive in self.archives:
            records = []
            
            print('before iterating records', time.process_time() - start)
            
            num_records = 0
            with open(archive, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type == 'response':
                        num_records +=1
                        if num_records >= 2000: break
                        try:
                            parser = BeautifulSoup(
                                record.content_stream().read(), features="html.parser")
                        except:
                            return
                        links = parser.find_all("a")
                        print(f"{num_records}: Processing {record.rec_headers['WARC-Target-URI']}")
                                    
                        if links:
                            for link in links:
                                href = link.attrs.get("href")
                                if (href is not None) and href.startswith("http"):
                                    
                                    link_origin = record.rec_headers['WARC-Target-URI']
                                    link_destination = href
                                    source_archive = archive.split('/')[-1]
                                    
                                    output_dict = {
                                        'link_origin': link_origin,
                                        'link_destination': link_destination,
                                        'source_archive': source_archive,
                                        'is_valid': True
                                        }
                                        
                                    self.output = pd.concat([
                                        self.output, 
                                        pd.DataFrame(output_dict, index=[1])
                                    ])
                        


    def insert_data_to_db(self, conn):
        """
            Inserts self.output to database
        """
        
        self.output.to_sql(name='links', con=conn, if_exists='append', index=False)



def main():
    # Setup db connection
    conn = sqlite3.connect(f"{PROJECT_DIR}/db/common-crawl-project.db")

    # Find common crawl files
    cc_archives = []
    for file in os.listdir(f"{PROJECT_DIR}/raw/"):
        cc_archives.append(f"{PROJECT_DIR}/raw/{file}")
    
    reader = cc_reader(cc_archives)
    print('create_reader', time.process_time() - start)
    reader.read_warc_archives()
    reader.insert_data_to_db(conn)
    print('insert_data_to_db', time.process_time() - start)
    
    
if __name__ == '__main__':
    # spark = SparkSession.builder.appName("common-crawl-project").getOrCreate()
    main()
    # spark.stop()