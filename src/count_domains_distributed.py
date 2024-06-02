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


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName('cc_count_domains').getOrCreate()



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
        
    
    @staticmethod
    def process_warc_arhive_stream(archive):
        """
            Accepts a warc archive and processes records in archive as a stream
            
            Args:
                archive {str}: Location of warc archive to be processed
        """
        processed_records = 0
        records=[]
        
        
            
        with open(archive, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    processed_records +=1
                    # if processed_records >= 1667 : break
                    try:
                        parser = BeautifulSoup(
                            record.content_stream().read(), features="html.parser")
                        
                        links = parser.find_all("a")
                        print(f"{processed_records}: Processing {record.rec_headers['WARC-Target-URI']}")
                                    
                        if links:
                            for link in links:
                                href = link.attrs.get("href")
                                if (href is not None) and href.startswith("http"):
                                    
                                    link_origin = record.rec_headers['WARC-Target-URI']
                                    link_destination = href
                                    source_archive = archive.split('/')[-1]     
                                    
                                    records.append((link_origin, link_destination, source_archive, True))
                                    

                                    if processed_records%2000 == 0:
                                        conn = sqlite3.connect(f"{PROJECT_DIR}/db/common-crawl-project.db")
                                        pd.DataFrame(records, columns = ['link_origin', 'link_destination', 'source_archive', 'is_valid']).to_sql(name='links', con=conn, if_exists='append', index=False)
                                        conn.close()
                                        records.clear()
                                
                    except:
                        continue
        
        output = pd.DataFrame(records, columns = ['link_origin', 'link_destination', 'source_archive', 'is_valid'])
        return output
        

    def read_warc_archives(self):
        rdd = spark.sparkContext.parallelize(self.archives)
        
        output_rdd = rdd.map(self.process_warc_arhive_stream)        
        rdd_output = output_rdd.collect()
        
        self.output = pd.concat(rdd_output)
        
    def insert_data_to_db(self, conn):
        """
            Inserts self.output to database
        """
        print(self.output)
        self.output.to_sql(name='links', con=conn, if_exists='append', index=False)



def main():
    # Setup db connection
    conn = sqlite3.connect(f"{PROJECT_DIR}/db/common-crawl-project.db")

    # Find common crawl files
    cc_archives = []
    for file in os.listdir(f"{PROJECT_DIR}/raw/"):
        cc_archives.append(f"{PROJECT_DIR}/raw/{file}")
    
    reader = cc_reader(cc_archives)
    reader.read_warc_archives()
    reader.insert_data_to_db(conn)

    
    
if __name__ == '__main__':
    main()
