# common-crawl-project
Just a little project playing with common crawl

The objective of this projiect is to find all hyperlinks in a common crawl, and record the origin and destination pairs in a database

## Code

In the src repo of this project, you will find:
- `common_crawl_downloader.py`, a script made to download a common crawl partition in the `common-crawl-project/raw/` directory. `awscli` must be set up in order for this script to work.
- `create_db.py`, a script that will create a sqlite database in the `common-crawl-project/db/` directory, and create a `links` table that has columns for `link_origin`, `link_destination`, `source_archive`, and `is_valid` columns.
- `count_domains.py` reads all partitions in `common-crawl-project/raw` and parses 10000 web pages and outputs their links in the `links` sqlite table. This script is single thread.
- `count_domains_distributed.py` is the distributed version of `count_domains.py` that runs on Spark, and will process all partitions found entirely (instead of only 10000 pages)

