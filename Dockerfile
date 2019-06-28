FROM python:3

ADD scraper.py /

RUN pip install requests BeautifulSoup4 kafka-python

ENTRYPOINT ["python", "./scraper.py"]