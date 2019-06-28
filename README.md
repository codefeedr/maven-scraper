# Incremental Maven releaes to Kafka
This Python script scrapes [maven-repository.com](maven-repository) and forwards it to Kafka.

## Prerequisites
Install all dependencies:
```bash 
python3 -m venv venv
. ./venv/bin/activate
pip install requests BeautifulSoup4
```

## How To Run

