# Incremental Maven releases to Kafka
This Python script scrapes [maven-repository.com](maven-repository) and forwards it to Kafka.

## Prerequisites
Install all dependencies:
```bash 
python3 -m venv venv
. ./venv/bin/activate
pip install requests BeautifulSoup4 kafka-python
```

## How To Run
```bash
usage: Scrape Maven releases to Kafka. [-h]
                                       start_date topic bootstrap_servers
                                       sleep_time
```

For example:
```sh
python scraper.py '2019-06-24 14:05:50' cf_mvn_releases localhost:29092 60
```

**Note**: `start_date` must be in `%Y-%m-%d %H:%M:%S` format. Multiple bootstrap servers should be `,` separated. Sleep time is in _seconds_.
## Sample data
Data will be send in the following format:
```json
{
  "groupId": "com.g2forge.alexandria",
  "artifactId": "alexandria",
  "version": "0.0.9",
  "date": "2019-06-24 14:42:49"
}
```

