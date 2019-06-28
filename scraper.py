from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import datetime
import json
import argparse
from kafka import KafkaProducer
import sys
import time

# Some statically defined variables.
url = "http://maven-repository.com/artifact/latest?page={0}"
date_format = "%Y-%m-%d %H:%M:%S"


class MavenRelease:
    """
        Represents a Maven Release.
    """

    def __init__(self, group_id, artifact_id, version, date):
        self.group_id = group_id
        self.artifact_id = artifact_id
        self.version = version
        self.date = date

    # Prints a release.
    def print_release(self):
        print("Release: {0}-{1}-{2}. Uploaded at: {3}".format(self.group_id, self.artifact_id, self.version, self.date))

    # Dumps a Maven Release into JSON format.
    def to_json(self):
        return json.dumps({
            "groupId": self.group_id,
            "artifactId": self.artifact_id,
            "version": self.version,
            "date": str(self.date)
        })


def retrieve_page(page_id):
    """
        Returns a Maven Releases HTML page.
    """
    try:
        with closing(get(url.format(page_id), stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        print('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
        Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)


def parse_page(page):
    """
        Parses a Maven HTML page and parses the releases.
    """
    html = BeautifulSoup(page, 'html.parser')
    releases = []
    for i, td in enumerate(html.select('tr')):
        artifact = td.select('td')
        if len(artifact) == 0: # If it is not an artifact, skip it.
            continue

        group_id = artifact[0].text
        artifact_id = artifact[1].text
        version = artifact[2].text
        date = datetime.datetime.strptime(artifact[3].text, date_format)

        releases.append(MavenRelease(group_id, artifact_id, version, date))

    return releases


def get_until_date(date):
    """
        Get all Maven Releases until a certain date.
    """

    date_found = False
    page_id = 1
    all_releases = []

    # Hacky way to retrieve all releases.
    while date_found is False:
        new_releases = parse_page(retrieve_page(page_id))
        new_releases_trim = list(filter(lambda x: x.date > date, new_releases))

        if len(new_releases_trim) < len(new_releases):
            date_found = True
            all_releases = all_releases + new_releases_trim
            break
        else:
            page_id += 1
            all_releases = all_releases + new_releases

    # Return them sorted.
    return sorted(all_releases, key=lambda x: x.date)


def produce_to_kafka(topic, servers, until_date):
    """
        Get all Maven Releases until a certain date and push to Kafka.
    """
    releases = get_until_date(until_date)

    producer = KafkaProducer(bootstrap_servers=servers.split(','), value_serializer=lambda x: x.encode('utf-8'))

    for release in releases:
        producer.send(topic, release.to_json())

    producer.flush()
    print("Sent {0} releases.".format(len(releases)))

    # Return latest date (if any new releases are found).
    if len(releases) is 0:
        return until_date
    return releases[-1].date

# Setup the scrape arguments.
parser = argparse.ArgumentParser("Scrape Maven releases to Kafka.")
parser.add_argument('start_date', type=lambda s: datetime.datetime.strptime(s, date_format),
                    help="The date to start scraping from. Must be in %Y-%m-%d %H:%M:%S format.")
parser.add_argument('topic', type=str, help="Kafka topic to push to.")
parser.add_argument('bootstrap_servers', type=str, help="Kafka servers, comma separated.")
parser.add_argument('sleep_time', type=int, help="Time to sleep in between each scrape (in sec).")


def main():
    args = parser.parse_args()

    # Retrieve all arguments.
    kafka_topic = args.topic
    latest_date = args.start_date
    bootstrap_servers = args.bootstrap_servers
    sleep_time = args.sleep_time

    # Forever: get releases from start_date, update latest_date based on latest release and push this to Kafka.
    while True:
        print("{0}: Scraping releases from {1} to now. Sending to {2}.".format(str(datetime.datetime.now()), str(latest_date), kafka_topic))
        latest_date = produce_to_kafka(kafka_topic, bootstrap_servers, latest_date)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
