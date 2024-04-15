import scrapy
import psycopg2
from datetime import date
from immo_crawl.items import ImmoCrawlItem, HomeGateLoader
from immo_crawl.credentials import HOSTNAME, USERNAME, PASSWORD, DATABASE


class HomegateSpider(scrapy.Spider):
    """
    :class:`HomegateSpider` crawls the advertisements of all rentable real estate 
    on https://www.homegate.ch, and saves the data in :class:`~immo_crawl.items.ImmoCrawlItem`.

    Attributes:
        name (str): name of the Spider
        allowed_domains (list): list of allowed Domains
    """
    name = 'homegate'
    allowed_domains = ['homegate.ch']

    def start_requests(self):
        """
        Sends requests with the start_urls.

        Yields:
            :class:`scrapy:scrapy.http.Request`: HTTP request that is processed with :meth:`parse`.

        Attributes:
            start_urls (list): List with the start URL for each canton.
            canton_list (list): List with the abbreviations of the cantons (in the order of :attr:`start_urls`).
        """
        # Create a list of URLs that have already been visited
        conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        with conn.cursor() as cur:
            cur.execute('SELECT url FROM eva_data;')
            self.visited_urls = [url[0] for url in cur.fetchall()]
        conn.close()

        start_urls = [
            'https://www.homegate.ch/mieten/immobilien/kanton-aargau/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-appenzellinnerrhoden/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-appenzellausserrhoden/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-baselland/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-baselstadt/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-bern/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-freiburg/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-genf/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-glarus/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-graubuenden/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-jura/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-luzern/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-neuenburg/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-nidwalden/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-obwalden/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-schaffhausen/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-schwyz/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-solothurn/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-stgallen/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-tessin/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-thurgau/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-uri/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-waadt/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-wallis/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-zug/trefferliste',
            'https://www.homegate.ch/mieten/immobilien/kanton-zuerich/trefferliste',
        ]
        canton_list = ['AG', 'AI', 'AR', 'BL', 'BS', 'BE', 'FR', 'GE', 'GL', 'GR',
                       'JU', 'LU', 'NE', 'NW', 'OW', 'SH', 'SZ', 'SO', 'SG', 'TI',
                       'TG', 'UR', 'VD', 'VS', 'ZG', 'ZH']
        canton_dict = dict(zip(start_urls, canton_list))

        # Send requests for the start URLs
        for start_url in start_urls:
            canton = canton_dict[start_url]
            yield scrapy.Request(start_url, callback=self.parse, cb_kwargs={'canton': canton})

    def parse(self, response, canton):
        """
        HTTP responses are processed that were requested with :meth:`start_requests` or :meth:`parse`.
        Links to advertisements are processed using :meth:`parse_item`. Only requests 
        for advertisements that have not yet been scraped will be sent. The current date is added to the database for existing URLs.

        Parameters:
            response (scrapy.http.Response): HTTP response to a sent request
            canton (str): Canton abbreviation assigned to the HTTP request sent

        Yields:
            :class:`scrapy:scrapy.http.Request`: HTTP request that will be processed with :meth:`parse` or :meth:`parse_item`
        """
        # Processing the results pages
        for item_link in response.css('div[data-test="result-list"] a'):
            url = response.urljoin(item_link.css('::attr(href)').get())
            # If the URL is already in the database, the date is added to it (date_last_seen)
            if url in self.visited_urls:
                conn = psycopg2.connect(
                    host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
                with conn.cursor() as cur:
                    cur.execute(
                        'UPDATE eva_data SET date_last_seen = %s WHERE url = %s;', (date.today(), url))
                    conn.commit()
                conn.close()
                continue
            else:
                yield response.follow(item_link, self.parse_item, cb_kwargs={'canton': canton})

        # Request for the next page
        next_page = response.css(
            'a[aria-label="Zur nächsten Seite"]::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse, cb_kwargs={'canton': canton})

    def parse_item(self, response, canton):
        """
        HTTP responses from :meth:`parse` are processed. The collected data will 
        be written to an Item by a Loader object. 

        Parameters:
            response (scrapy.http.Response): HTTP response to a sent request
            canton (str): Canton abbreviation assigned to the HTTP request sent

        Yields:
            :class:`~immo_crawl.items.ImmoCrawlItem`: The Item that will be processed by
            :attr:`~immo_crawl.settings.ITEM_PIPELINES`.

        Attributes:
            loader (:class:`~immo_crawl.items.HomeGateLoader`): object to collect data from HTML write to the Item
            immo_item (:class:`~immo_crawl.items.ImmoCrawlItem`): container for storing the data to be written to the database
        """
        loader = HomeGateLoader(item=ImmoCrawlItem(), response=response)
        loader.add_xpath('address', '//address/span/text()')
        loader.add_xpath('zip_code', '//address/span/text()')
        loader.add_xpath('city', '//address/span/text()')
        loader.add_value('canton', canton)
        loader.add_xpath(
            'rooms', '//div[h2="Eckdaten"]//dt[text()="Anzahl Zimmer:"]/following-sibling::*[1]//text()')
        loader.add_xpath(
            'area_m2', '//div[h2="Eckdaten"]//dt[text()="Wohnfläche:"]/following-sibling::*[1]/text()')
        loader.add_xpath(
            'price_chf', '//div[h2="Kosten"]//dt[text()="Miete:"]/following-sibling::*[1]//text()')
        loader.add_xpath('date_available', '//div[h2="Verfügbarkeit"]//dt[text()="Verfügbar ab:"]'
                                           '/following-sibling::*[1]//text()')
        loader.add_xpath(
            'floor', '//div[h2="Eckdaten"]//dt[text()="Etage:"]/following-sibling::*[1]/text()')
        loader.add_xpath('utilities_chf', '//div[h2="Kosten"]//dt[text()="Nebenkosten:"]'
                                          '/following-sibling::*[1]//text()')
        loader.add_value('date_scraped', date.today())
        loader.add_value('date_last_seen', date.today())
        loader.add_value('url', response.request.url)

        immo_item = loader.load_item()

        yield immo_item
