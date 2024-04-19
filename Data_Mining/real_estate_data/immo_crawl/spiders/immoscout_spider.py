import scrapy
import psycopg2
from datetime import date
from immo_crawl.items import ImmoCrawlItem, ImmoScoutLoader
from immo_crawl.credentials import HOSTNAME, USERNAME, PASSWORD, DATABASE


class ImmoscoutSpider(scrapy.Spider):
    """
    :class:`ImmoscoutSpider` crawls the advertisements of all rentable real estate 
    on https://www.immoscout24.ch and saves the data in :class:`~immo_crawl.items.ImmoCrawlItem`.

    Attributes:
        name (str): name of the spider
        allowed_domains (list): list of allowed domains
    """
    name = 'immoscout'
    allowed_domains = ['immoscout24.ch']

    def start_requests(self):
        """
        Sends requests with the start_urls.

        Yields:
            :class:`scrapy:scrapy.http.Request`: HTTP request that is processed with :meth:`parse`.

        Attributes:
            start_urls (list): List with the start URL for each canton.
        """
        # Create a list of URLs that have already been visited
        conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        with conn.cursor() as cur:
            cur.execute('SELECT url FROM eva_data;')
            self.visited_urls = [url[0] for url in cur.fetchall()]
        conn.close()

        start_urls = [
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-aargau?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-appenzell-ai?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-appenzell-ar?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-basel-landschaft?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-basel-stadt?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-bern?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-freiburg?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-genf?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-glarus?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-graubuenden?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-jura?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-luzern?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-neuenburg?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-nidwalden?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-obwalden?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-schaffhausen?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-schwyz?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-solothurn?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-st-gallen?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-tessin?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-thurgau?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-uri?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-waadt?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-wallis?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-zug?map=1&pn={}&se=16',
            'https://www.immoscout24.ch/de/immobilien/mieten/kanton-zuerich?map=1&pn={}&se=16',
        ]
        # Send requests for the start URLs
        page_nr = 1
        for start_url in start_urls:
            yield scrapy.Request(start_url.format(page_nr), callback=self.parse, cb_kwargs={'start_url': start_url})

    def parse(self, response, start_url=None):
        """
        HTTP responses are processed that were requested with :meth:`start_requests` or :meth:`parse`.
        Links to advertisements are processed using :meth:`parse_item`. If the response follows a request
        from :attr:`start_urls`, the URLs of the other result pages still have to be created. Only requests 
        for advertisements that have not yet been scraped will be sent. The current date is added to the 
        database for existing URLs.

        Parameters:
            response (scrapy.http.Response): HTTP response to a sent request
            start_url (str): requested URL, when from :attr:`start_urls` it defaults to ``None``.

        Yields:
            :class:`scrapy:scrapy.http.Request`:HTTP request that will be processed with :meth:`parse` or :meth:`parse_item`
        """
        # Processing the results pages
        for item_link in response.css('article a'):
            url = response.urljoin(item_link.css('::attr(href)').get())
            url = url.split('?')[0]
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
                yield response.follow(item_link, self.parse_item)

        # Create links if response to a start URL
        if start_url:
            last_page = response.xpath(
                '//span[text()="Vorwärts"]/../../preceding-sibling::div[1]/*[last()]/text()').get()
            if last_page:
                for page_num in range(int(last_page) - 1):
                    yield scrapy.Request(start_url.format(page_num + 2), callback=self.parse)

    def parse_item(self, response):
        """
        HTTP responses from :meth:`parse` are processed. The collected data will 
        be written to an Item by a Loader object. 

        Parameters:
            response (scrapy.http.Response): HTTP response to a sent request

        Yields:
            :class:`~immo_crawl.items.ImmoCrawlItem`: The Item that will be processed by
            :attr:`~immo_crawl.settings.ITEM_PIPELINES`

        Attributes:
            loader (:class:`~immo_crawl.items.ImmoScoutLoader`): object to collect data from HTML write to the Item
            immo_item (:class:`~immo_crawl.items.ImmoCrawlItem`): container for storing the data to be written to the database
        """
        # Scraping the data
        loader = ImmoScoutLoader(item=ImmoCrawlItem(), response=response)
        loader.add_xpath('address', '//article[h2="Standort"]//p/text()')
        loader.add_xpath('zip_code', '//article[h2="Standort"]//p/text()')
        loader.add_xpath('city', '//article[h2="Standort"]//p/text()')
        loader.add_xpath('canton', '//article[h2="Standort"]//p/text()')
        loader.add_xpath('rooms', '//article[1]/h2/text()')
        loader.add_xpath('area_m2', '//article[1]/h2/text()')
        loader.add_xpath('price_chf', '//article[1]/div/h2/text()')
        loader.add_xpath(
            'date_available', '//article[h2="Hauptangaben"]//tr[td="Verfügbarkeit"]//text()')
        loader.add_xpath(
            'floor', '//article[h2="Hauptangaben"]//tr[td="Stockwerk"]//text()')
        loader.add_xpath(
            'utilities_chf', '//article[h2="Preis"]//tr[starts-with(td,"Neben")]//text()')
        loader.add_value('date_scraped', date.today())
        loader.add_value('date_last_seen', date.today())
        loader.add_value('url', response.request.url.split('?')[0])

        immo_item = loader.load_item()

        yield immo_item
