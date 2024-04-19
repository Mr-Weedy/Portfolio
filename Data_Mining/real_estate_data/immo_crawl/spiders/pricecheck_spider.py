import scrapy
import psycopg2
from datetime import date, timedelta
from immo_crawl.items import PriceCheckItem, PriceCheckLoader
from immo_crawl.credentials import HOSTNAME, USERNAME, PASSWORD, DATABASE


class PricecheckSpider(scrapy.Spider):
    """
    :class:`PricecheckSpider` looks up all current ads stored in the database and 
    checks whether there have been any changes to the price, which will be updated in the database.

    Attributes:
        name (str): name of the spider
        custom_settings (dict): special settings for this spider
    """
    name = 'pricecheck'
    custom_settings = {
        'ITEM_PIPELINES': {
            'immo_crawl.pipelines.PriceCheckValidation': 100,
            'immo_crawl.pipelines.ComparePrice': 150,
            'immo_crawl.pipelines.WritePrice': 200
        }}

    def start_requests(self):
        """
        Read the active URLs in the database for which requests are sent to the server. 
        The resulting responses are processed using :meth:`parse`.

        Yields:
            :class:`scrapy:scrapy.http.Request`: HTTP request that is processed with :meth:`parse`
        """
        check_date = date.today() - timedelta(days=7)
        conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        with conn.cursor() as cur:
            cur.execute(
                'SELECT url FROM eva_data WHERE date_last_seen > %s;', (check_date, ))
            pricecheck_urls = [url[0] for url in cur.fetchall()]
        conn.close()

        for url in pricecheck_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        """
        The prices are scraped from the ads and written to a :attr:`price_item` by a :attr:`loader`.

        Parameters:
            response (scrapy.http.Response): HTTP response to a sent request

        Yields:
            :class:`~immo_crawl.items.PriceCheckItem`: The item that gets processed in the pipeline
            (cf. :attr:`custom_settings`)

        Attributes:
            loader (:class:`~immo_crawl.items.PriceCheckLoader`):  object to collect data from HTML write to :attr:`price_item`
            price_item (:class:`~immo_crawl.items.PriceCheckItem`): container for storing the data to be written to the database
        """
        loader = PriceCheckLoader(item=PriceCheckItem(), response=response)
        loader.add_value('url', response.request.url.split('?')[0])
        loader.add_value('date', date.today())
        loader.add_xpath('price_chf', '//article[1]/div/h2/text()')

        price_item = loader.load_item()

        yield price_item
