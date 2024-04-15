import scrapy
import psycopg2
from datetime import date, timedelta
from immo_crawl.items import PriceCheckItem, PriceCheckLoader
from immo_crawl.credentials import HOSTNAME, USERNAME, PASSWORD, DATABASE


class PricecheckSpider(scrapy.Spider):
    """
    Die :class:`PricecheckSpider` besucht alle noch aktuellen Inserate, die in der Datenbank
    gespeichert sind und überprüft, ob es Änderungen beim Preis gegeben hat. Allfällige Änderungen
    werden in der Preis-Tabelle der Datenbank festgehalten.

    Attributes:
        name (str): Name der Spider, wie er zum Start eines Durchlaufs verwendet wird.
        custom_settings (dict):

            Da sich die Verarbeitungslogik der Daten in der :class:`PriceCheckSpider`
            von den anderen Spiders unterscheidet (siehe :attr:`~immo_crawl.settings.ITEM_PIPELINES`),
            müssen hier die benötigten Abschnitte der Pipeline definiert werden:

            #. :class:`~immo_crawl.pipelines.PriceCheckValidation`
            #. :class:`~immo_crawl.pipelines.ComparePrice`
            #. :class:`~immo_crawl.pipelines.WritePrice`
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
        In dieser Methode werden die noch aktiven URLs aus der Datenbank ausgelesen, für die
        HTTP-Anfragen an den Server geschickt werden. Die resultierenden Antworten werden mit der
        :meth:`parse` Methode verarbeitet.

        Yields:
            :class:`scrapy:scrapy.http.Request`: HTTP-Anfrage, die mit der :meth:`parse` Methode bearbeitet wird.
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
        In dieser Methode werden die Preise aus den Inserateseiten ausgelesen und mithilfe
        eines :attr:`loader` in ein :attr:`price_item` geschrieben.

        Parameters:
            response (scrapy.http.Response): Die HTTP-Antwort einer gesendeten Anfrage.

        Yields:
            :class:`~immo_crawl.items.PriceCheckItem`: Das Item, das in der Pipeline
            (siehe :attr:`custom_settings`) weiter verarbeitet wird.

        Attributes:
            loader (:class:`~immo_crawl.items.PriceCheckLoader`): Objekt, das die Daten aus dem HTML-Code
                ausliest und in das :attr:`price_item` schreibt.
            price_item (:class:`~immo_crawl.items.PriceCheckItem`): Container, der die Daten enthält, die
                in die Datenbank geschrieben werden sollen.
        """
        loader = PriceCheckLoader(item=PriceCheckItem(), response=response)
        loader.add_value('url', response.request.url.split('?')[0])
        loader.add_value('date', date.today())
        loader.add_xpath('price_chf', '//article[1]/div/h2/text()')

        price_item = loader.load_item()

        yield price_item
