import scrapy
import psycopg2
from datetime import date
from immo_crawl.items import ImmoCrawlItem, ImmoScoutLoader
from immo_crawl.credentials import HOSTNAME, USERNAME, PASSWORD, DATABASE


class ImmoscoutSpider(scrapy.Spider):
    """
    Die :class:`ImmoscoutSpider` crawlt die Inserate aller Immobilien auf https://www.immoscout24.ch,
    die zur Miete angeboten werden und speichert die Daten, die gescraped wurden, in ein
    :class:`~immo_crawl.items.ImmoCrawlItem`.

    Attributes:
        name (str): Name der Spider, wie er zum Start eines Durchlaufs verwendet wird.
        allowed_domains (list): Liste der zugelassenen Domains, auf der die Spider scrapen darf.
    """
    name = 'immoscout'
    allowed_domains = ['immoscout24.ch']

    def start_requests(self):
        """
        In dieser Methode werden Anfragen für die definierten Start-URLs an den
        Webserver gesendet.

        Da diese Methode zu Beginn eines Durchlaufs ausgeführt wird um Daten zu sammeln, wird
        an dieser Stelle eine Liste der URLs erstellt, für die bereits in der Datenbank ein
        Eintrag vorhanden ist.

        Yields:
            :class:`scrapy:scrapy.http.Request`: HTTP-Anfrage, die mit der :meth:`parse` Methode bearbeitet wird.

        Attributes:
            start_urls (list): Liste mit der Start-URL für jeden Kanton.

                Da der nächste-Seite-Link der Ergebnisseiten nicht ausgelesen werden kann,
                wird die Seitenzahl in der Start-URL ausgelassen und erst beim Erstellen der Anfrage
                eingefügt.
        """
        # Eine Liste der URLs erstellen, die bereits besucht wurden.
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
        # Anfragen für die Start-URLs senden
        page_nr = 1
        for start_url in start_urls:
            yield scrapy.Request(start_url.format(page_nr), callback=self.parse, cb_kwargs={'start_url': start_url})

    def parse(self, response, start_url=None):
        """
        In dieser Methode werden die Ergebnisseiten der HTTP-Antworten verarbeitet, die mit der
        :meth:`start_requests` Methode oder der :meth:`parse` Methode angefragt wurden.

        Gesendete Anfragen für Links auf Inserate werden mit der :meth:`parse_item` Methode verarbeitet.

        Falls die HTTP-Antwort auf eine Anfrage aus den :attr:`start_urls` folgt, müssen die URLs der
        weiteren Ergebnisseiten noch erstellt werden.

        Um die Serverlast durch Anfragen gering zu halten, werden nur
        für diejenigen Links auf Inserate neue HTTP-Anfragen gesendet, die noch nicht in der Datenbank
        vorhanden sind. Bei bereits vorhandenen URLs wird jeweils das aktuelle Datum in der Datenbank ergänzt.

        Parameters:
            response (scrapy.http.Response): Die HTTP-Antwort einer gesendeten Anfrage.
            start_url (str): Die Anfrage-URL, falls sie von den :attr:`start_urls` stammt, standardmässig ``None``.

        Yields:
            :class:`scrapy:scrapy.http.Request`: HTTP-Anfragen, die entweder mit der :meth:`parse` Methode
            oder der :meth:`parse_item` Methode verarbeitet werden.
        """
        # Verarbeiten der Ergebnisseiten
        for item_link in response.css('article a'):
            url = response.urljoin(item_link.css('::attr(href)').get())
            url = url.split('?')[0]
            # Falls sich die URL bereits in der Datenbank befindet, wird das Datum hinzugefügt (date_last_seen)
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

        # Falls die Response von einer Start-URL stammt, müssen die weiteren Links noch erstellt werden.
        if start_url:
            last_page = response.xpath(
                '//span[text()="Vorwärts"]/../../preceding-sibling::div[1]/*[last()]/text()').get()
            if last_page:
                for page_num in range(int(last_page) - 1):
                    yield scrapy.Request(start_url.format(page_num + 2), callback=self.parse)

    def parse_item(self, response):
        """
        In dieser Methode werden die HTTP-Antworten der Inserateseiten verarbeitet, die durch
        Anfragen der :meth:`parse` Methode erhalten wurden.

        Die gesammelten Daten werden durch ein Loader-Objekt in ein Item geschrieben.

        Parameters:
            response (scrapy.http.Response): Die HTTP-Antwort einer gesendeten Anfrage.

        Yields:
            :class:`~immo_crawl.items.ImmoCrawlItem`: Das Item, das in der Pipeline
            (siehe :attr:`~immo_crawl.settings.ITEM_PIPELINES`) weiter verarbeitet wird.

        Attributes:
            loader (:class:`~immo_crawl.items.ImmoScoutLoader`): Objekt, das die Daten aus dem HTML-Code
                ausliest und in das :attr:`immo_item` schreibt.
            immo_item (:class:`~immo_crawl.items.ImmoCrawlItem`): Container, der die Daten enthält, die
                in die Datenbank geschrieben werden sollen.
        """
        # Auslesen der Daten aus den einzelnen Trefferseiten
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
