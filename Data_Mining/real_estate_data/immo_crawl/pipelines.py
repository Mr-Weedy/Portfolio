import psycopg2
from scrapy.exceptions import DropItem
from datetime import datetime
from immo_crawl.credentials import HOSTNAME, USERNAME, PASSWORD, DATABASE


class WriteToDB(object):
    """
    In diesem Abschnitt der Pipeline werden die Daten aus dem :class:`~immo_crawl.items.ImmoCrawlItem`
    in die Datenbank geschrieben.
    """

    def open_spider(self, spider):
        """
        Beim Start der Spider wird eine Verbindung zur Datenbank aufgebaut.
        """
        self.conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        """
        Wenn die Spider mit ihrem Durchgang fertig ist, wird die Datenbankverbindung, die beim
        Start aufgebaut wurde (:meth:`open_spider`), wieder geschlossen.
        """
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        """
        Von jedem :class:`~immo_crawl.items.ImmoCrawlItem` werden alle Daten in die
        Haupttabelle der Datenbank geschrieben. Die URL, das Datum und der Preis werden zudem
        in die Preis-Tabelle der Datenbank geschrieben.
        """
        self.cur.execute('INSERT INTO eva_data (address, zip, city, canton, price_chf, '
                         'rooms, area_m2, floor, utilities_chf, date_available, date_scraped, date_last_seen, url) '
                         'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                         (item['address'], item['zip_code'], item['city'], item['canton'],
                          item['price_chf'], item['rooms'], item['area_m2'], item['floor'],
                          item['utilities_chf'], item['date_available'], item['date_scraped'],
                          item['date_last_seen'], item['url']))
        self.cur.execute('INSERT INTO eva_prices (url, date, price_chf) VALUES (%s, %s, %s);',
                         (item['url'], item['date_scraped'], item['price_chf']))
        self.conn.commit()
        return item


class SetDefaultValues(object):
    """
    In diesem Abschnitt der Pipeline werden für die optionalen Datenfelder Standardwerte
    definiert, falls die Werte nicht aus dem HTML-Code ausgelesen werden konnte.
    """

    def process_item(self, item, spider):
        item['area_m2'] = item.get('area_m2', None)
        item['floor'] = item.get('floor', None)
        item['utilities_chf'] = item.get('utilities_chf', None)
        item['date_available'] = item.get('date_available', None)
        return item


class CleanData(object):
    """
    In diesem Teil der Pipeline werden die Daten bereinigt, indem die im :class:`~immo_crawl.items.ImmoCrawlItem`
    enthaltenen Strings in numerische Werte und Datumstypen umgewandelt werden.
    """

    def process_item(self, item, spider):
        try:
            item['price_chf'] = int(item['price_chf'])
        except (ValueError, KeyError):
            raise DropItem

        if item['area_m2'] == '':
            item['area_m2'] = None
        elif item['area_m2']:
            try:
                item['area_m2'] = int(item['area_m2'])
            except ValueError:
                item['area_m2'] = None

        if item['utilities_chf'] == '':
            item['utilities_chf'] = None
        elif item['utilities_chf']:
            try:
                item['utilities_chf'] = int(item['utilities_chf'])
            except ValueError:
                item['utilities_chf'] = None

        if item['floor'] == '':
            item['floor'] = None

        try:
            item['date_available'] = datetime.strptime(
                item['date_available'], '%d.%m.%Y')
        except (ValueError, TypeError, KeyError):
            item['date_available'] = None

        return item


class DataValidation(object):
    """
    In diesem Teil der Pipeline wird überprüft, ob die Datenfelder des :class:`~immo_crawl.items.ImmoCrawlItem`
    gültige Werte enthalten.
    """

    def process_item(self, item, spider):
        try:
            if not (item['zip_code'].isdecimal() and len(item['zip_code']) == 4):
                raise DropItem
        except KeyError:
            raise DropItem

        try:
            if item['rooms'] == '':
                raise DropItem
        except KeyError:
            raise DropItem

        return item


class PriceCheckValidation(object):
    """
    In diesem Teil der Pricecheck-Pipeline wird überprüft, ob es sich bei den extrahierten
    Daten um numerische Werte handelt.
    """

    def process_item(self, item, spider):
        try:
            item['price_chf'] = int(item['price_chf'])
        except (ValueError, KeyError):
            raise DropItem

        return item


class WritePrice(object):
    """
    In diesem Teil der Pricecheck-Pipeline werden die Daten der :class:`~immo_crawl.spiders.
    pricecheck_spider.PricecheckSpider` in die Preis-Tabelle der Datenbank geschrieben.
    """

    def open_spider(self, spider):
        """
        Beim Start der :class:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider`
        wird eine Verbindung zur Datenbank aufgebaut.
        """
        self.conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        """
        Wenn die :class:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider` mit
        ihrem Durchgang fertig ist, wird die Datenbankverbindung, die beim Start aufgebaut
        wurde (:meth:`open_spider`), wieder geschlossen.
        """
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        """
        Die Daten des :class:`~immo_crawl.items.PriceCheckItem` werden in die Preis-Tabelle
        der Datenbank geschrieben.
        """
        self.cur.execute('INSERT INTO eva_prices (url, date, price_chf) VALUES (%s, %s, %s);',
                         (item['url'], item['date'], item['price_chf']))
        self.conn.commit()
        return item


class ComparePrice(object):
    """
    In diesem Teil der Pricecheck-Pipeline werden die Preise, die im :class:`~immo_crawl.items.PriceCheckItem`
    gespeichert sind, mit dem neuesten Preis für die entsprechende URL in der Preis-Tabelle der Datenbank
    verglichen, um zu entscheiden, ob ein neuer Eintrag in die Preis-Tabelle erstellt oder das
    :class:`~immo_crawl.items.PriceCheckItem` verworfen wird.
    """

    def open_spider(self, spider):
        """
        Beim Start der :class:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider`
        wird eine Verbindung zur Datenbank aufgebaut.
        """
        self.conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        """
        Wenn die :class:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider` mit
        ihrem Durchgang fertig ist, wird die Datenbankverbindung, die beim Start aufgebaut
        wurde (:meth:`open_spider`), wieder geschlossen.
        """
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        """
        Der aktuellste Preis in der Preis-Tabelle der Datenbank wird mit dem Preis im
        :class:`~immo_crawl.items.PriceCheckItem` verglichen.
        """
        self.cur.execute('SELECT price_chf FROM eva_prices WHERE url = %s ORDER BY date desc',
                         (item['url'], ))
        price = self.cur.fetchone()[0]
        if item['price_chf'] == price:
            raise DropItem
        else:
            return item
