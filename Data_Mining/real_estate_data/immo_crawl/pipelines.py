import psycopg2
from scrapy.exceptions import DropItem
from datetime import datetime
from immo_crawl.credentials import HOSTNAME, USERNAME, PASSWORD, DATABASE


class WriteToDB(object):
    """
    Write the data from :class:`~immo_crawl.items.ImmoCrawlItem` to the database.
    """

    def open_spider(self, spider):
        """
        A connection to the database is established when the spider is started.
        """
        self.conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        """
        When the spider has finished its run, the database connection that was established 
        at the start (:meth:`open_spider`) is closed again.
        """
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        """
        All data from each :class:`~immo_crawl.items.ImmoCrawlItem` is written to 
        the main table of the database. The URL, the date and the price are also 
        written are also written to the price table.
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
    Default values are defined for the optional data fields if the values could 
    not be read from the HTML code.
    """

    def process_item(self, item, spider):
        item['area_m2'] = item.get('area_m2', None)
        item['floor'] = item.get('floor', None)
        item['utilities_chf'] = item.get('utilities_chf', None)
        item['date_available'] = item.get('date_available', None)
        return item


class CleanData(object):
    """
    The data is cleansed by converting the strings contained in  
    :class:`~immo_crawl.items.ImmoCrawlItem` into numerical values and date types.
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
    Checks whether the data fields of the :class:`~immo_crawl.items.ImmoCrawlItem`
    contain valid values.
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
    Checks whether the extracted data are numerical values.
    """

    def process_item(self, item, spider):
        try:
            item['price_chf'] = int(item['price_chf'])
        except (ValueError, KeyError):
            raise DropItem

        return item


class WritePrice(object):
    """
    The data of the :class:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider` 
    is written to the price table of the database.
    """

    def open_spider(self, spider):
        """
        A connection to the database is established when the 
        :class:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider` is started.
        """
        self.conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        """
        When the spider has finished its run, the database connection that was established 
        at the start (:meth:`open_spider`) is closed again.
        """
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        """
        The data of the :class:`~immo_crawl.items.PriceCheckItem` is written to 
        the price table of the database.
        """
        self.cur.execute('INSERT INTO eva_prices (url, date, price_chf) VALUES (%s, %s, %s);',
                         (item['url'], item['date'], item['price_chf']))
        self.conn.commit()
        return item


class ComparePrice(object):
    """
    The prices stored in the :class:`~immo_crawl.items.PriceCheckItem` are compared 
    with the latest price for the corresponding URL in the price table of the database 
    to decide whether a new entry should be created in the price table or the 
    :class:`~immo_crawl.items.PriceCheckItem` should be dropped.
    """

    def open_spider(self, spider):
        """
        A connection to the database is established when the 
        :class:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider` is started.
        """
        self.conn = psycopg2.connect(
            host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        """
        When the spider has finished its run, the database connection that was established 
        at the start (:meth:`open_spider`) is closed again.
        """
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        """
        The most recent price in the price table of the database is compared with 
        the price in the :class:`~immo_crawl.items.PriceCheckItem`.
        """
        self.cur.execute('SELECT price_chf FROM eva_prices WHERE url = %s ORDER BY date desc',
                         (item['url'], ))
        price = self.cur.fetchone()[0]
        if item['price_chf'] == price:
            raise DropItem
        else:
            return item
