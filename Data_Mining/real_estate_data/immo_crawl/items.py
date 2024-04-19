from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, Compose


def first_element(str_list):
    try:
        return str_list[0]
    except IndexError:
        return ''


def second_element(str_list):
    try:
        return str_list[1]
    except IndexError:
        return ''


def third_element(str_list):
    try:
        return str_list[2]
    except IndexError:
        return ''


def fourth_element(str_list):
    try:
        return str_list[3]
    except IndexError:
        return ''


def fifth_element(str_list):
    try:
        return str_list[4]
    except IndexError:
        return ''


def first_part(string):
    return string.split(' ')[0]


def second_part(string):
    try:
        return string.split(' ')[1]
    except IndexError:
        return ''


def remove_comma(string):
    return string.strip(', ')


def remove_chf(string):
    return string.strip('CHF .—–')


def remove_room(string):
    return string.strip(' Zimer')


def remove_m2(string):
    return string.strip(' m²')


def remove_apostrophe(string):
    return string.replace("’", "")


def replace_point(string):
    return string.replace('.', ',')


class ImmoCrawlItem(Item):
    """
    A container in which the data scraped from the advertisements is written. 
    The defined fields correspond to the data fields of the database into which 
    the collected data is written.

    The data from the different fields is processed in :attr:`~immo_crawl.settings.ITEM_PIPELINES`
    """
    address = Field()
    zip_code = Field()
    city = Field()
    canton = Field()
    rooms = Field()
    area_m2 = Field()
    price_chf = Field()
    date_scraped = Field()
    date_available = Field()
    date_last_seen = Field()
    floor = Field()
    utilities_chf = Field()
    url = Field()


class ImmoScoutLoader(ItemLoader):
    """
    Preprocesses the scraped data from www.immoscout24.ch and writes it to 
    :class:`ImmoCrawlItem`.
    """
    default_output_processor = TakeFirst()

    address_in = Compose(first_element)
    zip_code_in = Compose(second_element)
    city_in = Compose(fourth_element)
    canton_in = Compose(fifth_element, remove_comma)
    rooms_in = Compose(first_element, remove_room)
    area_m2_in = Compose(third_element, remove_m2)
    price_chf_in = Compose(first_element, remove_chf)
    date_available_in = Compose(second_element)
    floor_in = Compose(second_element)
    utilities_chf_in = Compose(second_element, remove_chf)


class HomeGateLoader(ItemLoader):
    """
    Preprocesses the scraped data from www.homegate.ch and writes it to 
    :class:`ImmoCrawlItem`.
    """
    default_output_processor = TakeFirst()

    address_in = Compose(first_element, remove_comma)
    zip_code_in = Compose(second_element, first_part)
    city_in = Compose(second_element, second_part)
    rooms_in = Compose(first_element, remove_comma, replace_point)
    area_m2_in = Compose(first_element, remove_m2)
    price_chf_in = Compose(second_element, remove_chf, remove_apostrophe)
    date_available_in = Compose(first_element)
    floor_in = Compose(first_element, remove_comma)
    utilities_chf_in = Compose(second_element, remove_chf)


class PriceCheckItem(Item):
    """
    A container for the data scraped by :class:`~immo_crawl.spider.PricecheckSpider`. 
    The defined fields correspond to the data fields of the database into which 
    the collected data is written.

    The data is processed in its own pipeline 
    (cf. :attr:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider.custom_settings`).
    """
    url = Field()
    date = Field()
    price_chf = Field()


class PriceCheckLoader(ItemLoader):
    """
    Preprocesses the data scraped by :class:`PricecheckSpider` and writes it to 
    :class:`PriceCheckItem`.
    """
    default_output_processor = TakeFirst()
    price_chf_in = Compose(first_element, remove_chf)
