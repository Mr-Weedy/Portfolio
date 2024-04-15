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
    Ein Container, in den die Daten geschrieben werden, die aus den Inseraten gescraped
    wurden. Die definierten Felder entsprechen den Datenfeldern der Datenbank, in welche
    die gesammelten Daten geschrieben werden.

    Die Daten der verschiedenen Feldern werden in der Pipeline (siehe :attr:`~immo_crawl.settings.ITEM_PIPELINES`)
    bearbeitet und in die Datenbank geschrieben.
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
    Der :class:`ImmoScoutLoader` befüllt den Container (:class:`ImmoCrawlItem`) mit den
    aus dem HTML-Code extrahierten Daten. Dabei ermöglicht er eine Vorverarbeitung der
    Daten, damit diese im gewünschten Format vorhanden sind, um sie in der Pipeline (siehe
    :attr:`~immo_crawl.settings.ITEM_PIPELINES`) weiter zu verarbeiten.

    Da die Daten auf verschiednen Plattformen in unterschiedlichen Formaten vorliegen,
    benötigt jede Website einen eigenen :class:`~scrapy:scrapy.loader.ItemLoader`.
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
    Der :class:`HomeGateLoader` befüllt den Container (:class:`ImmoCrawlItem`) mit den
    aus dem HTML-Code extrahierten Daten. Dabei ermöglicht er eine Vorverarbeitung der
    Daten, damit diese im gewünschten Format vorhanden sind, um sie in der Pipeline (siehe
    :attr:`~immo_crawl.settings.ITEM_PIPELINES`) weiter zu verarbeiten.

    Da die Daten auf verschiednen Plattformen in unterschiedlichen Formaten vorliegen,
    benötigt jede Website einen eigenen :class:`~scrapy:scrapy.loader.ItemLoader`.
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
    Ein Container, in den die :class:`~immo_crawl.spider.PricecheckSpider` die relevanten
    Daten schreibt. Die definierten Felder entsprechen den Datenfeldern der Preis-Tabelle in der
    Datenbank.

    Die werden in von der :class:`~immo_crawl.spider.PricecheckSpider` in einer eigenen
    Pipeline (siehe :attr:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider.custom_settings`)
    in die Datenbank geschrieben.
    """
    url = Field()
    date = Field()
    price_chf = Field()


class PriceCheckLoader(ItemLoader):
    """
    Der :class:`PriceCheckLoader` befüllt den Container (:class:`PriceCheckItem`) mit den
    aus dem HTML-Code extrahierten Daten. Dabei ermöglicht er eine Vorverarbeitung der
    Daten, damit diese im gewünschten Format vorhanden sind, um sie in der Pipeline (siehe
    :attr:`~immo_crawl.spiders.pricecheck_spider.PricecheckSpider.custom_settings`) weiter zu verarbeiten.
    """
    default_output_processor = TakeFirst()
    price_chf_in = Compose(first_element, remove_chf)
