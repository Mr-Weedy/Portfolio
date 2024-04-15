# Scrapy settings for immo_crawl project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
"""
In den :mod:`~immo_crawl.settings` werden die Einstellungen des Crawlers festgehalten.
Die zentralsten Einstellungen sind:

Attributes:
    USER_AGENT (str): Name (und URL) mit dem sich der Crawler den Webservern gegenüber
        zu erkennen gibt.
    ROBOTSTXT_OBEY (bool): Einstellung, ob den Einschränkungen der robots.txt-Datei
        folge geleistet werden.
    ITEM_PIPELINES (dict): Definiert, welche :mod:`~immo_crawl.pipelines`-Abschnitte
        ein :class:`~immo_crawl.items.ImmoCrawlItem` in welcher Reihenfolge durchläuft:

        #. :class:`~immo_crawl.pipelines.SetDefaultValues`
        #. :class:`~immo_crawl.pipelines.CleanData`
        #. :class:`~immo_crawl.pipelines.DataValidation`
        #. :class:`~immo_crawl.pipelines.WriteToDB`
    AUTOTHROTTLE_ENABLED (bool): Einstellung, ob die HTTP-Anfragen verzögert werden,
        wenn der Webserver länger braucht um zu reagieren.
"""
BOT_NAME = 'immo_crawl'

SPIDER_MODULES = ['immo_crawl.spiders']
NEWSPIDER_MODULE = 'immo_crawl.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Projekt_EVA (https://www.fhgr.ch)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 1
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'immo_crawl.middlewares.ImmoCrawlSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'immo_crawl.middlewares.ImmoCrawlDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'immo_crawl.pipelines.WriteToDB': 800,
    'immo_crawl.pipelines.SetDefaultValues': 100,
    'immo_crawl.pipelines.CleanData': 300,
    'immo_crawl.pipelines.DataValidation': 500,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Encode feed export in unicode (including JSON)
FEED_EXPORT_ENCODING = 'utf-8'
