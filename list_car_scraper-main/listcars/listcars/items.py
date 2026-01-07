# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import MapCompose


class CarItem(scrapy.Item):
    make = scrapy.Field()
    model = scrapy.Field()
    price = scrapy.Field()
    body_type = scrapy.Field()
    year = scrapy.Field()
    engine_type = scrapy.Field()
    engine_size = scrapy.Field()
    transmission = scrapy.Field()
    drive_type = scrapy.Field()
    mileage = scrapy.Field()
    condition = scrapy.Field()
    steering_wheel = scrapy.Field()
    cleared_customs = scrapy.Field()
    color = scrapy.Field()
    wheel_size = scrapy.Field()
    interior_material = scrapy.Field()
    sunroof = scrapy.Field()
    comfort = scrapy.Field()
    url = scrapy.Field()
