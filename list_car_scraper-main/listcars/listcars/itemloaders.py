from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose


# class CarItemLoader(ItemLoader):
#     default_output_processor = TakeFirst()
#     price_in = None
#     body_type_in = None
#     year_in = None
#     engine_type_in = None
#     engine_size_in = None
#     transmission_in = None
#     drive_type_in = None
#     mileage_in = None
#     condition_in = None
#     steering_wheel_in = None
#     cleared_customs_in = None
#     color_in = None
#     wheel_size_in = None
#     interior_material_in = None
#     sunroof_in = None
#     comfort_in = None