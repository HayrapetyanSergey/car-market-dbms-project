# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


# class ListcarsPipeline:
#     def process_item(self, item, spider):
#         return item
    

class PricePipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if not adapter.get('price'):
            raise DropItem('Missing price')
        if not adapter.get('price').startswith('$'):
            raise DropItem('Missing price')
    
        adapter['price'] = float(adapter['price'][1:].replace(',', ''))

        return item
    

class ComfortPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if not adapter.get('comfort'):
            adapter['comfort'] = 0
        else:
            adapter['comfort'] = len(adapter['comfort'].split(','))

        return item
    

class EngineSizePipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('engine_size'):
            adapter['engine_size'] = float(adapter['engine_size'][:-2])

        return item


class MileagePipeline:
    rate = 1.60934
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if not adapter.get('mileage'):
            raise DropItem()

        num, t = adapter.get('mileage').split()
        num = num.replace(',', '')
        if t == 'miles':
            adapter['mileage'] = int(int(num) * self.rate)
        else:
            adapter['mileage'] = int(num)

        return item
    

class ConditionPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if not adapter.get('condition'):
            raise DropItem()
    
        adapter['condition'] = adapter['condition'] == 'Car is not damaged'
        
        return item


class ConvertStringPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        adapter['wheel_size'] = int(adapter['wheel_size'][1:])
        adapter['year'] = int(adapter['year'])
        adapter['steering_wheel'] = adapter['steering_wheel'] == 'Left'
        adapter['cleared_customs'] = adapter['cleared_customs'] == 'Yes'

        return item