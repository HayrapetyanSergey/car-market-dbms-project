import scrapy
import random
# from listcars.itemloaders import CarItemLoader
from listcars.items import CarItem

class Listspider2Spider(scrapy.Spider):
    name = "listspider2"
    allowed_domains = ["list.am"]
    start_urls = ["https://list.am/en/category/23"]

    car_attributes = [
        'Make',
        'Model',
        'Body Type',
        'Year',
        'Engine Type',
        'Engine Size',
        'Transmission',
        'Drive Type',
        'Mileage',
        'Condition',
        'Steering Wheel',
        'Cleared Customs',
        'Color',
        'Wheel Size',
        'Interior Material',
        'Sunroof',
        'Comfort'
    ]

    @staticmethod
    def to_name(name):
        return name.replace('_', ' ').title()

    # user_agent_list = [
    #     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    #     'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
    #     'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    #     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75',
    #     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18363',
    # ]

    def parse(self, response):
        cars = response.css('div.dl div.gl a')

        for car in cars:
            car_url = 'https://list.am' + car.attrib['href']

            yield response.follow(
                car_url,
                callback=self.parse_car,
                # headers={'User-Agent':random.choice(self.user_agent_list)}
            )

        next_page = response.xpath('//a[contains(text(), "Next")]').attrib['href']
        if next_page is not None:
            next_url = 'https://list.am' + next_page

        yield response.follow(next_url, callback=self.parse)

    def parse_car(self, response):
        # loader = CarItemLoader()
        car_table = {}

        for attribute in self.car_attributes:
            # print(attribute, " - ", response.xpath(f'//div[contains(text(), "{attribute}")]/following-sibling::div/text()').get())
            car_table[attribute] = response.xpath(f'//div[contains(text(), "{attribute}")]/following-sibling::div/text()').get()
        car_table['url'] = response.url

        item = CarItem()
        for key in item.fields.keys():
            if key == 'url':
                item['url'] = car_table.get('url', '')
            elif key == 'price':
                item['price'] = response.xpath('//span[@class="xprice" and @id="xprice"]/span/text()').get()
            else:
                item[key] = car_table.get(self.to_name(key), '')

        yield item
