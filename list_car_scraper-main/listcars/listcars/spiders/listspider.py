import scrapy


class ListspiderSpider(scrapy.Spider):
    name = "listspider"
    # allowed_domains = ["www.list.am"]
    start_urls = ["https://www.list.am/category/23"]


    def parse(self, response):
        cars = response.css('div.dl div.gl a')

        for car in cars:
            yield {
                'name': car.css('div.l ::text').get(),
                'price': car.css('div.p ::text').get(),
                'address': car.css('div.at ::text').get(),
                'url': 'list.am' + car.attrib['href']
            }



        next_page = response.xpath('//a[contains(text(), "Հաջ")]').attrib['href']
        if next_page is not None:
            next_url = 'https://list.am' + next_page

        yield response.follow(next_url, callback=self.parse)
