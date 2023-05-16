import scrapy
from datetime import datetime

class MlKeyboardsSpider(scrapy.Spider):
    name = "ml_keyboards_spider"
    start_urls = ["https://lista.mercadolivre.com.br/teclado-mecanico#D[A:teclado%20mecanico]"]

    def parse(self, response, **kwargs):
        for i in response.xpath('//li[@class="ui-search-layout__item"]'):
            price = "R" + ("".join(i.xpath('.//span[@class="price-tag-amount"]//text()').getall())).split("R")[1]
            price = price.replace('"', '')
            price = price.replace('.', '')
            price = price.replace(",", ".")
            title = i.xpath('.//h2/text()').get()
            title = title.replace('"', '')
            title = title.replace(",", " ")
            link = i.xpath('.//a/@href').get()
            current_dt = datetime.now().strftime("%d/%m/%Y %H:%M:%S").split(" ")
            yield {
                'title': title,
                'price': price,
                'date': current_dt[0],
                'time': current_dt[1],
                'link': link
            }
        next_page = response.xpath('//a[contains(@title, "Seguinte")]/@href').get()
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)
