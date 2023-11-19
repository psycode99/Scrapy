# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # strip all whitespaces from strings
        field_names = adapter.field_names()
        for field in field_names:
            if field != 'description':
                value = adapter.get(field)
                adapter[field] = value.strip()

        # switch category and product type to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        # price --> float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price in price_keys:
            value = adapter.get(price)
            value = value.replace('£', '')
            adapter[price] = float(value)

        # availability --> exact no. of books
        availability = adapter.get('availability')
        split_array = availability.split('(')

        if len(split_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])


        # reviews --> int
        reviews = adapter.get('num_reviews')
        adapter['num_reviews'] = int(reviews)

        # stars --> int
        stars_str = adapter.get('stars')
        split_star_array = stars_str.split(' ')
        stars_text_val = split_star_array[1].lower()
        if stars_text_val == 'zero':
            adapter['stars'] = 0
        elif stars_text_val == 'one':
            adapter['stars'] = 1
        elif stars_text_val == 'two':
            adapter['stars'] = 2
        elif stars_text_val == 'three':
            adapter['stars'] = 3
        elif stars_text_val == 'four':
            adapter['stars'] = 4
        elif stars_text_val == 'five':
            adapter['stars'] = 5

        return item


class SaveToMySQLPipeline:
    
    def __init__(self) -> None:
        self.conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            passwd='wordpress',
            database='books',
            port=3306
        )

        self.cur = self.conn.cursor()

        sql = """
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment,
            url VARCHAR(255),
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text,
            PRIMARY KEY (id)
        )
        """

        self.cur.execute(sql)

    
    def process_item(self, item, spider):
        sql = """
        insert into books (
        url,
        title,
        upc,
        product_type,
        price_excl_tax,
        price_incl_tax,
        tax,
        price,
        availability,
        num_reviews,
        stars,
        category,
        description
        ) values(
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
            );""", (
        item['url'],
        item['title'],
        item['upc'],
        item['product_type'],
        item['price_excl_tax'],
        item['price_incl_tax'],
        item['tax'],
        item['price'],
        item['availability'],
        item['num_reviews'],
        item['stars'],
        item['category'],
        item['description']
        )
        

        self.cur.execute(*sql)
        self.conn.commit()
        return item
    

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()