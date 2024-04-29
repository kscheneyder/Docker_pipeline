import os
import pandas as pd
import sqlalchemy

#env
user = 'scheneyder'
password = 'pulse1010'
host = 'mysql'
port = '3306'
database = 'bronzedb'


string_conexao = f'mysql://{user}:{password}@{host}/{database}'

try:    
    engine = sqlalchemy.create_engine(string_conexao, future=True)
    conn = engine.connect()
    print('DB connected!')
except Exception as e:
    print(f'Error connection to MySQL: {e}')


query_create_tb = """
                    -- -----------------------------------------------------
                    -- Table `bronzedb`.`tb_order_items`
                    -- -----------------------------------------------------
                    CREATE TABLE IF NOT EXISTS `bronzedb`.`tb_order_items` (
                    `order_id` VARCHAR(100) NOT NULL,
                    `order_item_id` VARCHAR(100) NULL,
                    `product_id` VARCHAR(100) NULL,
                    `seller_id` VARCHAR(100) NULL,
                    `shipping_limit_date` DATETIME NULL,
                    `price` DECIMAL(10,2) NULL,
                    `freight_value` DECIMAL(10,2) NULL,
                    PRIMARY KEY (`order_id`))
                    ENGINE = InnoDB;
                    """
result = conn.execute(sqlalchemy.text(query_create_tb))


dir = './'
arq4 = 'olist_order_items_dataset.csv'
order_items = os.path.join(dir,arq4)
tb_order_items = pd.read_csv(order_items, sep=',', encoding = 'latin-1')

for value in tb_order_items.itertuples():
    order_id = value.order_id
    order_item_id = value.order_item_id
    product_id = value.product_id
    seller_id = value.seller_id
    shipping_limit_date = value.shipping_limit_date
    price = value.price
    freight_value = value.freight_value
    try:
        query_select = f"""SELECT COUNT(*) FROM tb_order_items WHERE order_id = '{order_id}' """
        result = conn.execute(sqlalchemy.text(query_select)).scalar()
        if result == 0:
            query_insert = f"""INSERT INTO tb_order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
                                VALUES ('{order_id}', '{order_item_id}', '{product_id}', '{seller_id}', '{shipping_limit_date}', '{price}', '{freight_value}')"""
            print(f'Record inserted successfully: {value}')
            conn.execute(sqlalchemy.text(query_insert))
            conn.commit()
        else:
            print(f'Existing record in database: {value}')
    except Exception as e:
        print(f'Operation failled. Error: {e}')

print(f'Tabela {arq4} atualizada com sucesso!')