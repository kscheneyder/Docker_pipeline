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
                    -- Table `bronzedb`.`tb_products`
                    -- -----------------------------------------------------
                    CREATE TABLE IF NOT EXISTS `bronzedb`.`tb_products` (
                    `product_id` VARCHAR(100) NOT NULL,
                    `product_category_name` VARCHAR(45) NULL,
                    `product_name_lenght` INT NULL,
                    `product_description_lenght` INT NULL,
                    `product_photos_qty` INT NULL,
                    `product_weight_g` INT NULL,
                    `product_length_cm` INT NULL,
                    `product_height_cm` INT NULL,
                    `product_width_cm` INT NULL,
                    PRIMARY KEY (`product_id`))
                    ENGINE = InnoDB;
                    """
result = conn.execute(sqlalchemy.text(query_create_tb))


dir = './'
arq6 = 'olist_products_dataset.csv'
products = os.path.join(dir,arq6)
tb_products = pd.read_csv(products, sep =',', encoding = 'latin-1')

for value in tb_products.itertuples():
    product_id = value.product_id
    product_category_name = value.product_category_name
    product_name_lenght = value.product_name_lenght
    product_description_lenght = value.product_description_lenght
    product_photos_qty = value.product_photos_qty
    product_weight_g = value.product_weight_g
    product_length_cm = value.product_length_cm
    product_height_cm = value.product_height_cm
    product_width_cm = value.product_width_cm
    try:
        query_select = f"""SELECT COUNT(*) FROM tb_products WHERE product_id = '{product_id}'"""
        result = conn.execute(sqlalchemy.text(query_select)).scalar()
        if result == 0:
            query_insert = f"""INSERT INTO tb_products (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
                                VALUES ('{product_id}', '{product_category_name}', '{product_name_lenght}', '{product_description_lenght}', '{product_photos_qty}', '{product_weight_g}', '{product_length_cm}', '{product_height_cm}', '{product_width_cm}')"""
            print(f'Record inserted successfully: {value}')
            conn.execute(sqlalchemy.text(query_insert))
            conn.commit()
        else:
            print(f'Existing record in database: {value}')
    except Exception as e:
        print(f'Operation failled. Error: {e}')

print(f'Tabela {arq6} atualizada com sucesso!')