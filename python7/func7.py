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
                    -- Table `bronzedb`.`tb_sellers`
                    -- -----------------------------------------------------
                    CREATE TABLE IF NOT EXISTS `bronzedb`.`tb_sellers` (
                    `seller_id` VARCHAR(100) NOT NULL,
                    `seller_zip_code_prefix` INT NULL,
                    `seller_city` VARCHAR(45) NULL,
                    `seller_state` VARCHAR(45) NULL,
                    PRIMARY KEY (`seller_id`))
                    ENGINE = InnoDB;
                    """
result = conn.execute(sqlalchemy.text(query_create_tb))


dir = './'
arq7 = 'olist_sellers_dataset.csv'
sellers = os.path.join(dir,arq7)
tb_sellers = pd.read_csv(sellers, sep = ',', encoding = 'latin-1')

for value in tb_sellers.itertuples():
    seller_id = value.seller_id
    seller_zip_code_prefix = value.seller_zip_code_prefix
    seller_city = value.seller_city
    seller_state = value.seller_state
    try:
        query_select = f"""SELECT COUNT(*) FROM tb_sellers WHERE seller_id = '{seller_id}'"""
        result = conn.execute(sqlalchemy.text(query_select)).scalar()
        if result == 0:
            query_insert = f"""INSERT INTO tb_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                                VALUES ('{seller_id}', '{seller_zip_code_prefix}', '{seller_city}', '{seller_state}')"""
            print(f'Record inserted successfully: {value}')
            conn.execute(sqlalchemy.text(query_insert))
            conn.commit()
        else:
            print(f'Existing record in database: {value}')
    except Exception as e:
        print(f'Operation failled. Error: {e}')

print(f'Tabela {arq7} atualizada com sucesso!')