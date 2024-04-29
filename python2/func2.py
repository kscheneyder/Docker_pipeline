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
                    -- Table `bronzedb`.`customers_data`
                    -- -----------------------------------------------------
                    CREATE TABLE IF NOT EXISTS `bronzedb`.`customers_data` (
                    `customer_id` VARCHAR(100) NOT NULL,
                    `customer_unique_id` VARCHAR(100) NULL,
                    `customer_zip_code_prefix` INT NULL,
                    `customer_city` VARCHAR(45) NULL,
                    `customer_state` VARCHAR(45) NULL,
                    PRIMARY KEY (`customer_id`))
                    ENGINE = InnoDB;
                    """

result = conn.execute(sqlalchemy.text(query_create_tb))

dir = './'
arq2 = 'olist_customers_dataset.csv'
customers = os.path.join(dir,arq2)
customers_data = pd.read_csv(customers, sep = ',', encoding = 'latin-1')

for value in customers_data.itertuples():
    customer_id = value.customer_id
    customer_unique_id = value.customer_unique_id
    customer_zip_code_prefix = value.customer_zip_code_prefix
    customer_city = value.customer_city
    customer_state = value.customer_state
    try:
        query_select = f"""SELECT COUNT(*) FROM customers_data WHERE customer_id = '{customer_id}' """
        result = conn.execute(sqlalchemy.text(query_select)).scalar()
        if result == 0:
            query_insert = f"""INSERT INTO customers_data (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                                VALUES  ('{customer_id}', '{customer_unique_id}', '{customer_zip_code_prefix}', '{customer_city}', '{customer_state}')"""
            print(f'Record inserted successfully: {value}')
            conn.execute(sqlalchemy.text(query_insert))
            conn.commit()
        else:
            print(f'Existing record in database: {value}')
    except Exception as e:
        print(f'Operation failled. Error: {e}')

print(f'Tabela {arq2} atualizada com sucesso!')