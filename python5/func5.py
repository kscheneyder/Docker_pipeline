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
                    -- Table `bronzedb`.`tb_order_payments`
                    -- -----------------------------------------------------
                    CREATE TABLE IF NOT EXISTS `bronzedb`.`tb_order_payments` (
                    `order_id` VARCHAR(100) NOT NULL,
                    `payment_sequential` VARCHAR(100) NULL,
                    `payment_type` VARCHAR(100) NULL,
                    `payment_installments` VARCHAR(45) NULL,
                    `payment_value` DECIMAL(10,2) NULL,
                    PRIMARY KEY (`order_id`))
                    ENGINE = InnoDB;
                    """
result = conn.execute(sqlalchemy.text(query_create_tb))


dir = './'
arq5 = 'olist_order_payments_dataset.csv'
payments = os.path.join(dir,arq5) 
tb_order_payments = pd.read_csv(payments, sep =',', encoding = 'latin-1')

for value in tb_order_payments.itertuples():
    order_id = value.order_id
    payment_sequential = value.payment_sequential
    payment_type = value.payment_type
    payment_installments = value.payment_installments
    payment_value = value.payment_value
    try:
        query_select = f"""SELECT COUNT(*) FROM tb_order_payments WHERE order_id = '{order_id}' """
        result = conn.execute(sqlalchemy.text(query_select)).scalar()
        if result == 0:
            query_insert = f"""INSERT INTO tb_order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
                                VALUES ('{order_id}', '{payment_sequential}', '{payment_type}', '{payment_installments}', '{payment_value}')"""
            print(f'Record inserted successfully: {value}')
            conn.execute(sqlalchemy.text(query_insert))
            conn.commit()
        else:
            print(f'Existing record in database: {value}')
    except Exception as e:
        print(f'Operation failled. Error: {e}')

print(f'Tabela {arq5} atualizada com sucesso!')