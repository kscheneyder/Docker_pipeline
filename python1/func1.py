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
                    -- Table `bronzedb`.`tb_orders`
                    -- -----------------------------------------------------
                    CREATE TABLE IF NOT EXISTS `bronzedb`.`tb_orders` (
                    `order_id` VARCHAR(100) NOT NULL,
                    `customer_id` VARCHAR(100) NULL,
                    `order_status` VARCHAR(45) NULL,
                    `order_purchase_timestamp` DATETIME NULL,
                    `order_approved_at` DATETIME NULL,
                    `order_delivered_carrier_date` DATETIME NULL,
                    `order_delivered_customer_date` DATETIME NULL,
                    `order_estimated_delivery_date` DATETIME NULL,
                    PRIMARY KEY (`order_id`))
                    ENGINE = InnoDB;
                    """

result = conn.execute(sqlalchemy.text(query_create_tb))

dir = './'
arq1 = 'olist_orders_dataset.csv'
orders = os.path.join(dir,arq1)
df_orders = pd.read_csv(orders, sep = ',', encoding = 'latin-1')

for value in df_orders.itertuples():
    order_id = value.order_id
    customer_id = value.customer_id
    order_status = value.order_status
    order_purchase_timestamp = value.order_purchase_timestamp
    order_approved_at = value.order_approved_at
    order_delivered_carrier_date = value.order_delivered_carrier_date
    order_estimated_delivery_date = value.order_estimated_delivery_date
    try:
        query_select = f"""SELECT COUNT(*) FROM tb_orders WHERE order_id= '{order_id}'"""
        result = conn.execute(sqlalchemy.text(query_select)).scalar()
        if result == 0:
            query_insert = f"""INSERT INTO tb_orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_estimated_delivery_date)
                                VALUES ('{order_id}','{customer_id}', '{order_status}', '{order_purchase_timestamp}', '{order_approved_at}', '{order_delivered_carrier_date}', '{order_estimated_delivery_date}')"""
            print(f'Record inserted successfully: {value}')
            conn.execute(sqlalchemy.text(query_insert))
            conn.commit()
        else:
            print(f'Existing record in the base: {value}')
    except Exception as e:
        print(f'Operation failled. Error: {e}')

print(f'Tabela {arq1} atualizada com sucesso!')

