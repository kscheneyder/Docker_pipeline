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
                    -- Table `bronzedb`.`tb_geolocation`
                    -- -----------------------------------------------------
                    CREATE TABLE IF NOT EXISTS `bronzedb`.`tb_geolocation` (
                    `geolocation_zip_code` INT NOT NULL,
                    `latitude` VARCHAR(45) NULL,
                    `longitude` VARCHAR(45) NULL,
                    `city` VARCHAR(45) NULL,
                    `state` VARCHAR(45) NULL,
                    PRIMARY KEY (`geolocation_zip_code`))
                    ENGINE = InnoDB;
                    """
result = conn.execute(sqlalchemy.text(query_create_tb))


dir = './'
arq3 = 'olist_geolocation_dataset.csv'
geolocation = os.path.join(dir,arq3)
tb_geolocation = pd.read_csv(geolocation, sep = ',', encoding = 'utf-8')

for value in tb_geolocation.itertuples():
    geolocation_zip_code = value.geolocation_zip_code_prefix
    geolocation_lat = value.geolocation_lat
    geolocation_lng = value.geolocation_lng
    geolocation_city = value.geolocation_city
    geolocation_state = value.geolocation_state
    try:
        query_select = f"""SELECT COUNT(*) FROM tb_geolocation WHERE geolocation_zip_code = '{geolocation_zip_code}' """
        result = conn.execute(sqlalchemy.text(query_select)).scalar()
        if result == 0:
            query_insert = f"""INSERT INTO tb_geolocation (geolocation_zip_code,latitude, longitude, city, state)
                                VALUES ('{geolocation_zip_code}','{geolocation_lat}', '{geolocation_lng}', '{geolocation_city}', '{geolocation_state}')"""
            print(f'Record inserted successfully: {value}')
            conn.execute(sqlalchemy.text(query_insert))
            conn.commit()
        else:
            print(f'Existing record in database: {value}')
    except Exception as e:
        print(f'Operation failled. Error: {e}')

print(f'Tabela {arq3} atualizada com sucesso!')