FROM python:3.11

RUN pip install pandas && pip install SQLAlchemy && pip install mysqlclient && pip install mysql-connector-python

WORKDIR /app

COPY func2.py ./
COPY olist_customers_dataset.csv ./