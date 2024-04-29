FROM python:3.11

RUN pip install pandas && pip install SQLAlchemy && pip install mysqlclient && pip install mysql-connector-python

WORKDIR /app

COPY func7.py ./
COPY olist_sellers_dataset.csv ./