FROM jupyter/pyspark-notebook:latest

WORKDIR /app
ADD . .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
