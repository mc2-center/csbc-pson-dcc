FROM python:3.7.4-slim-buster

COPY pubmed_crawler.py .
COPY entrypoint.sh .

COPY requirements.txt .

RUN chmod a+x entrypoint.sh
RUN pip3 install -r requirements.txt

ENTRYPOINT ["./entrypoint.sh"]
