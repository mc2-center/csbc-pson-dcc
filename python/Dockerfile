FROM python:3.7.4-slim-buster

COPY pubmed_crawler.py .

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "pubmed_crawler.py", "-t", "syn21868591"]
