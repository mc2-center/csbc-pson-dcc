FROM python:3.7.4-slim-buster

RUN mkdir manifest_templates
COPY manifest_templates/* manifest_templates/.

COPY python/pubmed_crawler.py .
COPY python/generate_templates.py .
COPY python/entrypoint.sh .
COPY python/requirements.txt .

RUN chmod a+x entrypoint.sh
RUN pip3 install -r requirements.txt

ENTRYPOINT [ "./entrypoint.sh" ]