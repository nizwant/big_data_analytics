FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DATA_DIR=/data
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gh_poller ./gh_poller
COPY scripts ./scripts

# uid 1000, like Kafka and Hadoop: on stage/prod chown /srv/bigdata/poller to 1000:1000.
RUN useradd --uid 1000 --create-home poller && mkdir -p /data && chown poller:poller /data
USER poller
VOLUME /data

CMD ["python", "-m", "gh_poller"]
