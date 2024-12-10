FROM python:3.9

WORKDIR /app
COPY /.google /app/.google

COPY requirements.txt /app/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY consumer.py /app/
ADD start.sh /app/start.sh

CMD ["sh", "/app/start.sh"]