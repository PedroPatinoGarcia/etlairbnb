FROM ubuntu:latest

COPY modules/ /modules/
COPY script.py /
WORKDIR /app

CMD ["python3", "script.py"]


