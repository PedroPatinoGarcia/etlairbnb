FROM ubuntu:latest

COPY src/ /src/
COPY script.py /
WORKDIR /app

CMD ["python3", "script.py"]


