FROM python:3.8


WORKDIR /app
COPY requirements.txt .

RUN python3 -m pip install -r requirements.txt

USER 1000

COPY . .

EXPOSE 5000

CMD ["python", "main.py"]
