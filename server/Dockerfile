FROM python:3.8

WORKDIR /app

COPY requirements.txt config.yml .
RUN pip install --no-cache-dir -r requirements.txt

COPY logactor.py agmactor.py .

CMD ["python", "agmactor.py"]
