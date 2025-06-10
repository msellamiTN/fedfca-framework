FROM python:3.9

WORKDIR /app

# Copier les fichiers de dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier les fichiers Python
COPY *.py .
    
CMD ["python", "agmactor_new.py"]
