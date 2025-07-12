FROM python:3.9-slim

# Instala dependencias del sistema necesarias para pdfplumber y poppler
RUN apt-get update && apt-get install -y \
    build-essential \
    libpoppler-cpp-dev \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia requirements y código
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

CMD ["bash"] 