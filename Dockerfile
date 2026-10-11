# PYTHON_VERSION must be a tag published on Docker Hub (e.g. 3.12, 3.13, 3.14).
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim-bookworm

# Java is only needed for the optional HermiT reasoner (--reasoner hermit).
ARG INSTALL_JAVA=false

WORKDIR /app

# Instalar dependencias del sistema necesarias para Playwright
RUN apt-get update && apt-get install -y \
    build-essential \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    libfreetype6-dev \
    liblcms2-dev \
    libwebp-dev \
    tcl8.6-dev \
    tk8.6-dev \
    python3-tk \
    curl && \
    if [ "$INSTALL_JAVA" = "true" ]; then apt-get install -y openjdk-17-jre-headless; fi && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Instalar los navegadores de Playwright
RUN playwright install --with-deps

COPY . .

# Exponer el puerto de Streamlit
EXPOSE 8501

CMD ["streamlit", "run", "ui/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"] 