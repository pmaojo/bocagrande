# Bocagrande

Bocagrande is a small semantic ETL prototype. CSV files are mapped to OWL graphs using YAML schemas.
A Streamlit UI orchestrates the process and integrates Google Gemini to suggest transformations.

## Main components
- `adapter/` contains infrastructure such as CSV and YAML loaders, a wrapper for the HermiT reasoner and SHACL validation helpers.
- `ontology/` defines dataclasses representing the domain and services to build the OWL graphs.
- `ui/streamlit_app.py` offers a Streamlit interface to compare data against schemas, generate suggestions and run the semantic pipeline.
- `tests/` holds unit tests and Playwright-based end‑to‑end tests.
- `docker-compose.yml` builds the app container and a service to run the e2e tests.

## Prerequisites
- Python 3.9 or later.
- The dependencies listed in `requirements.txt` (use `requirements-dev.txt` to install lint and typing tools).
- A `.env` file with the environment variables shown in `.env.example`.

## Running the Streamlit app
```bash
# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# edit .env to add your GEMINI_API_KEY

# Start the UI
streamlit run ui/streamlit_app.py
```
Alternatively you can use Docker Compose:
```bash
docker compose up --build
```
The app will be available on http://localhost:8501.

## Command-line interface
Run the ETL pipeline from the shell:
```bash
bocagrande --schema schema_yaml/CLIENTES.yaml --csv raw/CLIENTES_sample.csv --output clientes.ttl
```
You can also use `python -m bocagrande` with the same arguments.

## Testing workflow
Linting and typing checks are performed with `flake8` and `mypy` respectively. Unit and integration tests run with `pytest`.
End‑to‑end tests live under `tests/e2e` and drive the Streamlit UI with Playwright. They can be executed through Docker Compose:
```bash
docker compose run e2e-tests
```
The default `pytest.ini` excludes e2e tests unless explicitly selected:
```bash
pytest              # runs unit tests only
pytest -m e2e       # runs end-to-end tests
```

## Auto-fix loop
`tools/auto_fix.py` can run `pytest` in a loop and use a language model to
suggest patches. A temporary virtual environment `.autoenv` keeps dependencies
isolated and each iteration is logged under `artifacts/`.

Run it locally:
```bash
python tools/auto_fix.py --max-iter 3
```

In GitHub Actions simply invoke the script in a step:
```yaml
- name: Auto fix
  run: python tools/auto_fix.py --max-iter 3
```

## Configuration
Use `.env.example` as a template for your own `.env` file. Currently only `GEMINI_API_KEY` is required.
