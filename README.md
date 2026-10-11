# Bocagrande

Bocagrande is a small semantic ETL prototype. CSV files are mapped to OWL graphs using YAML schemas.
A Streamlit UI orchestrates the process and integrates Google Gemini to suggest transformations.
Consistency is checked with [OntoLogos](https://pypi.org/project/ontologos/) (a Rust reasoner, no Java needed) or, optionally, HermiT.

## Main components
- `adapter/` contains infrastructure such as CSV and YAML loaders, reasoner backends (OntoLogos, HermiT) and SHACL validation helpers.
- `ontology/` defines dataclasses representing the domain and services to build the OWL graphs.
- `ui/streamlit_app.py` offers a Streamlit interface to compare data against schemas, generate suggestions and run the semantic pipeline.
- `tests/` holds unit tests and Playwright-based end‑to‑end tests.
- `docker-compose.yml` builds the app container and a service to run the e2e tests.

## Prerequisites
 - Python 3.12 or later. Tested on 3.12, 3.13 and 3.14; 3.15 is experimental (see [Python versions](#python-versions)).
 - Java is only needed if you choose the optional HermiT reasoner.
- The dependencies listed in `requirements.txt` (use `requirements-dev.txt` to install lint and typing tools).
- A `.env` file with the environment variables shown in `.env.example`.

## Running the Streamlit app
```bash
# Install dependencies
pip install -r requirements.txt
pip install -e .

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

The reasoner is selectable with `--reasoner`:

| Value | Needs | Notes |
|---|---|---|
| `ontologos` (default) | nothing extra | Rust, OWL 2 RL/EL profiles. If it cannot fully reason about some axioms it warns that the result is incomplete. |
| `hermit` | Java 8+ | Full OWL 2 DL, uses `HermiT/HermiT.jar` (`--hermit` to change the path). |
| `none` | - | Skip reasoning (same as `--skip-reasoner`). |

In the Streamlit app set `BOCAGRANDE_REASONER=hermit` to use HermiT. In Docker, build with `--build-arg INSTALL_JAVA=true` if you need it.

## LangChain-based ETL step generation
`bocagrande.langchain_agent` integrates [LangChain](https://github.com/langchain-ai/langchain) to produce `ETLStep` definitions from the CSV headers and the YAML schema. The default chain uses the Gemini model provided by `google-generativeai`.

Set the `GEMINI_API_KEY` environment variable to enable this feature. Required packages are listed under `requirements.txt` (`langchain-core`, `langchain-google-genai` and `google-generativeai`).

## Decision models (optional)

Bocagrande can use fast, typed "System One" decision models through OpenRouter
([Mercury Decide](https://openrouter.ai/inception/mercury-decide:free), Jev). They do not write text:
they answer narrow questions and return a calibrated probability, so the code decides what to trust.

Set `OPENROUTER_API_KEY` (and optionally `DECISION_MODEL`) to enable them. Today they are used to map
CSV columns to schema fields (`bocagrande/mapping.py`):

1. Columns whose name matches a field (ignoring case, accents and separators) are mapped by code, with no model call.
2. The remaining fields go to the decision model in **one request**. Only answers with confidence of at
   least 0.8 are used, and a column feeds at most one field.
3. Everything else is listed as pending, or handed to Gemini if `GEMINI_API_KEY` is also set.

Only column names and field descriptions are sent. Example values from your data are sent only if you pass
`send_samples=True`. Without `OPENROUTER_API_KEY` the Streamlit app behaves exactly as before.

Formulas written by a language model are checked by `bocagrande/formula_screen.py` before `DataFrame.eval`
runs them: only arithmetic, comparisons, a few math functions and `.str`/`.dt` accessors are accepted. It is a
screen, not a sandbox.

## SHACL validation and triage

`bocagrande/validation.py` checks the generated graph against SHACL shapes built from the YAML schema
(datatype, required, maximum length, numeric precision, ISO date patterns). `validate_dataframe_report`
returns each violation as data and classifies the groups of violations by likely cause
(`bocagrande/shacl_triage.py`):

| Cause | Meaning | Typical action |
|---|---|---|
| `data_error` | a few rows hold bad values | fix or remove those rows |
| `schema_error` | the constraint is wrong or too strict | review the schema |
| `mapping_error` | the wrong column, or none, feeds the field | check the mapping |
| `needs_review` | not enough evidence | look at it manually |

Violations are grouped by field and constraint, so thousands of rows failing the same way are one decision.
Code settles the clear cases (up to 2% of rows failing is a data error; a required field empty in every row is
a mapping error). The rest go to the decision model in one request and only answers with confidence of at
least 0.8 are used; without a model they are reported as `needs_review`. Example values are sent only with
`send_samples=True`.

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

## Python versions

Python 3.12, 3.13 and 3.14 are supported. Python 3.15 runs the test suite, but installing the
dependencies needs a workaround: Streamlit pins `pyarrow<26` while `pyarrow` 26 is the first
release with 3.15 wheels. The CI runs 3.15 as an experimental job using `uv pip install --override`.
Once Streamlit relaxes that pin, 3.15 becomes a normal target. Set the Docker version with
`--build-arg PYTHON_VERSION=3.14`.

## License

MIT, see [LICENSE](LICENSE). `HermiT/HermiT.jar` is a third-party component with its own terms, see [NOTICE](NOTICE).
