# Wikipedia Analytics

Extract Wikipedia data into DuckDB and analyze it using dbt.

## Features
- Extract featured articles from Wikipedia API
- Store data in DuckDB for efficient querying
- Transform and analyze data using dbt
- Comprehensive test suite
- CI/CD pipeline using GitHub Actions

## artefacts
- dbt is initialized as: dbt init iv_wiki_analytics

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/yourusername/wikipedia-analytics.git
cd wikipedia-analytics
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Run the ETL pipeline:
```bash
poetry run python -m iv_wikipedia_analytics.setup
```

4. Initialize dbt:
```bash
cd dbt_wikipedia
dbt init
```

5. Run dbt models:
```bash
dbt run
```

## Development Setup

1. Install pre-commit hooks:
```bash
pre-commit install
```

2. Run tests:
```bash
make test
```

3. Format code:
```bash
make lint
```

## Project Structure

- `src/wikipedia_analytics/`: Main source code
- `dbt_wikipedia/`: DBT models and transformations
- `tests/`: Test suite
- `notebooks/`: Jupyter notebooks for exploration


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.