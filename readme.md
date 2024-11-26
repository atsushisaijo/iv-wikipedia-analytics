# Wikipedia API/dbt Code for Technical Interview

Extract Wikipedia data via API into DuckDB and analyze it using dbt.

## Features
- Extract information of edited articles from Wikipedia API on specific date - 24hours
- Store data in the local DuckDB 
- Apply transformation using dbt

## Deliverable
```sql
https://github.com/atsushisaijo/iv-wikipedia-analytics.git
```
- Jupyter notebook for each code component - testable
- a python file, executable
- logging to monitor the actual process - useful to monitor api-get call
- dbt transformation logic
- unit tests - runnable via IDE (PyCharm), but also in Notebook

## Environment
- Development was performed using venv on Windows11, with Python 3.13 (latest stable release, Oct-2024)
  - It is anticipated to run in Python 3.11/12.
- dbt version is 1.8.9 (latest)
- Dependencies are exported to requirements.txt
- The developer avoided using Poetry - as it tries to manage all virtual environments + more configurations
  - Setting up in Windows OS can be performed via command prompt (example - manual install): 
```bash
python -m venv .venv
source .venv\Scripts\activate
pip install -r requirements.txt 
```
## API 
- URL for api call
`https://en.wikipedia.org/w/api.php`
- API detail, specified as list=recentchanges (rc)
`https://en.wikipedia.org/w/api.php?action=help&modules=query%2Brecentchanges`
- full details of API is: https://en.wikipedia.org/w/api.php
- the target API is described in the subsection of query > list: recentchanges

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/atsushisaijo/iv-wikipedia-analytics.git
cd iv-wikipedia-analytics
```

2. Install dependencies - here example is manual install
```bash
python -m venv .venv
source .venv\Scripts\activate
pip install -r requirements.txt 
```

3. Run the ETL pipeline:
- IDE (PyCharm) has been used to run the code, using .venv code. 

4. dbt:
- connection to dbt after python process `dbt_wiki_analytics>dbt debug`
- dbt build `dbt_wiki_analytics>dbt build`
- run dbt `dbt_wiki_analytics>dbt run`
- dbt tests are only supplied with generic tests
   

5. Any investigation/queries can be performed using Jupyter Notebook.
- Connecting to DuckDB at `data/wikipedia.db`
- Note that notebook should be deployed on ./notebook/ folder 
```python
import os
import duckdb
db_path = os.path.join("..", "data", "wikipedia.db") 
conn = duckdb.connect(db_path)
```
- close connection to DuckDB
```python
conn.close()
```

## unit/pipeline tests
- PyCharm has been configured for testing folders
- unit test succeeds, however; coverage would be insufficient - this is todo item
- pipeline test should be supplied - this is todo item
- However without it, it may require such code to add absolute paths: 
```bash
# to import src code on batch mode
 sys.path.append(str(Path(__file__).parent.parent))

# on Jupyter
 current_dir = Path.cwd()
 parent_dir = current_dir.parent
 sys.path.append(str(parent_dir))
```


## Project Structure
- `src\python\`: Main source code
- `tests\`: unit testing code
- `dbt_wiki_analytics\`: dbt models and transformations
- `data\`: DuckDB data
- `notebooks\`: notebooks for EDA and checks
- `logs\`: containing python execution log and dbt log. Each run in Python process will delete the log.


## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
