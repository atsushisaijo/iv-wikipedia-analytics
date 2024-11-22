# bash script to set up the venv
bash_script = """
python -m venv venv
source venv\Scripts\activate
pip install requests pandas duckdb
"""