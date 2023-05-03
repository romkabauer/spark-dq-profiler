## Snowflake Profiler

Utility for profiling tables of the Snowflake database and suggest constraints for columns worth adding in DQ rules

### Get started

#### Prerequisites

- Python 3.10 or higher

#### Setup utility

- Clone repository
- In project folder sequentially run 
  - `python3 -m venv venv`
  - `source venv/bin/activate`
  - `pip install -r requirements.txt`
- Adjust `config.py` with the tables you want to profile
- Add `snf_config.py` file with Snowflake credentials, you can find example in `snf_config_example.py`
- Run `main.py`
