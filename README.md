## Snowflake Profiler

Utility for profiling tables of the Snowflake database and suggest constraints for columns worth adding in DQ rules

### Get started

#### Prerequisites

- Python 3.10 or higher
- Docker Engine

#### Setup utility

- Clone repository
- In project folder sequentially run 
  - `docker build -t dano-uki-profiler-img:0.0.1 .`
  - This operation above can take a lng time to complete because base image is quite big
  - `docker run --name container_name -p 4040:4040 -p 4041:4041 -p 8888:8888 -v path/to/local/project/dir:/app dano-uki-profiler-img:0.0.1`
- You will be given with localhost URL with port token where Jupyter Lab is running
- Copy it from terminal and paste into browser tab
- Locally in IDE/in opened Jupyter Lab:
  - Adjust `config.py` with the tables you want to profile and constraints you want to check
  - Add `snf_config.py` file with Snowflake credentials (or comment initialization of SNFProfiler in `main.py`), you can find example in `snf_config_example.py`
- Open terminal within Jupyter Lab and run `python main.py`
