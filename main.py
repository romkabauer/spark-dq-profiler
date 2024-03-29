import itertools
from asyncio import run, gather
from numpyencoder import NumpyEncoder
import json
import time
import os

from config.config import TO_PROFILE, FLAG_PRINT_PROFILING_STAT, \
    FLAG_SUGGEST_MERGE_STATEMENT_FOR_ADF_FRAMEWORK, CONSTRAINT_IDENTIFICATION_RULES, \
    CSV_SEPARATOR, WRITE_TO_FILE, ANALYSIS_OUTPUT_FILE_PATH, PROFILING_OUTPUT_FILE_PATH
from utils.profilers import Profiler, SNFProfiler, SparkProfiler
from utils.analyzer import Analyzer
from helpers.results_printing import print_results


async def gather_profiling_results(*profilers: Profiler) -> list:
    results_by_profiler = await gather(*[profiler.get_tables_descriptions()
                                         for profiler in profilers
                                         if profiler.table_config])

    all_configured_tables_descriptions = []
    for results in results_by_profiler:
        for table_description in results:
            all_configured_tables_descriptions.append(table_description)

    return all_configured_tables_descriptions


if __name__ == '__main__':
    ts = time.time()
    for old_out_files in os.listdir(os.curdir):
        if old_out_files == f'{ANALYSIS_OUTPUT_FILE_PATH}' or \
           old_out_files == f'{PROFILING_OUTPUT_FILE_PATH}':
            os.remove(old_out_files)

    available_profilers = {
        "SNF": SNFProfiler([]),
        "SPARK": SparkProfiler([], csv_separator=CSV_SEPARATOR)
    }

    for datasource_type, tables in itertools.groupby(TO_PROFILE, lambda x: x.get("datasource_type")
                                                                           if x.get("datasource_type")
                                                                           else "SPARK"):
        if available_profilers.get(datasource_type):
            available_profilers[datasource_type].table_config = list(tables)

    profilers_results = run(gather_profiling_results(*available_profilers.values()))

    if available_profilers.get("SNF"):
        available_profilers["SNF"].executor.shutdown()

    print(f"Profiling took: {time.time() - ts} sec.\n\n")

    analyzer = Analyzer(profiling_results=profilers_results,
                        constraint_identification_rules=CONSTRAINT_IDENTIFICATION_RULES,
                        add_adf_framework_template=FLAG_SUGGEST_MERGE_STATEMENT_FOR_ADF_FRAMEWORK)

    if FLAG_PRINT_PROFILING_STAT:
        print(json.dumps(profilers_results, indent=4, cls=NumpyEncoder))
    if WRITE_TO_FILE:
        print_results(json.dumps(profilers_results, indent=4, cls=NumpyEncoder),
                      output_file_path=PROFILING_OUTPUT_FILE_PATH)
        print_results(json.dumps(analyzer.suggest_constraints(), indent=4, cls=NumpyEncoder),
                      output_file_path=ANALYSIS_OUTPUT_FILE_PATH)
        print(f"Constraint suggestions are available at '{ANALYSIS_OUTPUT_FILE_PATH}'")
    else:
        print(json.dumps(analyzer.suggest_constraints(), indent=4, cls=NumpyEncoder))
