import itertools
from asyncio import run, gather
import json
import time

from config.config import TO_PROFILE
from utils.profilers import Profiler, SNFProfiler, CSVProfiler
from utils.analyzer import Analyzer


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
    available_profilers = {
        "SNF": SNFProfiler([]),
        "CSV": CSVProfiler([])
    }

    for datasource_type, tables in itertools.groupby(TO_PROFILE, lambda x: x["datasource_type"]):
        if available_profilers.get(datasource_type):
            available_profilers[datasource_type].table_config = list(tables)

    profilers_results = run(gather_profiling_results(*available_profilers.values()))

    if available_profilers.get("SNF"):
        available_profilers["SNF"].executor.shutdown()

    print(json.dumps(profilers_results, indent=4))
    print(f"Profiling took: {time.time() - ts} sec.\n\n")

    analyzer = Analyzer(profilers_results)
    print(json.dumps(analyzer.suggest_constraints(), indent=4))
