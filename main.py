from asyncio import run
import json
import time

from config import TO_PROFILE
from profiler import Profiler
from analyzer import Analyzer

if __name__ == '__main__':
    ts = time.time()
    profiler = Profiler(TO_PROFILE)
    profiling_results = run(profiler.get_tables_descriptions())
    print(json.dumps(profiling_results, indent=4))
    print(f"Profiling took: {time.time() - ts} sec.")
    profiler.executor.shutdown()
    analyzer = Analyzer(profiling_results)
    print(json.dumps(analyzer.suggest_constraints(), indent=4))
