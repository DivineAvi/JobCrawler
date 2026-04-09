import sys

from scraper.cli import merge_csv_main, run

if __name__ == "__main__":
    av = sys.argv[1:]
    if av and av[0] == "merge":
        raise SystemExit(merge_csv_main(av[1:]))
    raise SystemExit(run(av))
