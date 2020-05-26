import sys
from argparse import ArgumentParser

from config import customer_id, shared_key
from log_analytics_client import post_data, csv_to_json


def main():
    usage = "python --workspace_id='xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' " \
            "--primary_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' --blob=<file1 " \
            "file2 or url1 url2>  --table_name=<table_name> " \
            "--use_types=True\n" \
            "--workspace_id: Mandatory - The unique identifier for the Log " \
            "Analytics workspace. \n" \
            "--primary_key: Mandatory - Primary key for authentication. Can" \
            " be retrived from Log Analytics workspace then Advanced " \
            "Settings and then Connected Sources \n" \
            "--blob: Mandatory - CSV file or url to a blob containing " \
            "events.\n" \
            " May be used as list with space delimiter," \
            " i.e 'url1 url2 url3 .. urln'\n" \
            "--table_name: Mandatory - The target table name to publish the " \
            "data to in Log Analytics. \n" \
            "--use_types: Optional - when used, the special types as " \
            "number and timestamp preserve their type while " \
            "inserted to the Log Analytics workspace"

    if len(sys.argv) == 1:
        print(f"Wrong usage! \n ARGS: {sys.argv[1:]} \n{usage}")
        sys.exit(1)

    parser = ArgumentParser(usage=usage)
    parser.add_argument("-w", "--workspace_id", action="store", dest="w_id",
                        required=True, help="Mandatory - The unique "
                                            "identifier for the Log Analytics"
                                            " workspace.")
    parser.add_argument("-p", "--primary_key", required=True,
                        action="store", dest="p_key",
                        help="Mandatory - Primary key for authentication. "
                             "Can be retrived from Log Analytics workspace "
                             "then Advanced Settings and then Connected "
                             "Sources ")
    parser.add_argument("-b", "--blob", action="store", dest="blob",
                        nargs='+', required=True,
                        help="Mandatory - CSV file or url to a blob "
                             "containing events.\n May be used as list with "
                             "space delimiter, i.e 'url1 url2 url3 .. urln'")
    parser.add_argument("-t", "--table_name", action="store", dest="table",
                        required=True,
                        help="Mandatory - The target table name to publish the"
                             " data to in Log Analytics")
    parser.add_argument("--use_types", action="store_true", dest="use_types",
                        default=False,
                        help="Optional - when used, the special types as "
                             "number and timestamp preserve their type while "
                             "inserted to the Log Analytics workspace")

    args = parser.parse_args()
    # convert downloaded csv file from url, to json format
    # push data to Log Analytics
    for url in args.blob:
        for body in csv_to_json(url=url):
            post_data(customer_id=args.w_id, shared_key=args.p_key, body=body,
                      log_type=args.table)
        print(f"File {args.table} was uploaded")
    print(f"done urls")


if __name__ == "__main__":
    # main()
    urls = ["https://galhaeastus.blob.core.windows.net/sre/perf/perf1.csv",
            "https://galhaeastus.blob.core.windows.net/sre/consumers"
            "/consumers1.csv"]
    urls2 = ["https://galhaeastus.blob.core.windows.net/sre/perf"
             "/perf2.csv",
             "https://galhaeastus.blob.core.windows.net/sre/perf/perf3.csv",
             "https://galhaeastus.blob.core.windows.net/sre/consumers"
             "/consumers2.csv",
             "https://galhaeastus.blob.core.windows.net/sre/consumers"
             "/consumers3.csv",
             "https://galhaeastus.blob.core.windows.net/sre/consumers"
             "/consumers4.csv"]
    # push data to Log Analytics
    for url in urls2:
        fname = url.split('/')[-1].split('.')[0]
        for body in csv_to_json(url=url, use_type=True):
            post_data(customer_id=customer_id, shared_key=shared_key,
                      body=body,
                      log_type=fname)
        print(f"done file {fname}")
    print(f"done urls")
