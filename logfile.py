""" This module defines methods creating the logfile.

"""

# standard library imports
import datetime
import time
import pickle

# related third party imports
import pandas as pd
from tabulate import tabulate

# local application/library specific imports
# None


def create_logfile(logfile_path):
    log = []
    event = "Start"
    description = "Pipeline initialized."
    runtime = 0
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {'Event': event, 'Description': description, 'Runtime (s)': runtime,
                 'Timestamp': timestamp}
    log.append(log_entry)

    with open(logfile_path, "wb") as fp:
        pickle.dump(log, fp)

    return log


def append_logfile(logfile_path, event_title, event_description, runtime):
    with open(logfile_path, "rb") as fp:
        log = pickle.load(fp)

    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {'Event': event_title, 'Description': event_description,
                 'Runtime (s)': runtime, 'Timestamp': timestamp}
    log.append(log_entry)

    with open(logfile_path, "wb") as fp:
        pickle.dump(log, fp)


def end_logfile(logfile_path):
    with open(logfile_path, "rb") as fp:
        log = pickle.load(fp)

    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {'Event': "End", 'Description': "Pipeline finished.", 'Runtime (s)': 0,
                 'Timestamp': timestamp}
    log.append(log_entry)

    with open(logfile_path, "wb") as fp:
        pickle.dump(log, fp)


def pretty_print_logfile(logfile_path):
    with open(logfile_path, "rb") as fp:
        log = pickle.load(fp)

    log_data_frame = pd.DataFrame(log, columns=['Event', 'Description', 'Runtime (s)',
                                                'Timestamp'])
    print(tabulate(log_data_frame, headers=['Event', 'Description', 'Runtime (s)',
                                            'Timestamp']))
