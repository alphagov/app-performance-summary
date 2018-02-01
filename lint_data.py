import csv
import sys
from pathlib import Path

fails = []

def success():
    '''
    Report a check that passed
    '''
    print('.', end='')


def fail(msg, file, lineno):
    '''
    Report a check that failed
    '''
    fails.append(msg + ' ({file} on line {lineno})'.format(file=file, lineno=lineno))
    print('x', end='')


def test_line(time, errors, total, file, lineno):
    '''
    Check the numbers look reasonable
    '''
    try:
        errors = float(errors) if errors != '' else 0
    except ValueError:
        fail('Errors should be a number or empty string', file=csv_file.name, lineno=i+1)
        return
    else:
        success()
    try:
        total = float(total) if total != '' else 0
    except ValueError:
        fail('Total should be a number', file=csv_file.name, lineno=i+1)
        return
    else:
        success()

    if errors <= total:
        success()
    else:
        fail('Errors should be less than or equal to total', file=csv_file.name, lineno=i+1)

    if errors >= 0 and total >= 0:
        success()
    else:
        fail('Counts should be positive', file=csv_file.name, lineno=i+1)


csv_files = [Path(path) for path in sys.argv[1:]]

for csv_file in csv_files:
    with csv_file.open(newline='') as f:
        reader = csv.reader(f)
        next(reader)
        last_time = None

        for i, line in enumerate(reader):
            time, errors, total = line
            test_line(time, errors, total, file=csv_file.name, lineno=i+1)

            if last_time is None or (last_time < time):
                success()
            else:
                fail('Timestamps should increase', file=csv_file.name, lineno=i+1)

            last_time = time

        if last_time is not None:
            success()
        else:
            fail('Should have some data points', file=csv_file.name, lineno=1)

print()
if fails:
    for fail in fails:
        print(fail)
    sys.exit(1)
else:
    print('No problems found.')
