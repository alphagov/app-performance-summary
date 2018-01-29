# GOV.UK App performance summary

Extracts error rate information from graphite.

## Nomenclature

- **Error count**: The total number of 5xx errors encountered within a 10m window.
- **Total response count**: The total number of responses served within a 10m window.
- **Error rate**: The error count as a percentage of total responses.

## Technical documentation

Install dependencies using [pipenv](https://docs.pipenv.org/).

### Dependencies

This script assumes the machine running it can access graphite.

### Running the script
`pipenv run python run_summary.py`

You can check the data quality with:
`pipenv run lint_data.py output/*.csv`

## Licence

[MIT License](LICENCE)
