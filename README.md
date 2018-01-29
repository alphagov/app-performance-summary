# GOV.UK App performance summary

Extracts error rate information from graphite

## Nomenclature

- **Error rate**: The rate of 5xx HTTP responses as a percentage of all
  responses for an application.

## Technical documentation

Install dependencies using [pipenv](https://docs.pipenv.org/).

### Dependencies

This script assumes the machine running it can access graphite.

### Running the application

`pipenv run python run_summary.py`

## Licence

[MIT License](LICENCE)
