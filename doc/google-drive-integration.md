# Google drive integration

Outputs are written to spreadsheets in google drive via the google drive
and google sheets APIs.

You'll need to set the following environment variables:

- `GOOGLE_CLIENT_EMAIL`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_PRIVATE_KEY_ID`
- `GOOGLE_PRIVATE_KEY`

Finally, `PLATFORM_METRICS_MAILING_LIST` should be set to a google group
or email for the outputs to be shared with.

## Managing credentials
Encrypted credentials are stored in [govuk-secrets](https://github.com/alphagov/govuk-secrets).

Service account credentials should be added to the encrypted hieradata for puppet.

## Setting up a service account
Log into the [google api account](https://github.com/alphagov/govuk-secrets/tree/master/pass/2ndline/google-accounts) to manage service user accounts.

We use these accounts so that outputs of data pipelines are not tied to
individual users.

Follow the instructions in [http://pygsheets.readthedocs.io/en/latest/authorizing.html](authorizing pygsheets).

When setting up the account, choose the JSON version of the key so you can
read off the credentials you need.

Further information: [Understanding service accounts](https://cloud.google.com/iam/docs/understanding-service-accounts).
