# tap-pardot
Singer tap for replicating Pardot data.

## Create a Config file
```
{
  "client_id": "secret_client_id",
  "client_secret": "secret_client_secret",
  "refresh_token": "abc123",
  "start_date": "2017-11-02T00:00:00Z",
  "api_output_type": "bulk"
}
```
The `client_id` and `client_secret` keys are your OAuth Salesforce App secrets. The refresh_token is a secret created during the OAuth flow. For more info on the Pardot OAuth flow, visit the Pardot [API documentation](https://developer.salesforce.com/docs/marketing/pardot/guide/authentication.html).

The `start_data` is used by the tap as a bound on the query request, for more information about the format check [Singer best practices for dates](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#dates).

The `api_output_type` is used to define the output on the API call. The default is "bulk" (more information on the "bulk" output call on [Query the Pardot API](https://developer.salesforce.com/docs/marketing/pardot/guide/bulk-data-pull.html#query-the-pardot-api)). With the bulk API call, the call is optimized, on the other hand, it doesn't return additional data in the response (such as nested objects and custom fields). If additional data is needed, change this variable (to either "simple" or "full"), and add the additional data explicitly to the select statement.

---

Copyright &copy; 2019 Stitch
