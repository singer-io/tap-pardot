config = {
    "test_name": "tap_pardot_combined_test",
    "tap_name": "tap-pardot",
    "type": "platform.pardot",
    "properties": {"start_date": "TAP_PARDOT_START_DATE", "email": "TAP_PARDOT_EMAIL",},
    "credentials": {
        "password": "TAP_PARDOT_PASSWORD",
        "user_key": "TAP_PARDOT_USER_KEY",
    },
    "streams": {
        "campaigns": {"id"},
        "email_clicks": {"id"},
        "list_memberships": {"id"},
        "lists": {"id"},
        "opportunities": {"id"},
        "prospect_accounts": {"id"},
        # "prospects": {"id"}, this stream is HUGE and can take a long time to sync
        "users": {"id"},
        "visitor_activities": {"id"},
        "visitors": {"id"},
        "visits": {"id"},
    },
    "exclude_streams": [],
}
