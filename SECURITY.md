# Security policy

Do not put Redis passwords in source, logs, panic messages, or CI output. Use
`REDIS_TEST_URL` only in secret-managed environments. `rediss://` never falls
back to cleartext. Hostname verification is enabled for rustls connections.

Report security issues privately to the repository owner rather than opening a
public issue with credentials or exploit details.
