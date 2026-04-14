# HeatWave Demo

This application now uses Flask instead of Streamlit while preserving the same core logic:

- login with a saved MySQL connection profile
- maintain `nlsql.configdb`
- run HeatWave `sys.NL_SQL`
- run HeatWave visual prompts with `sys.ML_GENERATE`

## Install

```bash
pip install -r requirements.txt
```

## Run

Start the app with HTTPS on port `443`:

```bash
./start_https.sh
```

The launcher generates a self-signed certificate under `.certs/` if one does not already exist.
If port `443` requires elevated privileges on your host, the script re-runs itself with `sudo`.

## Configure

1. Open the app.
2. Create or select a saved connection profile on the login page.
3. Log in with the database user and password for that profile.
4. Open `Admin > Setup configdb` and choose the schemas NL_SQL should use.
5. Use `HeatWave > HWnlsql` or `HeatWave > HWVision`.

Profiles are stored in `profiles.json`. Only non-secret connection details are stored there.
