# HeatWave Demo

This application now uses Flask instead of Streamlit while preserving the same core logic:

- login with a saved MySQL connection profile
- maintain `nlsql.configdb`
- run HeatWave `sys.NL_SQL`
- run HeatWave visual prompts with `sys.ML_GENERATE`

## Install

Install `git` first, depending on your operating system:

### Ubuntu

```bash
sudo apt update
sudo apt install -y git python3 python3-pip python3-venv
```

### Oracle Linux 8

```bash
sudo dnf install -y git python3 python3-pip python3-setuptools python3-wheel
```

### Oracle Linux 9

```bash
sudo dnf install -y git python3 python3-pip python3-setuptools python3-pip-wheel
```

Clone the repository and enter the project directory:

```bash
git clone https://github.com/ivanxma/HW-NLSQL-py
cd HW-NLSQL-py
```

Then install the application dependencies at the global system level so they are available when you run the app as `opc` and also when the launcher re-runs with `sudo`:

```bash
sudo python3 -m pip install -r requirements.txt
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
