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
sudo apt install -y git python3 python3-pip python3-venv python3-full
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

Run the setup script to install the OS packages and create a local Python virtual environment in `.venv`:

```bash
chmod +x setup.sh start_https.sh
sudo ./setup.sh
```

The setup script:

- installs `python3`, `pip`, and `openssl`
- creates `.venv` in the project directory
- installs the Python packages from `requirements.txt` into that virtual environment
- does not use `systemctl` on Ubuntu environments
- creates and starts `hw-nlsql-https.service` only on supported non-Ubuntu `systemd` environments

## Run

If the service was created, manage the HTTPS service with `systemctl`:

```bash
sudo systemctl status hw-nlsql-https.service
sudo systemctl restart hw-nlsql-https.service
sudo journalctl -u hw-nlsql-https.service -f
```

The launcher still generates a self-signed certificate under `.certs/` if one does not already exist.

If `systemd` is not available, start the app directly:

```bash
/bin/bash ./start_https.sh
```

## Configure

1. Open the app.
2. Create or select a saved connection profile on the login page.
3. Log in with the database user and password for that profile.
4. Open `Admin > Setup configdb` and choose the schemas NL_SQL should use.
5. Use `HeatWave > HWnlsql` or `HeatWave > HWVision`.

Profiles are stored in `profiles.json`. Only non-secret connection details are stored there.
