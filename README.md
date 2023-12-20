# Trading Bot

### Personal trading machine for ETF and Cryptocurrency

[![Test](https://github.com/kai-trading-bot/crypto_bot/actions/workflows/test.yml/badge.svg)](https://github.com/kai-trading-bot/crypto_bot/actions/workflows/test.yml)

## Setup
(Please install [poetry](https://python-poetry.org/docs/) first.)
```bash
poetry install
```

## Start
```bash
# Start Binance bot
python -m stream.binance
```

## Docker
```bash
docker-compose up
# Run InfluxDB CLI Commands
docker exec -it influxdb influx
```

## Research

### JupyterLab support with Plotly
```bash
# Check if the jupyterlab-plotly is installed
jupyter labextension list
# Manually install the plugin
jupyter labextension install @jupyter-widgets/jupyterlab-manager jupyterlab-plotly
# Install the required pip packages:
poetry add ipywidgets --dev
```

Time-series: https://plotly.com/python/time-series/

### Running Installed Scripts in Cronjob
In order to directly run the script, you need to add PATH in the beginning of crontab
```bash
PATH=/home/<user>/anaconda/bin:$PATH
* * * * * some_command
```
