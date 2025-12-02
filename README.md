# 🚀 High-Performance ETL Engine

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![PostgreSQL](https://img.shields.io/badge/postgresql-supported-336791)
![License](https://img.shields.io/badge/license-MIT-orange)
![Maintained](https://img.shields.io/badge/maintained-yes-success)
![Platform](https://img.shields.io/badge/platform-linux-lightgrey)
![ETL](https://img.shields.io/badge/type-ETL-blueviolet)
![Workers](https://img.shields.io/badge/multiprocessing-enabled-blue)
![Metrics](https://img.shields.io/badge/monitoring-Prometheus-yellow)

## 📌 О проекте
Высокопроизводительный ETL-движок для массового импорта
данных из множества форматов (CSV, JSON, XLSX, TXT, LOG, SQL,
GZ и др.) в PostgreSQL с поддержкой многопроцессной
обработки, больших батчей, устойчивости к ошибкам и встроенным мониторингом.

## ✨ Возможности
- 🔥 Многопроцессный ingestion (количество воркеров = CPU-1)
- 💾 Batch-insert в PostgreSQL
- 📂 Рекурсивный обход директорий
- 📊 Встроенные Prometheus-метрики (Light Mode)
- 📝 Расширенный JSON-лог + errors.log
- 🔄 Checkpoint — продолжение после падения
- 🧩 Универсальные парсеры для большинства текстовых форматов
- 📈 Web-панель мониторинга (FastAPI Light)

## 🗂 Поддерживаемые форматы
- CSV, TSV  
- JSON, JSONL  
- TXT, LOG  
- GZ (JSON lines)  
- XLS, XLSX  
- SQL dumps (строчным парсером)  
- INFO  
- DB (generic raw loader)

## 🏗 Архитектура
```bash
Directory Scanner → Worker Pool → Writer Queue → PostgreSQL
↑
Prometheus
```

## ⚙ Установка
```bash
sudo bash deploy.sh
```

## ▶ Запуск
```bash
sudo systemctl start etl-loader
sudo systemctl status etl-loader
```

## 📡 Метрики
Prometheus endpoint:
```bash
http://localhost:9090/etl/metrics
```


## 📝 Логи
```bash
/opt/etl_loader/logs/etl.log
/opt/etl_loader/logs/etl.jsonl
/opt/etl_loader/logs/errors.log
```


## 📄 License
MIT License
