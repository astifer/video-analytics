# Распределенная видеоаналитика

### Запуск

```
docker-compose up --build
```

### Ключевые моменты реализации
- Создание сценария: `POST /scenario/` генерирует уникальный идентификатор сценария, устанавливает начальный статус и инициализирует предсказания.
- Обновление статуса сценария: `POST /scenario/{scenario_id}/`позволяет изменить статус сценария, проверяя наличие указанного сценария.
- Получение информации о сценарии: `GET /scenario/{scenario_id}/` возвращает текущий статус выбранного сценария.
- Получение результатов предсказаний: `GET /prediction/{scenario_id}/` возвращает результаты предсказаний, привязанные к сценарию (в данном случае реализован stub‑вариант).


### Структура
```
project/
├── api/
│   ├── main.py
│   ├── Dockerfile
|   └── requirements.txt
├── inference/
│   ├── inference_service.py
│   ├── model.py
│   ├── Dockerfile
|   └── requirements.txt
├── runner/
│   ├── runner.py
│   ├── Dockerfile
|   └── requirements.txt
└── docker-compose.yml
```