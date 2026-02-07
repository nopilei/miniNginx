Запуск

```python proxy/main.py config.yaml```

## Тесты

#### Конфиг сервера
![img_8.png](img_8.png)

rate: 2500, workers=1 -> 1500 RPS
![img_4.png](img_4.png)

rate: 6500, workers=8 -> 6000 RPS
![img_5.png](img_5.png)

rate: 8000, workers=8, logs off -> 7700 RPS
![img_7.png](img_7.png)