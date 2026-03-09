Запуск

```python proxy/main.py config.yaml```

## Тесты

#### GOMAXPROCS=1
![alt text](image.png)

rate: 12000, maxVU: 3000 -> 10500 RPS, CPU -> 82%, p95 -> 400ms
![alt text](image-1.png)
![alt text](image-2.png)

#### GOMAXPROCS=8
rate: 12000, maxVU: 3000 -> 10500 RPS, CPU -> 150%, p95 -> 61ms
![alt text](image-3.png)
![alt text](image-4.png)
