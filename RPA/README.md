## Robotics Process Automation


```BASH
docker build -f Dockerfile.RPA . -t rpa_selenium_image
```

```BASH
docker-compose -f docker-compose.RPA.yaml up -d
```

```BASH
docker exec -it rpa-rpa-1 python3 rpa_script.py
```
