Run: `./run.sh`
Stop: `docker-compose down` or `docker-compose down -v`


Web Services

- Mage.ai: http://localhost:6789
- pgAdmin: http://localhost:5050
- MinIO: http://localhost:9001
- MLflow: http://localhost:5000
- BentoML: http://localhost:3000


Run test predict: 
`pip install -r requirements.txt`
`python ./test_scripts/test_service.py`
