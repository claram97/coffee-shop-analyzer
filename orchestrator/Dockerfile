FROM python:3.9.7-slim
COPY orchestrator /
RUN python -m unittest tests/test_common.py
ENTRYPOINT ["/bin/sh"]
