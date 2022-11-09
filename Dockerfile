FROM python:3

RUN pip install coscmd

ENTRYPOINT ["/usr/local/bin/coscmd"]