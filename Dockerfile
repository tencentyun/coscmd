FROM python:3.6

RUN pip install coscmd

ENTRYPOINT ["/usr/local/bin/coscmd"]