ARG base_image=python:3.8.12
FROM ${base_image}

COPY setup.py requirements.txt ./ 
COPY contrib ./contrib/

USER root
RUN apt-get update && apt-get install -y apt-file && apt-file update && apt-get install -y vim python3-dev build-essential
RUN pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl
RUN pip install -r ./requirements.txt
RUN pip install -e .

RUN useradd -ms /bin/bash ray
WORKDIR /home/ray

COPY setup.py requirements.txt /home/ray/
COPY contrib /home/ray/contrib/
RUN chown -R ray:ray /home/ray/
