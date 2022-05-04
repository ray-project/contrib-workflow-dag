ARG base_image=rayproject/ray:1.8.0
FROM ${base_image}

#RUN sed -in 's:LOGGER_LEVEL = "info":LOGGER_LEVEL = "warning":g' /home/ray/anaconda3/lib/python3.7/site-packages/ray/ray_constants.py

COPY setup.py requirements.txt ./ 
COPY contrib ./contrib/

USER root
RUN apt-get update && apt-get install -y apt-file && apt-file update && apt-get install -y vim python3-dev build-essential
RUN pip install -r ./requirements.txt
RUN pip install -e .

COPY setup.py requirements.txt /home/ray/
COPY contrib /home/ray/contrib/
RUN chown -R ray:users /home/ray/

USER ray

