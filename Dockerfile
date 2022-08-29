# docker build -t brian/sz_incremental_withinfo .
# docker run --user $UID -it -v $PWD:/data -e SENZING_ENGINE_CONFIGURATION_JSON brian/sz_incremental_withinfo -o /data/delta.json -i /data/tmpinfo.json /dev/null

ARG BASE_IMAGE=senzing/senzingapi-runtime
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2022-08-27

LABEL Name="brain/sz_incremental_withinfo" \
      Maintainer="brianmacy@gmail.com" \
      Version="DEV"

RUN apt-get update \
 && apt-get -y install \
        python3 python3-pip

RUN python3 -mpip install orjson

RUN apt-get -y remove build-essential python3-pip
RUN apt-get -y autoremove

COPY sz_incremental_withinfo.py /app/

ENV PYTHONPATH=/opt/senzing/g2/sdk/python

USER 1001

WORKDIR /app
ENTRYPOINT ["/app/sz_incremental_withinfo.py"]

