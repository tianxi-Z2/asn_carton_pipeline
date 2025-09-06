FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim


COPY requirements.txt requirements.txt

RUN apt update && apt install -y \
    curl jq unzip wget zip \
    python3-pip nodejs npm

RUN apt-get install -y python3-venv

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade virtualenv

ENV VIRTUAL_ENV=/opt/venv_sofg
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$virtual_env/bin:$PATH"

RUN pip3 install -r requirements.txt