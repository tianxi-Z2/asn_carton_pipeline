#!/bin/sh

rm -rf /google-cloud-sdk/google-cloud-sdk
curl -sSL https://sdk.cloud.google.com >/tmp/gcl && bash /tmp/gcl --install-dir=/google-cloud-sdk --disable-prompts
/google-cloud-sdk/google-cloud-sdk/bin/gcloud config set disable_usage_reporting true
/google-cloud-sdk/google-cloud-sdk/bin/gcloud components install core beta gsutil --quiet
rm -rf /tmp/gcl

echo 'if [ -f "/google-cloud-sdk/google-cloud-sdk/path.bash.inc" ]; then source "/google-cloud-sdk/google-cloud-sdk/path.bash.inc"; fi' >>/home/airflow/.bashrc
echo 'if [ -f "/google-cloud-sdk/google-cloud-sdk/path.bash.inc" ]; then source "/google-cloud-sdk/google-cloud-sdk/path.bash.inc"; fi' >>/home/airflow/.profile

chown -R airflow:airflow /home/airflow/.config