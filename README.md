## Description
This is a Python script to handle long-running OCR/hOCR tasks from an Islandora site. It replaces the
Hypercube microservice and is designed to be run as a standalone daemon. 

It connects in place of Alpaca (i.e you should disable the ocr derivative service from your Alpaca configuration).

It connects to your ActiveMQ/Artemis message broker, listens for OCR task messages, processes them by calling tesseract
with the provided arguments. It then uploads the resulting OCR/hOCR files directly back to your Islandora Drupal site 
skipping Alpaca). 

This script requires Python 3.x and the Python packages specified in `requirements.txt`.

I wrote this as some of our images required a long time to process with tesseract and the task was causing some messages to time out while waiting for the response. This
script will NACK messages it can't process so they are not lost and only ACK once the OCR/hOCR files are successfully uploaded back to Islandora.

## Configuration

Configuration is done via a YAML file and/or command line arguments. If you provide both, command line arguments will override the YAML file settings.

You can find a sample configuration file [here](config.yml.dist).

### Configuration Parameters

* `--config-file`: (string) Path to the YAML configuration file.
* `--stomp-server`: (string) The hostname or IP address of the STOMP message broker (default: 127.0.0.1).
* `--stomp-port`: (int) The port number of the STOMP message broker (default: 61613).
* `--stomp-login`: (string) The login username for the STOMP message broker.
* `--stomp-password`: (string) The login password for the STOMP message broker.
* `--stomp-queue`: (string) The name of the STOMP queue to listen to.
* `--jwt-username`: (string) A Drupal username for authenticating with Islandora.
* `--jwt-password`: (string) The password for the Drupal user.
* `--jwt-drupal_base_url`: (string) The hostname of the Drupal site.
* `--concurrent-workers`: (int) The number of concurrent worker threads to process messages (default: 1).
* `--tesseract-path`: (string) The path to the tesseract executable.
* `--convert-path`: (string) The path to the ImageMagick convert executable.
* `--identify-path`: (string) The path to the ImageMagick identify executable.
* `--temporary-directory`: (string) The path to a temporary directory for processing files (default: /tmp).
* `--log-file`: (string) The path to the log file.
* `--log-level`: (string) The logging level (DEBUG, INFO, WARNING, ERROR) (default: INFO).

### Sample Configuration File

```yaml
stomp:
    server: localhost
    port: 61613
    login: guest
    password: guest
    queue: islandora/connector-ocr
jwt:
    username: drupal_user
    password: drupal_password
    drupal_base_url: http://localhost
concurrent_workers: 1
temporary_directory: /tmp/image_processor
tools:
    tesseract_path: /usr/bin/tesseract
    convert_path: /usr/bin/convert
    identify_path: /usr/bin/identify
log_file: ocr_worker.log
log_level: DEBUG
```

## Usage
To run the OCR worker, use the following command:

```bash
python ocr_worker.py --config-file config.yml
```

### JWT expiration

Messages retrieved from the Stomp/JMS queue contain a JWT token for authentication with Drupal. These tokens have
an expiration time and if the token is expired when the message is processed, the script will attempt to 
re-authenticate with Drupal using the provided username and password to obtain a new token.

Ensure that the Drupal user specified in the configuration has the necessary permissions to upload OCR/hOCR files.

### SystemD

You can create a SystemD service file to run this script as a daemon. Here is a sample service file:

```ini
[Unit]
Description=OCR Worker Service
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/ocr_worker
ExecStart=/opt/ocr_worker/.venv/bin/python3 /opt/ocr_worker/worker.py --config-file /opt/ocr_worker/local.yml
Restart=on-failure
RestartSec=5
Environment=PYTHONUNBUFFERED=1
User=root
Group=root

[Install]
WantedBy=multi-user.target
```
