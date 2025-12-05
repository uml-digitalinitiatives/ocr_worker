import argparse
import glob
import json
import logging
import os
import shlex
import subprocess
import time
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from json import JSONDecodeError
from tempfile import NamedTemporaryFile

import requests
import stomp
import yaml
from stomp.connect import StompConnection12

class MessageListener(stomp.ConnectionListener):
    connection = None
    executor = None
    configuration = {}

    def __init__(self, conn: StompConnection12, executor: ThreadPoolExecutor, config: dict, logger: logging.Logger):
        self.executor = executor
        self.connection = conn
        self.configuration = config
        self.logger = logger

    def on_error(self, frame):
        """Message listener error handler.
        :param frame: STOMP frame
        """
        self.logger.error('received an error headers -> ("%s"), body -> ("%s")' % (frame.headers, frame.body))

    def on_message(self, frame):
        """Message listener message handler.
        :param frame: STOMP frame
        """
        self.logger.debug('received a message headers -> ("%s"), body -> ("%s")' % (frame.headers, frame.body))
        try:
            future = self.executor.submit(self._process_message, frame)
            future.add_done_callback(self._log_future_exception)
        except Exception as e:
            self.logger.error(f"Failed to submit message to executor: {e}")

    def _log_future_exception(self, future):
        """Log exceptions from future tasks.
        :param future: Future object
        """
        exception = future.exception()
        if exception:
            self.logger.error(f"Exception in message processing: {exception}")

    def _process_message(self, frame):
        """Process a single message frame.
        :param frame: STOMP frame
        """
        self.logger.debug(f"Processing message: {frame.headers['message-id']}")
        message_id = frame.headers.get('message-id')
        subscription_id = frame.headers.get('subscription')
        authorization = frame.headers.get('Authorization') or frame.headers.get('authorization')
        try:
            payload = json.loads(frame.body)
        except JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e} in message ID: {message_id}")
            self.connection.nack(message_id, subscription_id)
            return

        try:
            content = payload['attachment']['content']
            additional_args = content.get('args', '')
            mimetype = content['mimetype']
            resource_uri = content['source_uri']
            content_location = content['file_upload_uri']
            destination = content['destination_uri']
            process_id = payload['object']['id']
        except KeyError as e:
            self.logger.error(f"Missing expected element: {e} in message ID: {message_id}")
            self.connection.nack(message_id, subscription_id)
            return

        try:
            temp_dir = self.configuration.get('temporary_directory', '/tmp')
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Failed to create temporary directory: {e} in message ID: {message_id}")
            self.connection.nack(message_id, subscription_id)
            return

        output_id = f"{process_id.replace(':','_')}-{message_id.replace(',','_')}"

        resp = requests.get(resource_uri, headers={'Authorization': authorization}, stream=True, timeout=30)
        resp.raise_for_status()

        with NamedTemporaryFile(delete=False, dir=temp_dir) as tmp:
            tmp_file_path = tmp.name
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)

        tess_path = self.configuration['tesseract_path']
        extra_args_list = shlex.split(additional_args) if additional_args else []
        output_base = os.path.join(temp_dir, output_id)

        arguments = [tess_path, tmp_file_path, output_base] + extra_args_list
        self.logger.debug(f"Running Tesseract with arguments: {arguments}")
        result = subprocess.run(args=arguments, cwd=temp_dir, capture_output=True, text=True)
        if result.returncode != 0:
            self.logger.error(
                f"Tesseract failed (rc={result.returncode}) for message ID: {output_base}. "
                f"stdout: {result.stdout!r} stderr: {result.stderr!r}"
            )
            self.connection.nack(message_id, subscription_id)
            return

        self.logger.info(f"Tesseract completed successfully for message ID: {output_id}")
        files = glob.glob(temp_dir + "/" + output_id + ".*")
        try:
            if len(files) == 1:
                headers = {'Authorization': authorization, 'Content-Type': mimetype,
                           'Content-Location': content_location}
                with open(files[0], 'rb') as f:
                    put_resp = requests.put(destination, data=f, headers=headers, timeout=300)
                put_resp.raise_for_status()
                self.logger.info(f"Successfully uploaded processed file to {destination} for message ID: {output_id}")
                self.connection.ack(message_id, subscription_id)
            else:
                self.logger.error(f"Expected one output file, found {len(files)} for message ID: {output_id}")
                self.connection.nack(message_id, subscription_id)
                return
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error during file upload for message ID: {output_id}: {e}")
            self.connection.nack(message_id, subscription_id)
            return
        finally:
            # cleanup temporary files
            try:
                if os.path.exists(tmp_file_path):
                    os.remove(tmp_file_path)
            except Exception:
                pass
            for f in files:
                try:
                    os.remove(f)
                except Exception:
                    pass


def main_worker(configuration: dict) -> None:
    """Main worker function to set up the message processing.
    :param configuration: configuration dictionary
    """
    logger = logging.Logger('ocrWorker')
    logger.setLevel(configuration['log_level'])
    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if 'log_file' in configuration:
        ch = logging.FileHandler(configuration['log_file'])
    else:
        ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    logger.propagate = False
    conn = stomp.Connection12([(configuration['stomp_server'], configuration['stomp_port'])], heartbeats=(30000, 30000))
    executor = ThreadPoolExecutor(max_workers=configuration['concurrent_workers'])
    listener = MessageListener(conn, executor, configuration, logger)
    conn.set_listener('ocrWorker', listener)
    conn.connect(configuration['stomp_login'], configuration['stomp_password'], wait=True)
    for i in range(configuration['concurrent_workers']):
        conn.subscribe(destination=configuration['stomp_queue'], id=f'ocrWorker-{i}', ack='client-individual')

    # Wait until we receive a shutdown signal
    # TODO: Is this the most efficient way to do this?
    try:
        while True:
            time.sleep(100)
    except (KeyboardInterrupt, SystemExit):
        print('Received keyboard interrupt, quitting.')
    finally:
        try:
            executor.shutdown(wait=True)
            conn.disconnect()
        except Exception as e:
            print(f"Error during shutdown: {e}")


def parse_config(config_file_path: str) -> dict:
    """Parse configuration file and return configuration dictionary.
    :param config_file_path: configuration file path
    :return: configuration dictionary
    """
    config = {}
    with open(config_file_path, 'r') as f:
        config_data = yaml.safe_load(f)
    if 'stomp' in config_data:
        config['stomp_server'] = config_data['stomp'].get('server', '127.0.0.1')
        config['stomp_port'] = config_data['stomp'].get('port', 61613)
        config['stomp_login'] = config_data['stomp'].get('login', None)
        config['stomp_password'] = config_data['stomp'].get('password', None)
        config['stomp_queue'] = config_data['stomp'].get('queue', None)
    if 'tools' in config_data:
        config['tesseract_path'] = config_data['tools'].get('tesseract_path')
        config['convert_path'] = config_data['tools'].get('convert_path')
        config['identify_path'] = config_data['tools'].get('identify_path')
    if 'temporary_directory' in config_data:
        config['temporary_directory'] = config_data.get('temporary_directory')
    if 'concurrent_workers' in config_data:
        config['concurrent_workers'] = config_data.get('concurrent_workers')
    if 'log_level' in config_data:
        config['log_level'] = config_data.get('log_level', 'INFO')
    if 'log_file' in config_data:
        config['log_file'] = config_data.get('log_file')
    return config

def parse_command_line_args(config_data: dict, parser_args: Namespace) -> dict:
    """Override configuration dictionary with command line arguments if provided.
    :param config_data: configuration dictionary
    :param parser_args: command line arguments
    :return: updated configuration dictionary
    """
    if 'stomp-server' in parser_args and parser_args['stomp-server'] is not None:
        config_data['stomp_server'] = parser_args['stomp-server']
    if 'stomp-port' in parser_args and parser_args['stomp-port'] is not None:
        config_data['stomp_port'] = parser_args['stomp-port']
    if 'stomp-login' in parser_args and parser_args['stomp-login'] is not None:
        config_data['stomp_login'] = parser_args['stomp-login']
    if 'stomp-password' in parser_args and parser_args['stomp-password'] is not None:
        config_data['stomp_password'] = parser_args['stomp-password']
    if 'concurrent-workers' in parser_args and parser_args['concurrent-workers'] is not None:
        config_data['concurrent_workers'] = parser_args['concurrent-workers']
    if 'concurrent_workers' not in config_data:
        config_data['concurrent_workers'] = 4  # default value
    if 'tesseract-path' in parser_args and parser_args['tesseract-path'] is not None:
        config_data['tesseract_path'] = parser_args['tesseract-path']
    if 'convert-path' in parser_args and parser_args['convert-path'] is not None:
        config_data['convert_path'] = parser_args['convert-path']
    if 'identify-path' in parser_args and parser_args['identify-path'] is not None:
        config_data['identify_path'] = parser_args['identify-path']
    if 'temporary-directory' in parser_args and parser_args['temporary-directory'] is not None:
        config_data['temporary_directory'] = parser_args['temporary-directory']
    if 'log-level' in parser_args and parser_args['log-level'] is not None:
        config_data['log_level'] = parser_args['log-level']
    if 'log-file' in parser_args and parser_args['log-file'] is not None:
        config_data['log_file'] = parser_args['log-file']
    return config_data

def main(args: Namespace) -> None:
    """Main function to process arguments and call main worker.
    :param args: command line arguments
    """
    config_data = parse_config(args.config_file)
    config_data = parse_command_line_args(config_data, args)
    errors = []
    if 'tesseract_path' not in config_data:
        errors.append("tesseract_path not defined")
    if 'convert_path' not in config_data:
        errors.append("convert_path not defined")
    if 'identify_path' not in config_data:
        errors.append("identify_path not defined")
    if errors:
        for error in errors:
            print(f"Error: {error}")
        raise ValueError("Configuration errors found. Please fix and try again.")
    print("Configuration successfully loaded:")
    main_worker(config_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OCR/hOCR derivative service")
    parser.add_argument("--config-file", type=str, default="config.yaml", required=False, help="Path to a Yaml configuration file")
    g2 = parser.add_argument_group("Command line options (override config file settings, if set)")
    g2.add_argument("--stomp-server", type=str, default=None, required=False, help="Stomp server address (default: 127.0.0.1)")
    g2.add_argument("--stomp-port", type=int, default=None, required=False, help="Stomp server port (default: 61613)")
    g2.add_argument("--stomp-login", type=str, default=None, required=False, help="Stomp login")
    g2.add_argument("--stomp-password", type=str, default=None, required=False, help="Stomp password")
    g2.add_argument("--stomp-queue", type=str, default=None, required=False, help="Stomp message queue name")
    g2.add_argument("--concurrent-workers", type=int, default=None, required=False, help="Number of concurrent workers (default: 4)")
    g2.add_argument("--tesseract-path", type=str, default=None, required=False, help="Path to tesseract executable")
    g2.add_argument("--convert-path", type=str, default=None, required=False, help="Path to ImageMagick convert executable")
    g2.add_argument("--identify-path", type=str, default=None, required=False, help="Path to ImageMagick identify executable")
    g2.add_argument("--temporary-directory", type=str, default=None, required=False, help="Path to temporary working directory (default: /tmp)")
    g2.add_argument("--log-file", type=str, default=None, required=False, help="Path to log file")
    g2.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], required=False, help="Logging level (default: INFO)")
    parser_args = parser.parse_args()
    main(parser_args)
