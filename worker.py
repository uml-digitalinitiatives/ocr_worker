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
from typing import Union

import requests
import stomp
import yaml
from stomp.connect import StompConnection12

class TokenFailedException(Exception):
    """Custom exception for token retrieval failures."""
    pass

class MessageListener(stomp.ConnectionListener):
    connection = None
    executor = None
    configuration = {}
    local_token = None

    def __init__(self, conn: StompConnection12, executor: ThreadPoolExecutor, config: dict, logger: logging.Logger, shutdown_flag: bool = False):
        self.executor = executor
        self.connection = conn
        self.configuration = config
        self.logger = logger
        self.shutdown_flag = shutdown_flag

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
        if not self.shutdown_flag:
            try:
                future = self.executor.submit(self._process_message, frame)
                future.add_done_callback(self._log_future_exception)
            except Exception as e:
                self.logger.error(f"Failed to submit message to executor: {e}")
        else:
            self.logger.info("Shutdown flag is set, not processing new messages.")

    def _log_future_exception(self, future):
        """Log exceptions from future tasks.
        :param future: Future object
        """
        exception = future.exception()
        if exception:
            self.logger.error(f"Exception in message processing: {exception}")

    def _get_new_jwt_token(self, refresh: bool = False) -> Union[str, None]:
        """Obtain a new JWT token.
        :param refresh: Whether to refresh the token
        :return: JWT token string
        """
        if self.local_token is not None and not refresh:
            return self.local_token
        try:
            username = self.configuration.get('jwt_username', None)
            password = self.configuration.get('jwt_password', None)
            drupal_base_url = self.configuration.get('jwt_drupal_base_url', None)
            if username is None or password is None:
                self.logger.error("JWT username or password not provided in configuration.")
                return None
            if drupal_base_url is None:
                self.logger.error("Drupal base URL not provided in configuration.")
                return None
            drupal_base_url = drupal_base_url.rstrip('/')

            resp = requests.post(
                f"{drupal_base_url}/user/login?_format=json",
                json={"name": username, "pass": password},
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get('access_token', None)
            if not token:
                self.logger.error("JWT token (access_token) not found in response.")
                return None
            self.local_token = f"Bearer {token}"
        except KeyError as e:
            self.logger.error(f"Missing expected configuration key: {e}")
            return None
        except JSONDecodeError as e:
            self.logger.error(f"JSON decode error while obtaining JWT token: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error while obtaining JWT token: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error while obtaining JWT token: {e}")
            return None
        return self.local_token

    def _do_file_upload(self, url: str, file_name: str, token: str, mimetype: str, location: str, msg_id: str, sub_id: str) -> None:
        """Upload a file to the specified URL.
        :param url: Upload URL
        :param file_name: Local file name to upload
        :param token: Authorization token
        :param mimetype: MIME type of the file
        :param location: Content-Location header value
        :param msg_id: Message ID for acknowledgment
        :param sub_id: Subscription ID for acknowledgment
        """
        try:
            headers = {'Authorization': token, 'Content-Type': mimetype,
                       'Content-Location': location}
            with open(file_name, 'rb') as f:
                put_resp = requests.put(url, data=f, headers=headers, timeout=300)
            put_resp.raise_for_status()
            self.logger.info(f"Successfully uploaded processed file to {url}")
            self.connection.ack(msg_id, sub_id)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if getattr(e, "response", None) is not None else None
            if status and status == 403:
                self.logger.debug(f"Received 403 status code while uploading file to {url}: {status}")
                raise TokenFailedException(e)
            self.logger.error(f"Failed to upload file to {url}")
            raise


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
            temp_dir = self.configuration.get('temporary_directory')
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
        if len(files) != 1:
            self.logger.error(f"Expected one output file, found {len(files)} for message ID: {output_id}")
            self.connection.nack(message_id, subscription_id)
            return

        has_local_token = self.local_token is not None
        try:
            self._do_file_upload(destination, file_name=files[0], token=authorization, mimetype=mimetype, location=content_location, msg_id=message_id, sub_id=subscription_id)
        except TokenFailedException:
            new_token = self._get_new_jwt_token()
            if new_token:
                try:
                    self._do_file_upload(destination, file_name=files[0], token=new_token, mimetype=mimetype, location=content_location, msg_id=message_id, sub_id=subscription_id)
                except TokenFailedException:
                    if has_local_token:
                        self.logger.error(f"Failed to upload with existing new token, refreshing our local token")
                        new_token = self._get_new_jwt_token(True)
                        if new_token:
                            # If this fails, we give up
                            self._do_file_upload(destination, file_name=files[0], token=new_token, mimetype=mimetype, location=content_location, msg_id=message_id, sub_id=subscription_id)
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
        ch = logging.StreamHandler() #  log to stderr
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
            listener.shutdown_flag = True
            executor.shutdown(wait=True)
            conn.disconnect()
        except Exception as e:
            print(f"Error during shutdown: {e}")


def _parse_config(config_file_path: str) -> dict:
    """Parse configuration file and return configuration dictionary.
    :param config_file_path: configuration file path
    :return: configuration dictionary
    """
    config = {}
    with open(config_file_path, 'r') as f:
        config_data = yaml.safe_load(f)
    if 'stomp' in config_data:
        config['stomp_server'] = config_data['stomp'].get('server')
        config['stomp_port'] = config_data['stomp'].get('port')
        config['stomp_login'] = config_data['stomp'].get('login', None)
        config['stomp_password'] = config_data['stomp'].get('password', None)
        config['stomp_queue'] = config_data['stomp'].get('queue', None)
    if 'jwt' in config_data:
        config['jwt_username'] = config_data['jwt'].get('username', None)
        config['jwt_password'] = config_data['jwt'].get('password', None)
        config['jwt_drupal_base_url'] = config_data['jwt'].get('drupal_base_url', None)
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

def _parse_command_line_args(config_data: dict, parser_cli_args: Namespace) -> dict:
    """Override configuration dictionary with command line arguments if provided.
    :param config_data: configuration dictionary
    :param parser_cli_args: command line arguments
    :return: updated configuration dictionary
    """
    override_args = [
        'stomp_server',
        'stomp_port',
        'stomp_login',
        'stomp_password',
        'concurrent_workers',
        'tesseract_path',
        'convert_path',
        'identify_path',
        'temporary_directory',
        'log_level',
        'log_file',
        'jwt_username',
        'jwt_password',
        'jwt_drupal_base_url'
    ]
    for arg in override_args:
        try:
            if getattr(parser_cli_args, arg) is not None:
                config_data[arg] = getattr(parser_cli_args, arg)
        except AttributeError:
            pass
    return config_data

def _set_defaults(config_data: dict) -> dict:
    """Set default values for configuration dictionary if not already set.
    :param config_data: configuration dictionary
    :return: updated configuration dictionary with defaults set
    """
    defaults = {
        'stomp_server': '127.0.0.1',
        'stomp_port': 61613,
        'concurrent_workers': 1,
        'temporary_directory': '/tmp',
        'log_level': 'INFO'
    }
    for key, value in defaults.items():
        if key not in config_data or config_data[key] is None:
            config_data[key] = value
    return config_data

def main(args: Namespace) -> None:
    """Main function to process arguments and call main worker.
    :param args: command line arguments
    """
    if args.config_file is not None:
        config_data = _parse_config(args.config_file)
    else:
        print("There is no configuration file specified, using only command line arguments.")
        config_data = {}
    config_data = _parse_command_line_args(config_data, args)
    config_data = _set_defaults(config_data)
    errors = []
    check_exec = ['tesseract_path', 'convert_path', 'identify_path']
    for e in check_exec:
        if e not in config_data or not config_data[e]:
            errors.append(f"{e} not defined")
        elif not os.path.isfile(config_data[e]) or not os.access(config_data[e], os.X_OK):
            errors.append(f"{e} '{config_data[e]}' does not exist or is not executable")

    if errors:
        for error in errors:
            print(f"Error: {error}")
        raise ValueError("Configuration errors found. Please fix and try again.")
    main_worker(config_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OCR/hOCR derivative service")
    parser.add_argument("--config-file", type=str, default=None, required=False, help="Path to a Yaml configuration file")
    g2 = parser.add_argument_group("Command line options (override config file settings, if set)")
    g2.add_argument("--stomp-server", type=str, default=None, required=False, help="Stomp server address (default: 127.0.0.1)")
    g2.add_argument("--stomp-port", type=int, default=None, required=False, help="Stomp server port (default: 61613)")
    g2.add_argument("--stomp-login", type=str, default=None, required=False, help="Stomp login")
    g2.add_argument("--stomp-password", type=str, default=None, required=False, help="Stomp password")
    g2.add_argument("--stomp-queue", type=str, default=None, required=False, help="Stomp message queue name")
    g2.add_argument("--concurrent-workers", type=int, default=None, required=False, help="Number of concurrent workers (default: 1)")
    g2.add_argument("--tesseract-path", type=str, default=None, required=False, help="Path to tesseract executable")
    g2.add_argument("--convert-path", type=str, default=None, required=False, help="Path to ImageMagick convert executable")
    g2.add_argument("--identify-path", type=str, default=None, required=False, help="Path to ImageMagick identify executable")
    g2.add_argument("--temporary-directory", type=str, default=None, required=False, help="Path to temporary working directory (default: /tmp)")
    g2.add_argument("--jwt-username", type=str, default=None, required=False, help="username for Drupal to get new JWT token")
    g2.add_argument("--jwt-password", type=str, default=None, required=False, help="password for Drupal to get new JWT token")
    g2.add_argument("--jwt-drupal_base_url", type=str, default=None, required=False, help="Base URL for Drupal site to get new JWT token")
    g2.add_argument("--log-file", type=str, default=None, required=False, help="Path to log file")
    g2.add_argument("--log-level", type=str, default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"], required=False, help="Logging level (default: INFO)")
    parser_args = parser.parse_args()
    main(parser_args)
