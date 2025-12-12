import argparse
import importlib.util
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
worker_path = PROJECT_ROOT / "worker.py"
spec = importlib.util.spec_from_file_location("worker", str(worker_path))
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)

class TestWorker:

    def test_configuration_file(self):
        filepath = Path(__file__).resolve().parent / "resources" / "test1.config.yaml"
        expected = {
            "stomp_server": "example.server.com",
            "stomp_port": 61613,
            "stomp_login": "alpaca",
            "stomp_password": "alpacaPassword",
            "stomp_queue": "islandora/connector-ocr",
            "jwt_username": "drupal_user",
            "jwt_password": "drupalPassword",
            "jwt_drupal_base_url": "https://drupal.instance.url",
            "concurrent_workers": 3,
            "tesseract_path": "/usr/bin/tesseract",
            "convert_path": "/usr/bin/convert",
            "identify_path": "/usr/bin/identify",
            "log_file": "/opt/ocrWorker/ocr_worker.log",
            "log_level": "DEBUG",
        }
        output = worker._parse_config(filepath)
        assert output == expected

    def test_command_line_args_empty(self):
        parser = argparse.Namespace()
        parsed_args = worker._parse_command_line_args({}, parser)
        assert {} == parsed_args

    def test_command_line_args(self):
        parser = argparse.Namespace(
            stomp_server="cli.server.com",
            stomp_port=61614,
            temporary_directory="/var/tmp",
            concurrent_workers=5,
            log_level="DEBUG",
        )
        parsed_args = worker._parse_command_line_args({}, parser)
        expected = {
            "stomp_server": "cli.server.com",
            "stomp_port": 61614,
            "temporary_directory": "/var/tmp",
            "concurrent_workers": 5,
            "log_level": "DEBUG",
        }
        assert parsed_args == expected

    def test_add_defaults_to_args(self):
        args = {}
        parsed_args = worker._set_defaults(args)
        assert parsed_args["stomp_server"] == "127.0.0.1"
        assert parsed_args["stomp_port"] == 61613
        assert parsed_args["temporary_directory"] == "/tmp"
        assert parsed_args["concurrent_workers"] == 1
        assert parsed_args["log_level"] == "INFO"

    def test_override_config_with_args(self):
        config = {
            "stomp_server": "example.server.com",
            "stomp_port": 61613,
            "concurrent_workers": 3,
            "log_level": "DEBUG",
            "temporary_directory": "/opt/worker/tmp",
        }
        args = argparse.Namespace(
            stomp_server="cli.server.com",
            stomp_port=61614,
            concurrent_workers=5,
            log_level="WARNING",
        )
        merged = worker._parse_command_line_args(config, args)
        expected = {
            "stomp_server": "cli.server.com",
            "stomp_port": 61614,
            "concurrent_workers": 5,
            "log_level": "WARNING",
            "temporary_directory": "/opt/worker/tmp",
        }
        assert merged == expected

    def test_full_configuration_process(self):
        filepath = Path(__file__).resolve().parent / "resources" / "test1.config.yaml"
        config = worker._parse_config(filepath)
        args = argparse.Namespace(
            stomp_server="cli.server.com",
            stomp_port=61614,
            concurrent_workers=5,
            log_level="WARNING",
            jwt_drupal_base_url="https://test.instance.url",
        )
        merged = worker._parse_command_line_args(config, args)
        final_config = worker._set_defaults(merged)
        expected = {
            "stomp_server": "cli.server.com",
            "stomp_port": 61614,
            "stomp_login": "alpaca",
            "stomp_password": "alpacaPassword",
            "stomp_queue": "islandora/connector-ocr",
            "jwt_username": "drupal_user",
            "jwt_password": "drupalPassword",
            "jwt_drupal_base_url": "https://test.instance.url",
            "concurrent_workers": 5,
            "tesseract_path": "/usr/bin/tesseract",
            "convert_path": "/usr/bin/convert",
            "identify_path": "/usr/bin/identify",
            "log_file": "/opt/ocrWorker/ocr_worker.log",
            "log_level": "WARNING",
            "temporary_directory": "/tmp",
        }
        assert final_config == expected