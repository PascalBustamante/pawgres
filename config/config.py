from configparser import ConfigParser
import os


def config(filename="../config/config.ini", section="postgresql"):
    print(os.getcwd())
    print(filename)
    parser = ConfigParser()
    parser.read(filename)
    config_dict = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config_dict[param[0]] = param[1]
            print(param)

    else:
        raise Exception(f"Section {section} not found in {filename}")

    return config_dict

logging_config = {
  "version": 1,
  "disable_existing_loggers": false,
  "formatters": {
    "simple": {
      "format": "%(levelname)s: %(message)s",
      "datefmt": "%Y-%m-%dT%H:%M:%S%z"
    }
  },
  "filters": {
    "no_errors": {
      "()": "mylogger.NonErrorFilter"
    }
  },
  "handlers": {
    "stdout": {
      "class": "logging.StreamHandler",
      "formatter": "simple",
      "stream": "ext://sys.stdout",
      "filters": ["no_errors"]
    },
    "stderr": {
      "class": "logging.StreamHandler",
      "formatter": "simple",
      "stream": "ext://sys.stderr",
      "level": "WARNING"
    }
  },
  "loggers": {
    "root": {
      "level": "DEBUG",
      "handlers": [
        "stdout",
        "stderr"
      ]
    }
  }
}