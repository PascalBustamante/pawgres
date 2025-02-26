from configparser import ConfigParser


def config(filename="../config/config.ini", section="postgresql"):
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
