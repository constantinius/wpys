import os
import yaml
from dataclasses import dataclass, field
from typing import List

CONFIG_ENV_NAME = "WPYS_CONFIG_FILE"

class ConfigurationException(Exception):
    pass


@dataclass
class ServiceInfo:
    title: str = None
    abstract: str = None
    keywords: List[str] = None
    fees: str = None
    access_constraints: str = None

    provider_name: str = None
    provider_site: str = None
    individual_name: str = None
    electronical_mail_address: str = None


@dataclass
class OperationsInfo:
    pass

@dataclass
class ProcessConfig:
    locations: List[str] = field(default_factory=list)


@dataclass
class WPySConfig:
    main_endpoint_name: str = "/"
    result_endpoint_name: str = "/result/<uuid:job_id>/<result_name>"

    result_chunk_size: int = 65535

    broker_type: str = "redis"
    broker_options: dict = field(default_factory=dict)

    result_backend_type: str = "redis"
    result_backend_options: dict = field(default_factory=dict)

    expiration_time: float = None

    debug: bool = False
    pretty_print: bool = True

    service_info: ServiceInfo = field(default_factory=ServiceInfo)
    process_config: ProcessConfig = field(default_factory=ProcessConfig)

    logging: dict = field(default_factory=dict)

    @classmethod
    def from_config(cls, conf):
        conf['service_info'] = ServiceInfo(**conf.pop('service_info', {}))
        conf['process_config'] = ProcessConfig(**conf.pop('process_config', {}))
        return cls(**conf)

def load_config(config_filename=None) -> WPySConfig:
    config_filename = config_filename or os.environ.get(CONFIG_ENV_NAME)
    if not config_filename:
        raise ConfigurationException(
            f'Unable to load configuration, is the `{CONFIG_ENV_NAME}` variable set?'
        )
    with open(config_filename) as f:
        return WPySConfig.from_config(yaml.load(f))
