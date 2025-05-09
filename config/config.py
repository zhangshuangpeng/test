from typing import TypeVar
from pydantic import BaseModel
import yaml
import os

T = TypeVar("T")

class BaseConfig(BaseModel):
    """
    Config 基类
    """
    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def parse_from_config(cls: T, config_path: str = r"E:\fastApiProject\md4x\config\config.yaml") -> T:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件未找到: {config_path}")
        with open(config_path, 'r', encoding='utf-8') as file:
            config_data = yaml.safe_load(file)
        if cls.__name__.lower() not in config_data:
            raise KeyError(f"配置文件中缺少 {cls.__name__.lower()} 配置")
        return cls(**config_data[cls.__name__.lower()])

class TaosConfig(BaseConfig):
    host: str
    port: int
    reset_timeout: int
    user: str
    password: str
    timezone: str
    database:str



