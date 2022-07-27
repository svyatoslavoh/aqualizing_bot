import configparser
from dataclasses import dataclass


@dataclass
class TgBot:
    token: str
    admin_id: list
    che: list
    discount_projects: list
    discount_networks: list
    conf_url: str


@dataclass
class Config:
    tg_bot: TgBot


def load_config(path: str):
    config = configparser.ConfigParser()
    config.read(path)

    tg_bot = config["tg_bot"]

    return Config(
        tg_bot=TgBot(
            token=tg_bot["token"],
            admin_id=tg_bot["admin_id"].split(','),
            che=tg_bot["che"].split(','),
            discount_projects=tg_bot["discount_projects"].split(','),
            discount_networks=tg_bot["discount_networks"].split(','),
            conf_url=tg_bot["conf_url"]
        )
    )
