from enum import Enum


class Topology(Enum):
    ONE_TO_ONE = "docker-compose-one-to-one.yml"
    ONE_TO_MANY = "docker-compose-one-to-many.yml"
