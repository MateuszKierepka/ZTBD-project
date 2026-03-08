from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class VolumeConfig:
    name: str
    users: int
    people: int
    content: int
    watch_history: int
    my_list_per_profile: tuple[int, int]
    ratings_per_profile: tuple[int, int]


VOLUMES: dict[str, VolumeConfig] = {
    "small": VolumeConfig(
        name="small",
        users=1_000,
        people=2_000,
        content=500,
        watch_history=50_000,
        my_list_per_profile=(0, 20),
        ratings_per_profile=(0, 15),
    ),
    "medium": VolumeConfig(
        name="medium",
        users=100_000,
        people=20_000,
        content=10_000,
        watch_history=5_000_000,
        my_list_per_profile=(0, 15),
        ratings_per_profile=(0, 10),
    ),
    "large": VolumeConfig(
        name="large",
        users=1_000_000,
        people=50_000,
        content=30_000,
        watch_history=50_000_000,
        my_list_per_profile=(0, 8),
        ratings_per_profile=(0, 5),
    ),
}

PLATFORM_START = datetime(2020, 1, 1)
PLATFORM_END = datetime(2025, 12, 31)
