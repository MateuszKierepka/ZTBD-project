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
        users=5_000,
        people=2_000,
        content=2_000,
        watch_history=300_000,
        my_list_per_profile=(0, 15),
        ratings_per_profile=(0, 10),
    ),
    "medium": VolumeConfig(
        name="medium",
        users=20_000,
        people=5_000,
        content=5_000,
        watch_history=500_000,
        my_list_per_profile=(0, 10),
        ratings_per_profile=(0, 8),
    ),
    "large": VolumeConfig(
        name="large",
        users=100_000,
        people=20_000,
        content=15_000,
        watch_history=6_000_000,
        my_list_per_profile=(0, 8),
        ratings_per_profile=(0, 5),
    ),
}

PLATFORM_START = datetime(2020, 1, 1)
PLATFORM_END = datetime(2025, 12, 31)
