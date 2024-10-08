# This code is borrowed from the Hopsworks TikTok RecSys Demo

# Original Source: https://github.com/davitbzh/tiktok-recsys/blob/main/python/Jupyter/features/interactions.py

from mimesis import Generic
from mimesis.locales import Locale
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import config
import numpy as np


def generate_interactions(
    num_interactions: int, users: List[Dict[str, str]], videos: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Generate a list of dictionaries, each representing an interaction between a user and a video.

    This function creates interaction data by randomly pairing users with videos and assigning
    interaction details like interaction type, watch time, and whether the video was watched till the end.
    The likelihood of a video being watched till the end is inversely proportional to its length.

    Args:
        num_interactions (int): The number of interactions to generate.
        users (List[Dict[str, str]]): A list of dictionaries, where each dictionary contains user data.
        videos (List[Dict[str, str]]): A list of dictionaries, where each dictionary contains video data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing interaction data.
    """
    generic = Generic(locale=Locale.EN)
    interactions = []  # List to store generated interaction data

    current_time = datetime.now()

    for _ in range(num_interactions):
        user = random.choice(users)
        video = random.choice(videos)

        # Parse dates from strings
        user_registration_date = datetime.strptime(
            user["registration_date"], config.DATE_TIME_FORMAT
        )
        video_upload_date = datetime.strptime(
            video["upload_date"], config.DATE_TIME_FORMAT
        )

        # Determine the earliest possible date for the interaction
        earliest_date = max(user_registration_date, video_upload_date)

        # Generate a random date for the interaction
        days_since_earliest = (datetime.now() - earliest_date).days
        random_days = random.randint(0, days_since_earliest)

        # interaction_date = earliest_date + timedelta(days=random_days)
        random_delay = timedelta(seconds=random.randint(0, 600))
        interaction_date = current_time + random_delay

        previous_interaction_date = interaction_date - timedelta(
            days=random.randint(0, random.randint(0, 90))
        )

        interaction_types = ["like", "dislike", "view", "comment", "share", "skip"]
        weights = [1.5, 0.2, 3, 0.5, 0.8, 10]

        # Generate watch time and determine if the video was watched till the end
        watch_time = random.randint(1, video["video_length"])

        probability_watched_till_end = 1 - (watch_time / video["video_length"])
        watched_till_end = random.random() < probability_watched_till_end

        if watched_till_end:
            watch_time = video[
                "video_length"
            ]  # Adjust watch time to video length if watched till the end

        # Constructing the interaction dictionary
        interaction = {
            "interaction_id": int(generic.person.identifier(mask="##########")),
            "user_id": user["user_id"],
            "video_id": video["video_id"],
            "category_id": video["category_id"],
            "interaction_type": random.choices(interaction_types, weights=weights, k=1)[
                0
            ],
            "watch_time": watch_time,
            "interaction_date": interaction_date.strftime(config.DATE_TIME_FORMAT),
            "previous_interaction_date": previous_interaction_date.strftime(
                config.DATE_TIME_FORMAT
            ),
            "interaction_month": interaction_date.strftime(config.MONTH_FORMAT),
        }

        interactions.append(interaction)  # Add the interaction to the list
        current_time = current_time + timedelta(seconds=1)

    return interactions


def generate_user_interactions_window_agg(
    num_interactions: int, users: List[Dict[str, str]], videos: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Generate a list of dictionaries, each representing an interaction between a user and a video.

    This function creates interaction data by randomly pairing users with videos and assigning
    interaction details like interaction type, watch time, and whether the video was watched till the end.
    The likelihood of a video being watched till the end is inversely proportional to its length.

    Args:
        num_interactions (int): The number of interactions to generate.
        users (List[Dict[str, str]]): A list of dictionaries, where each dictionary contains user data.
        videos (List[Dict[str, str]]): A list of dictionaries, where each dictionary contains video data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing interaction data.
    """
    generic = Generic(locale=Locale.EN)
    interactions = []  # List to store generated interaction data

    for _ in range(num_interactions):
        user = random.choice(users)
        video = random.choice(videos)

        # Parse dates from strings
        user_registration_date = datetime.strptime(
            user["registration_date"], config.DATE_TIME_FORMAT
        )
        video_upload_date = datetime.strptime(
            video["upload_date"], config.DATE_TIME_FORMAT
        )

        # Determine the earliest possible date for the interaction
        earliest_date = max(user_registration_date, video_upload_date)

        # Generate  interaction
        interaction_types = ["like", "dislike", "view", "comment", "share", "skip"]
        weights = [1.5, 0.2, 3, 0.5, 0.8, 10]

        # Constructing the interaction dictionary
        interaction_date = video_upload_date + timedelta(hours=random.randint(0, 100))
        interaction = {
            "user_id": user["user_id"],
            "category_id": video["category_id"],
            "window_end_time": interaction_date.strftime(config.DATE_TIME_FORMAT),
            "interaction_month": interaction_date.strftime(config.MONTH_FORMAT),
            "like_count": random.randint(0, 100),
            "dislike_count": random.randint(0, 100),
            "view_count": random.randint(0, 100),
            "comment_count": random.randint(0, 100),
            "share_count": random.randint(0, 100),
            "skip_count": random.randint(0, 100),
            "total_watch_time": random.randint(0, 100),
        }

        interactions.append(interaction)  # Add the interaction to the list

    return interactions


def generate_video_interactions_window_agg(
    num_interactions: int, users: List[Dict[str, str]], videos: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Generate a list of dictionaries, each representing an interaction between a user and a video.

    This function creates interaction data by randomly pairing users with videos and assigning
    interaction details like interaction type, watch time, and whether the video was watched till the end.
    The likelihood of a video being watched till the end is inversely proportional to its length.

    Args:
        num_interactions (int): The number of interactions to generate.
        users (List[Dict[str, str]]): A list of dictionaries, where each dictionary contains user data.
        videos (List[Dict[str, str]]): A list of dictionaries, where each dictionary contains video data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing interaction data.
    """
    generic = Generic(locale=Locale.EN)
    interactions = []  # List to store generated interaction data

    for _ in range(num_interactions):
        user = random.choice(users)
        video = random.choice(videos)

        # Parse dates from strings
        user_registration_date = datetime.strptime(
            user["registration_date"], config.DATE_TIME_FORMAT
        )
        video_upload_date = datetime.strptime(
            video["upload_date"], config.DATE_TIME_FORMAT
        )

        # Determine the earliest possible date for the interaction
        earliest_date = max(user_registration_date, video_upload_date)

        # Generate  interaction
        interaction_types = ["like", "dislike", "view", "comment", "share", "skip"]
        weights = [1.5, 0.2, 3, 0.5, 0.8, 10]

        # Constructing the interaction dictionary
        interaction_date = video_upload_date + timedelta(hours=random.randint(0, 100))
        interaction = {
            "video_id": video["video_id"],
            "category_id": video["category_id"],
            "window_end_time": interaction_date.strftime(config.DATE_TIME_FORMAT),
            "interaction_month": interaction_date.strftime(config.MONTH_FORMAT),
            "like_count": random.randint(0, 100),
            "dislike_count": random.randint(0, 100),
            "view_count": random.randint(0, 100),
            "comment_count": random.randint(0, 100),
            "share_count": random.randint(0, 100),
            "skip_count": random.randint(0, 100),
            "total_watch_time": random.randint(0, 100),
        }

        interactions.append(interaction)  # Add the interaction to the list

    return interactions


# Calculate ondemand features the sine and cosine of the month of interaction date
def month_sine(interaction_date):
    # Calculate a coefficient for adjusting the periodicity of the month
    coef = np.random.uniform(0, 2 * np.pi) / 12

    # month_of_purchase = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S").month
    month_of_interaction = interaction_date.month

    # Calculate the sine and cosine components for the month_of_purchase
    return float(np.sin(month_of_interaction * coef))


def month_cosine(interaction_date):
    # Calculate a coefficient for adjusting the periodicity of the month
    coef = np.random.uniform(0, 2 * np.pi) / 12

    # month_of_purchase = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S").month
    month_of_interaction = interaction_date.month

    # Calculate the sine and cosine components for the month_of_purchase
    return float(np.cos(month_of_interaction * coef))
