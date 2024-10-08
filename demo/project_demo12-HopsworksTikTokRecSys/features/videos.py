# This code is borrowed from the Hopsworks TikTok RecSys Demo

# Original Source: https://github.com/davitbzh/tiktok-recsys/blob/main/python/Jupyter/features/videos.py

from mimesis import Generic
from mimesis.locales import Locale
import random
from datetime import datetime, timedelta
from typing import List, Dict
import config


def generate_video_content(num_videos: int, historical=False) -> List[Dict[str, str]]:
    """
    Generate a list of dictionaries, each representing video content with various attributes.

    Each video includes details such as a unique video ID, category,
    video length in seconds, and the upload date. The function uses the mimesis library
    for generating random data and Python's random module for numerical attributes.

    Args:
        num_videos (int): The number of video entries to generate.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each containing details of a video.
    """
    generic = Generic(locale=Locale.EN)
    videos = []  # List to store generated video data

    for _ in range(num_videos):
        if historical:
            days_ago = random.randint(
                0, 730
            )  # Choose a random number of days up to two years
            upload_date = datetime.now() - timedelta(
                days=days_ago
            )  # Compute the upload date

        else:
            upload_date = datetime.now()

        categories = [
            "Education",
            "Entertainment",
            "Lifestyle",
            "Music",
            "News",
            "Sports",
            "Technology",
            "Dance",
            "Cooking",
            "Comedy",
            "Travel",
        ]
        categories_dict = {
            "Education": 1,
            "Entertainment": 2,
            "Lifestyle": 3,
            "Music": 4,
            "News": 5,
            "Sports": 6,
            "Technology": 7,
            "Dance": 8,
            "Cooking": 9,
            "Comedy": 10,
            "Travel": 11,
        }

        video_length_seconds = random.randint(10, 250)  # Video length in seconds
        video_category = random.choice(categories)

        video = {
            "video_id": int(
                generic.person.identifier(mask="#####")
            ),  # Unique video identifier
            "category_id": categories_dict[video_category],
            "category": video_category,
            "video_length": video_length_seconds,
            "upload_date": upload_date.strftime(config.DATE_TIME_FORMAT),
            "upload_month": upload_date.strftime(config.MONTH_FORMAT),
        }

        videos.append(video)  # Add the video to the list

    return videos
