# This code is borrowed from the Hopsworks TikTok RecSys Demo

# Original Source: https://github.com/davitbzh/tiktok-recsys/blob/main/python/Jupyter/features/users.py

from mimesis import Generic
from mimesis.locales import Locale
import random
from datetime import datetime, timedelta
from typing import List, Dict
import config


def generate_users(num_users: int, historical=False) -> List[Dict[str, str]]:
    """
    Generate a list of dictionaries, each representing a user with various attributes.

    The function creates fake user data including user ID, gender, age, and country
    using the mimesis library. The user ID is generated based on a specified mask.

    Args:
        num_users (int): The number of user profiles to generate.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each containing details of a user.
    """
    generic = Generic(locale=Locale.EN)
    users = []  # List to store generated user data

    for _ in range(num_users):
        if historical:
            days_ago = random.randint(
                0, 730
            )  # Choose a random number of days up to two years
            registration_date = datetime.now() - timedelta(
                days=days_ago
            )  # Compute the date of registration
        else:
            registration_date = datetime.now()

            # Generate each user's details
        user = {
            "user_id": int(
                generic.person.identifier(mask="#####")
            ),  # Unique user identifier
            "gender": generic.person.gender(),  # Randomly generated gender
            "age": random.randint(12, 90),  # Randomly generated age between 12 and 90
            "country": generic.address.country(),  # Randomly generated country name
            "registration_date": registration_date.strftime(config.DATE_TIME_FORMAT),
            "registration_month": registration_date.strftime(config.MONTH_FORMAT),
        }
        users.append(user)  # Add the user to the list

    return users
