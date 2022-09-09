#!/usr/bin/env python3
# Firebase admin tool
# prune anonymous auth users left over from site visitors who didn't sign up
# But only if they haven't been back for 90 days

from pprint import pprint
from time import sleep
import os
import json
import sys
import logging
from datetime import datetime, timedelta
import click
from firebase_admin import auth, initialize_app, exceptions

logging.basicConfig(level=logging.INFO)
# Used if running as a Cloud Run Job
TASK_INDEX = os.getenv('CLOUD_TASK_INDEX', '0')
TASK_ATTEMPT = os.getenv('CLOUD_TASK_ATTEMPT', '0')


def filter_anon(user: auth.UserRecord, expiry_date: datetime) -> str:
    """Return a list of users who are past their sell-by date.

    To qualify, they must
        - be anonymous (no email or phone #)
        - have existed longer than expiry_date
        - have not been to the site since expiry_date
    """
    if (
        not any([user.email, user.phone_number])
        and datetime.fromtimestamp(
            user.user_metadata.creation_timestamp // 1000
        )
        < expiry_date
        and (
            user.user_metadata.last_refresh_timestamp is None
            or datetime.fromtimestamp(
                user.user_metadata.last_refresh_timestamp // 1000
            )
            < expiry_date
        )
    ):
        return user.uid


def delete_users(uuids: list, kind: str = '') -> int:
    # Delete stuff in chunks
    users_deleted_count = 0
    try:
        result = auth.delete_users(uids=uuids)
        logging.info(
            f'Successfully deleted {result.success_count} {kind} users'
        )
        users_deleted_count += result.success_count
        if result.failure_count:
            logging.info(f'Failed to delete {result.failure_count} users')
        for err in result.errors:
            logging.error(f'error #{result.index}, reason: {result.reason}')
    except exceptions.InvalidArgumentError as e:
        logging.warning(e)
        logging.warning('Pausing for 60 seconds before retrying')
        sleep(60)
    return users_deleted_count


@click.command(no_args_is_help=True)
@click.option(
    '--shelflife',
    '-s',
    default=90,
    help="Remove anon users who haven't been on the site in this many days.",
    type=int,
)
@click.option(
    '--dry-run',
    'DEBUG',
    flag_value=True,
    default=False,
    help='Print results without changing database.',
)
@click.option(
    '--anon',
    '-a',
    'PRUNE_ANON',
    flag_value=True,
    default=False,
    help='Prune anonymous users (no email or phone number).',
)
@click.option(
    '--test-addresses',
    '-t',
    default='',
    type=str,
    nargs=1,
    help='Address for which you want to prune all email+xxx@nocapshows.com tester accounts',
)
def prune_anon(
    shelflife: int = 90,
    test_addresses: str = '',
    *,
    PRUNE_ANON: bool = False,
    DEBUG: bool = True,
) -> int:
    """Remove expired users from the Authentication database."""
    # Initialize Firebase from env
    default_app = initialize_app()
    logging.info(
        f'Cleaning up firebase auth users from {default_app.project_id}'
    )

    expiry_date: datetime = datetime.now() - timedelta(days=shelflife)

    page = auth.list_users()
    total_users_deleted: int = 0
    while page:
        # Did we request to delete test accounts with '+' in them?
        if test_addresses:
            test_user_uids = [
                userid
                for userid in [
                    user.uid
                    for user in page.users
                    if user.email
                    and user.email.startswith(
                        f'{test_addresses.split("@",1)[0]}+'
                    )
                ]
            ]
            if test_user_uids:
                if DEBUG:
                    logging.info(
                        f'dry-run: Would have deleted {len(test_user_uids)} test users of {test_addresses}'
                    )
                else:
                    logging.info(
                        f'Removing test users with "+" sign in email address {test_addresses}'
                    )
                    # pprint(test_user_uids)
                    total_users_deleted += delete_users(test_user_uids, 'test')
            else:
                logging.info(
                    f'No test users for {test_addresses} in this chunk'
                )

        # Are we pruning stale anonymous users?
        if PRUNE_ANON:
            anon_user_uids = [
                userid
                for userid in [
                    filter_anon(user, expiry_date) for user in page.users
                ]
                if userid is not None
            ]
            if anon_user_uids:
                if DEBUG:
                    logging.info(
                        f'dry-run: Would have deleted {len(anon_user_uids)} anonymous users'
                    )
                else:
                    total_users_deleted += delete_users(
                        anon_user_uids, 'stale anonymous'
                    )
            else:
                logging.info('No anonymous users stale enough to delete')

        page = page.get_next_page()  # get the next 1000 users
    return total_users_deleted


if __name__ == '__main__':
    try:
        logging.info(f'{prune_anon()} users deleted')
    except Exception as err:
        message = (
            f'Task #{TASK_INDEX}, Attempt #{TASK_ATTEMPT} failed: {str(err)}'
        )

        logging.error(json.dumps({'message': message, 'severity': 'ERROR'}))
        sys.exit(1)  # Retry Job Task by exiting the process
