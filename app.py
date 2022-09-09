from collections import namedtuple
from pprint import pprint
from flask import Flask, render_template, request
from firebase_admin import auth, initialize_app, exceptions
#import logging

from prune import delete_users


app = Flask(__name__)
# Initialize Firebase from env
default_app = initialize_app()

AuthUser = namedtuple('AuthUser', 'uid email')


@app.get('/user/<username>')
def list_testers(username):  # put application's code here

    app.logger.info(
        f'Cleaning up firebase auth users from {default_app.project_id} for {username}'
    )   

    user_uids: list = []

    page = auth.list_users()
    while page:
        total_users_deleted: int = 0
        test_user_uids = [
            userid
            for userid in [
                AuthUser(user.uid, user.email)
                for user in page.users
                if user.email
                and user.email.startswith(f'{username.split("@",1)[0]}+')
            ]
        ]
        if test_user_uids:
            pprint(test_user_uids)
        user_uids += test_user_uids
        page = page.get_next_page()  # get the next 1000 users
    
    return render_template('users.html', users=user_uids)

@app.post('/user/delete/uuids')
def delete_uuids():
    uuids = [
        key for key in request.form.keys() if request.form[key] == 'on'
    ]

    for uuid in uuids:
        app.logger.info(f"Deleting {uuid}...")
    if uuids:
        delete_users(uuids=uuids, kind="tester")
    return f"Deleting {len(uuids)} tester account{'s' if len(uuids) > 1 or len(uuids) == 0 else ''}..."


if __name__ == '__main__':
    app.run()
