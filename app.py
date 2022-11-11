from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text

def create_app(test_config=None):
    app = Flask(__name__)

    if test_config is None:
        app.config.from_pyfile("config.py")
    else:
        app.config.update(test_config)

    database = create_engine(app.config['DB_URL'], encoding='utf-8', max_overflow=0)
    app.database=database

    @app.route("/sign-up", methods=['POST'])
    def sign_up():
        new_user = request.json
        new_user_id = app.database.execute(text("""
            INSERT INTO users (
                name,
                email,
                profile,
                hashed_password
            ) VALUES (
                :name,
                :email,
                :profile,
                :password
            )
        """), new_user). lastrowid

        row = current_app.database.execute(text("""
            SELECT
                id,
                name,
                email,
                profile,
            FROM
                users
            WHERE id = :user_id
        """), {
            'user_id': new_user_id
        }).fetchone()

        created_user = {
            'id': row['id'],
            'name': row['name'],
            'email': row['email'],
            'profile': row['profile']
        } if row else None
        return jsonify(created_user)


    @app.route('/tweet', methods=['POST'])
    def tweet():
        user_tweet = request.json
        tweet = user_tweet['tweet']

        if len(tweet) > 300:
            return "Content length can't exceed 300 words limit", 400

        app.database.execute(text("""
            INSERT INTO tweets (
                user_id,
                tweet
            ) VALUES (
                :id,
                :tweet
            )
        """), user_tweet)

        return '', 200



    return app