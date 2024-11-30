from collections import defaultdict
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from textblob import TextBlob
import uuid
from datetime import datetime, timedelta
from faker import Faker
import random


class CassandraModel:
    def __init__(self):
        self.cluster = None
        self.session = None

    def connect_to_cassandra(self):
        self.cluster = Cluster(["127.0.0.1"])
        self.session = self.cluster.connect()
        print("Connected to Cassandra.")
        self.setup_keyspace_and_tables()

    def setup_keyspace_and_tables(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS social_media
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        self.session.set_keyspace('social_media')
        print("Keyspace set to 'social_media'.")
        self.create_tables()

    def create_tables(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                joined_date TIMESTAMP,
                followers_count INT,
                following_count INT
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                post_id UUID PRIMARY KEY,
                user_id UUID,
                content TEXT,
                timestamp TIMESTAMP,
                like_count INT,
                comment_count INT,
                share_count INT,
                tags SET<TEXT>
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS user_posts (
                user_id UUID,
                post_id UUID,
                content TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                comment_id UUID PRIMARY KEY,
                post_id UUID,
                user_id UUID,
                content TEXT,
                timestamp TIMESTAMP
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS post_comments (
                post_id UUID,
                comment_id UUID,
                user_id UUID,
                content TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (post_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS likes (
                post_id UUID,
                user_id UUID,
                timestamp TIMESTAMP,
                PRIMARY KEY (post_id, user_id)
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                tag TEXT PRIMARY KEY,
                post_count INT,
                last_used TIMESTAMP
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS user_activity (
                user_id UUID,
                activity_id UUID,
                type TEXT,
                target_id UUID,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS shares (
                post_id UUID,
                user_id UUID,
                timestamp TIMESTAMP,
                PRIMARY KEY (post_id, user_id)
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS posts_by_tag (
                tag TEXT,
                post_id UUID,
                like_count INT,
                share_count INT,
                comment_count INT,
                PRIMARY KEY (tag, post_id)
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS tag_popularity (
                tag TEXT,
                post_count INT,
                PRIMARY KEY (tag, post_count)
            ) WITH CLUSTERING ORDER BY (post_count DESC);
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS top_shared_posts (
            share_count INT,
            post_id UUID,
            PRIMARY KEY (share_count, post_id)
        ) WITH CLUSTERING ORDER BY (post_id DESC);
        """)

        print("Tables created.")

    # Logic for cassandra requirements (1-11)

    # 1. Follower Number Analysis
    def follower_number_analysis(self):
        print("Executing Follower Number Analysis...")
        query = "SELECT user_id, followers_count, following_count FROM users"
        rows = self.session.execute(query)
        for row in rows:
            print(f"User ID: {row.user_id}, Followers: {row.followers_count}, Following: {row.following_count}")

    # 2. User Interaction Patterns by Time of Day
    def user_interaction_patterns(self):
        print("Analyzing User Interaction Patterns by Time of Day...")
        query = "SELECT user_id, activity_id, type, timestamp FROM user_activity"
        rows = self.session.execute(query)
        rows_list = list(rows)
        print(f"Rows returned: {len(rows_list)}")

        for row in rows_list:
            print(f"User ID: {row.user_id}, Type: {row.type}, Timestamp: {row.timestamp}")

    # 3. Trend Analysis of Popular Topics
    def trend_analysis_of_topics(self):
        print("Analyzing Trending Topics...")
        query = "SELECT tag, post_count FROM tag_popularity LIMIT 10"
        rows = self.session.execute(query)
        for row in rows:
            print(f"Tag: {row.tag}, Post Count: {row.post_count}")

    # 4. User Sentiment Analysis
    def user_sentiment_analysis(self):
        query = "SELECT post_id, content FROM social_media.posts;"
        rows = self.session.execute(query)

        sentiment_results = []

        for row in rows:
            content = row.content
            sentiment_score = TextBlob(content).sentiment.polarity  # Polarity: -1 to 1
            sentiment = "positive" if sentiment_score > 0 else "negative" if sentiment_score < 0 else "neutral"

            sentiment_results.append({
                'post_id': row.post_id,
                'content': content,
                'sentiment': sentiment
            })

            print(f"Post ID: {row.post_id}, Sentiment: {sentiment}")

        return sentiment_results

    # 5. Content Type Performance Analysis
    def content_type_performance(self):
        print("Analyzing Content Type Performance...")
        query = "SELECT user_id, COUNT(*) AS post_count FROM user_posts GROUP BY user_id"
        rows = self.session.execute(query)
        for row in rows:
            print(f"User ID: {row.user_id}, Post Count: {row.post_count}")

    # 6. Most Engaging Post Types for Specific Hashtags
    def most_engaging_post_types(self, example_tag):
        print(f"Finding Most Engaging Posts for Tag: {example_tag}")
        query = """
            SELECT post_id, like_count, share_count, comment_count
            FROM posts_by_tag
            WHERE tag = %s;
        """
        rows = self.session.execute(query, (example_tag,))
        for row in rows:
            total_engagement = (row.like_count or 0) + (row.share_count or 0) + (row.comment_count or 0)
            print(f"Post ID: {row.post_id}, Total Engagement: {total_engagement}")

    # 7. Keyword Influence on Engagement
    def keyword_influence_on_engagement(self):
        print("Analyzing Keyword Influence on Engagement...")

        keyword_engagement = defaultdict(lambda: {"likes": 0, "comments": 0, "shares": 0, "count": 0})

        posts_query = "SELECT post_id, content FROM social_media.posts;"
        posts = self.session.execute(posts_query)

        for post in posts:
            post_id = post.post_id
            content = post.content
            keywords = set(content.split())  # Split content into words for simplicity

            engagement_query = """
            SELECT like_count, comment_count, share_count
            FROM social_media.posts
            WHERE post_id = %s;
            """
            engagement_data = self.session.execute(engagement_query, (post_id,)).one()

            if engagement_data:
                like_count, comment_count, share_count = engagement_data.like_count, engagement_data.comment_count, engagement_data.share_count
                for keyword in keywords:
                    keyword_engagement[keyword]["likes"] += like_count
                    keyword_engagement[keyword]["comments"] += comment_count
                    keyword_engagement[keyword]["shares"] += share_count
                    keyword_engagement[keyword]["count"] += 1

        keyword_averages = {}
        for keyword, data in keyword_engagement.items():
            if data["count"] > 0:
                keyword_averages[keyword] = {
                    "avg_likes": data["likes"] / data["count"],
                    "avg_comments": data["comments"] / data["count"],
                    "avg_shares": data["shares"] / data["count"]
                }

        sorted_keywords = sorted(keyword_averages.items(), key=lambda x: x[1]["avg_likes"], reverse=True)

        print("Top 10 keywords influencing engagement (by average likes):")
        for keyword, averages in sorted_keywords[:10]:
            print(
                f"Keyword: {keyword}, Avg Likes: {averages['avg_likes']:.2f}, Avg Comments: {averages['avg_comments']:.2f}, Avg Shares: {averages['avg_shares']:.2f}")

    # 8. Follower-to-Engagement Ratio
    def follower_to_engagement_ratio(self):
        print("Calculating Follower-to-Engagement Ratios...")

        query = "SELECT user_id, like_count, comment_count FROM posts"
        rows = self.session.execute(query)

        user_engagement = {}
        for row in rows:
            if row.user_id not in user_engagement:
                user_engagement[row.user_id] = 0
            user_engagement[row.user_id] += row.like_count + row.comment_count

        followers_query = "SELECT user_id, followers_count FROM users"
        followers_rows = {row.user_id: row.followers_count for row in self.session.execute(followers_query)}

        for user_id, total_engagement in user_engagement.items():
            followers_count = followers_rows.get(user_id, 0)  # Default to 0 if no followers count found
            ratio = total_engagement / followers_count if followers_count else 0
            print(f"User ID: {user_id}, Ratio: {ratio:.2f}")

    # 9. Average Response Time to Comments
    def average_response_time_to_comments(self):
        print("Calculating Average Response Time to Comments...")

        posts_query = """
            SELECT post_id, user_id, timestamp as post_timestamp
            FROM social_media.posts;
        """
        posts = self.session.execute(posts_query)

        for post in posts:
            comments_query = """
                SELECT comment_id, timestamp as comment_timestamp
                FROM social_media.post_comments
                WHERE post_id = %s
            """
            comments = self.session.execute(comments_query, [post.post_id])
            comments_list = list(comments)

            if comments_list:
                total_response_time = timedelta()
                comment_count = 0

                for comment in comments_list:
                    response_time = comment.comment_timestamp - post.post_timestamp
                    if response_time.total_seconds() > 0:
                        total_response_time += response_time
                        comment_count += 1

                if comment_count > 0:
                    avg_response_time = total_response_time / comment_count

                    total_seconds = int(avg_response_time.total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60

                    print(f"Post ID: {post.post_id}")
                    print(f"Number of comments: {comment_count}")
                    print(f"Average response time: {hours}h {minutes}m {seconds}s")
                    print("---")

    # 10. Time-to-First-Engagement Analysis
    def time_to_first_engagement(self):
        print("Analyzing Time-to-First-Engagement...")
        query = """
            SELECT post_id, MIN(timestamp) AS first_engagement_time FROM likes GROUP BY post_id;
        """
        rows = self.session.execute(query)
        for row in rows:
            print(f"Post ID: {row.post_id}, First Engagement Time: {row.first_engagement_time}")

    # 11. Top Shared Posts
    def top_shared_posts(self):
        print("Finding Top Shared Posts...")

        query = """
            SELECT post_id, share_count 
            FROM posts 
            WHERE share_count > 0 
            ALLOW FILTERING
        """
        try:
            rows = self.session.execute(query)
            posts = list(rows)
            posts.sort(key=lambda x: x.share_count, reverse=True)
            if not posts:
                print("No shared posts found.")
            else:
                print("\nTop Shared Posts:")
                for post in posts[:10]:
                    print(f"Post ID: {post.post_id}, Share Count: {post.share_count}")

        except Exception as e:
            print(f"Error retrieving top shared posts: {str(e)}")

    # 12. Video Completion Rate
    def video_completion_rate(self):
        # Not sure how to create this method, will most like work better with Dghraph
        print("Video Completion Rate Analysis:.")

    def close_connection(self):
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        print("Connection to Cassandra closed.")

    def populate_database(self):
        fake = Faker()

        users = []
        posts = []
        # Creates fake users
        for _ in range(50):
            user_id = uuid.uuid4()
            users.append(user_id)
            username = fake.user_name()
            email = fake.email()
            joined_date = fake.date_time_this_decade()
            followers_count = random.randint(0, 5000)
            following_count = random.randint(0, 2000)

            query = """
            INSERT INTO social_media.users (user_id, username, email, joined_date, followers_count, following_count)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            self.session.execute(query, (user_id, username, email, joined_date, followers_count, following_count))

        # Creates posts and user_posts
        for _ in range(100):
            post_id = uuid.uuid4()
            posts.append(post_id)
            user_id = random.choice(users)
            content = fake.text(max_nb_chars=200)
            timestamp = fake.date_time_this_year()
            like_count = random.randint(0, 1000)
            comment_count = random.randint(0, 500)
            share_count = random.randint(0, 200)
            tags = set(fake.words(nb=random.randint(1, 5)))

            # Insert posts table
            query = """
            INSERT INTO social_media.posts (post_id, user_id, content, timestamp, like_count, comment_count, share_count, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            self.session.execute(query,
                                 (post_id, user_id, content, timestamp, like_count, comment_count, share_count, tags))

            # Insert user_posts table
            query = """
            INSERT INTO social_media.user_posts (post_id, user_id, content, timestamp)
            VALUES (%s, %s, %s, %s);
            """
            self.session.execute(query, (post_id, user_id, content, timestamp))

            # Insert top_shared_posts table (this isn't totally needed, since I changed the top shared posts logic.
            # Although this would work better in theory.)
            query = """
            INSERT INTO social_media.top_shared_posts (share_count, post_id)
            VALUES (%s, %s);
            """
            self.session.execute(query, (share_count, post_id))

            # Creates tag_popularity data
            for tag in tags:
                post_count = random.randint(1, 20)
                query = """
                INSERT INTO social_media.tag_popularity (post_count, tag)
                VALUES (%s, %s);
                """
                self.session.execute(query, (post_count, tag))

            # Creates user activity table data (for posts)
            activity_id = uuid.uuid4()
            activity_type = "post"
            target_id = post_id
            timestamp = datetime.now()
            query = """
            INSERT INTO social_media.user_activity (user_id, activity_id, type, target_id, timestamp)
            VALUES (%s, %s, %s, %s, %s);
            """
            self.session.execute(query, (user_id, activity_id, activity_type, target_id, timestamp))

        # Creates comments and post_comments
        for post_id in posts:
            # Get post timestamp
            post_query = "SELECT timestamp FROM social_media.posts WHERE post_id = %s"
            post_timestamp = self.session.execute(post_query, [post_id]).one().timestamp

            # Generates 1-5 comments for each post
            for _ in range(random.randint(1, 5)):
                comment_id = uuid.uuid4()
                user_id = random.choice(users)
                content = fake.text(max_nb_chars=100)
                # Generates comment timestamp that's after the post timestamp, it'll be between 1 minute and 24 hours
                time_diff = random.randint(60, 86400)
                timestamp = post_timestamp + timedelta(seconds=time_diff)

                # Insert comments
                query = """
                INSERT INTO social_media.comments (comment_id, post_id, user_id, content, timestamp)
                VALUES (%s, %s, %s, %s, %s);
                """
                self.session.execute(query, (comment_id, post_id, user_id, content, timestamp))

                # Insert post_comments
                query = """
                INSERT INTO social_media.post_comments (post_id, comment_id, user_id, content, timestamp)
                VALUES (%s, %s, %s, %s, %s);
                """
                self.session.execute(query, (post_id, comment_id, user_id, content, timestamp))

                # Creates user activity data (for comments)
                activity_id = uuid.uuid4()
                activity_type = "comment"
                target_id = comment_id
                query = """
                INSERT INTO social_media.user_activity (user_id, activity_id, type, target_id, timestamp)
                VALUES (%s, %s, %s, %s, %s);
                """
                self.session.execute(query, (user_id, activity_id, activity_type, target_id, timestamp))

        # Creates likes
        for _ in range(300):
            post_id = random.choice(posts)
            user_id = random.choice(users)
            timestamp = fake.date_time_this_year()

            query = """
            INSERT INTO social_media.likes (post_id, user_id, timestamp)
            VALUES (%s, %s, %s);
            """
            self.session.execute(query, (post_id, user_id, timestamp))

            # Creates user activity data (for likes)
            activity_id = uuid.uuid4()
            activity_type = "like"
            target_id = post_id
            timestamp = datetime.now()
            query = """
            INSERT INTO social_media.user_activity (user_id, activity_id, type, target_id, timestamp)
            VALUES (%s, %s, %s, %s, %s);
            """
            self.session.execute(query, (user_id, activity_id, activity_type, target_id, timestamp))

        # Creates tags
        tags = set()
        for row in self.session.execute("SELECT tags FROM social_media.posts;"):
            tags.update(row.tags)

        for tag in tags:
            post_count = random.randint(1, 20)
            last_used = fake.date_time_this_year()

            query = """
            INSERT INTO social_media.tags (tag, post_count, last_used)
            VALUES (%s, %s, %s);
            """
            self.session.execute(query, (tag, post_count, last_used))

        print("Test data inserted into the database.")


