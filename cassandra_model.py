import pydgraph
import uuid
import random
from faker import Faker
import json
import pydgraph
from itertools import combinations  


def set_schema(client):
    """Define the schema and types for the Dgraph database."""
    schema = """
        type User {
            user_id: uid
            name: string
            email: string
            daily_usage: float
            weekly_usage: float
            monthly_usage: float
            yearly_usage: float
            interests: [string]
            last_active: datetime
            clusters: [Cluster]
        }

        type Post {
            post_id: uid
            content: string
            content_length: int
            views: int
            likes: int
            comments: int
            shares: int
            engagement_count: int
            retention_time: float
            metric: string
            user: User
        }

        type Engagement {
            engagement_id: uid
            user: User
            post: Post
            timestamp: datetime
            type: string
        }

        type Trend {
            trend_id: uid
            day: int
            week: int
            month: int
            year: int
            engagement_count: int
            engagement_percentage: float
        }

        type Cluster {
            cluster_id: uid
            interest_keywords: [string]
            representative_users: [User]
        }

        type Metric {
            metric_id: uid
            name: string
            description: string
            top_post: Post
        }

        type Inactivity {
            user: User
            last_active: datetime
            inactivity_duration: int
        }

        user_id: uid .
        name: string @index(term) .
        email: string @index(exact) .
        daily_usage: float .
        weekly_usage: float .
        monthly_usage: float .
        yearly_usage: float .
        interests: [string] @index(term) .
        last_active: datetime @index(hour) .
        clusters: [uid] @reverse .
        post_id: uid .
        content: string @index(term) .
        content_length: int .
        views: int .
        likes: int .
        comments: int .
        shares: int .
        engagement_count: int .
        retention_time: float .
        metric: string @index(exact) .
        engagement_id: string .
        user: uid .  
        post: uid .  
        timestamp: datetime @index(hour) .
        type: string @index(exact) .
        trend_id: uid .
        day: int @index(int) .
        week: int @index(int) .
        month: int @index(int) .
        year: int @index(int) .
        engagement_percentage: float .
        cluster_id: uid .
        interest_keywords: [string] @index(term) .
        representative_users: [uid] .
        metric_id: uid .
        description: string .
        top_post: uid .
        inactivity_duration: int .
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)
    print("Schema with types set successfully.")


#1 Daily Usage Time
def analyze_platform_usage(client):
    """Analyze platform usage time."""
    query = """
        {
            usageStats(func: has(daily_usage)) {
                user_id
                name
                daily_usage
            }
        }
    """
    res = client.txn(read_only=True).query(query)
    print(res.json.decode("utf-8"))

#2 Daily Engagement Trend
def view_daily_engagement(client):
    # counts and sums engagement of posts from this day
    specific_day = 15  
    query = f"""
        {{
            dailyTrends(func: eq(day, {specific_day})) {{
                engagement_count_var as engagement_count
            }}
            totalEngagement() {{
                total_engagement: sum(val(engagement_count_var))
            }}
        }}
    """
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json.decode("utf-8"))
    if "totalEngagement" in data and data["totalEngagement"]:
        print(f"Engagement of day: {data['totalEngagement'][0]['total_engagement']}")
    else:
        print("No engagement data for the specified day.")

#3 Weekly Engagement Trend
def view_weekly_engagement(client):
    # counts and sums engagement of posts from this week
    specific_week = 10
    query = f"""
        {{
            weeklyTrends(func: eq(week, {specific_week})) {{
                engagement_count_var as engagement_count
            }}
            totalEngagement() {{
                total_engagement: sum(val(engagement_count_var))
            }}
        }}
    """
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json.decode("utf-8"))
    if "totalEngagement" in data and data["totalEngagement"]:
        print(f"Engagement of week: {data['totalEngagement'][0]['total_engagement']}")
    else:
        print("No engagement data for the specified week.")


#4 Monthly Engagement 
def view_monthly_engagement(client):
    # counts and sums engagement of posts from this month
    specific_month = 10
    query = f"""
        {{
            monthlyTrends(func: eq(month, {specific_month})) {{
                engagement_count_var as engagement_count
            }}
            totalEngagement() {{
                total_engagement: sum(val(engagement_count_var))
            }}
        }}
    """
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json.decode("utf-8"))
    if "totalEngagement" in data and data["totalEngagement"]:
        print(f"Engagement of month: {data['totalEngagement'][0]['total_engagement']}")
    else:
        print("No engagement data for the specified month.")

#5 Yearly Engagement
def view_yearly_engagement(client):
    # counts and sums engagement of posts from this year
    specific_year = 2024
    query = f"""
        {{
            yearlyTrends(func: eq(year, {specific_year})) {{
                engagement_count_var as engagement_count
            }}
            totalEngagement() {{
                total_engagement: sum(val(engagement_count_var))
            }}
        }}
    """
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json.decode("utf-8"))
    if "totalEngagement" in data and data["totalEngagement"]:
        print(f"Engagement of year: {data['totalEngagement'][0]['total_engagement']}")
    else:
        print("No engagement data for the specified year.")

#6 User Interest Clustering
def cluster_users_by_interests(client):
    # Query to retrieve interest clusters and users associated with them
    query = """
        {
            interestClusters(func: has(interest_keywords)) {
                interest_keywords
                representative_users {
                    uid  
                }
            }
        }
    """
    
    # Execute the query
    res = client.txn(read_only=True).query(query)
    
    # Decode the response from JSON, ensuring it's a valid JSON object
    try:
        data = json.loads(res.json.decode("utf-8"))
    except Exception as e:
        print(f"Error decoding JSON: {e}")
        return
    
    # Check if the response contains the 'interestClusters' key
    clusters = data.get('interestClusters', [])
    
    # Dictionary to group users by pairs of interests
    interest_groups = {}
    
    # Loop through the clusters to create pairs of interests
    for cluster in clusters:
        interests = cluster.get('interest_keywords', [])
        
        # Generate all combinations of 2 interests
        for pair in combinations(sorted(interests), 2):
            if pair not in interest_groups:
                interest_groups[pair] = []
            
            # Add user IDs to the interest pair
            for user in cluster.get('representative_users', []):
                user_id = user.get('uid')  
                if user_id:
                    interest_groups[pair].append(user_id)
    
    # Print the formatted results
    print("User Interest Clusters (Pairs of Interests):\n")
    
    for interests, user_ids in interest_groups.items():
        print(f"Cluster ({', '.join(interests)}):")
        if user_ids:
            for user_id in user_ids:
                print(f"    - User ID: {user_id}")
        else:
            print("    No users found in this cluster.")
        print()




#7 Inactive User identification
def identify_inactive_users(client):
    # shows user that have not been active since a certain time
    query = """
        {
            inactiveUsers(func: le(last_active, "2024-05-28T00:00:00Z")) {
                user_id
                name
                last_active
            }
        }
    """
    res = client.txn(read_only=True).query(query)
    print(res.json.decode("utf-8"))

#8 Post Retention Analysis
def analyze_post_retention(client):
    """Analyze post retention."""
    query = """
        {
            retentionAnalysis(func: has(retention_time)) {
                post_id
                retention_time
                engagement_count
            }
        }
    """
    res = client.txn(read_only=True).query(query)
    print(res.json.decode("utf-8"))

#9 Top Performing Post
def find_top_performing_post(client):
    """Find top-performing post by metric."""
    query = """
        {
            topPosts(func: has(metric), orderdesc: engagement_count, first: 1) {
                post_id
                content
                metric
                engagement_count
            }
        }
    """
    res = client.txn(read_only=True).query(query)
    print(res.json.decode("utf-8"))



fake = Faker()

#10 create sample data
def populate_database(client):
    """Populate the Dgraph database with sample data for testing."""
    
    users = []
    posts = []
    clusters = []
    
    # Create fake users
    for _ in range(20):
        user_id = '_:{}'.format(uuid.uuid4()) 
        users.append(user_id)
        name = fake.name()
        email = fake.email()
        daily_usage = random.uniform(0.5, 5.0)
        weekly_usage = random.uniform(3.5, 35.0)
        monthly_usage = random.uniform(15.0, 150.0)
        yearly_usage = random.uniform(50.0, 500.0)
        interests = random.sample(['sports', 'music', 'technology', 'travel', 'food', 'movies'], 3)
        last_active = fake.date_this_year()

        p = {
            'uid': user_id,
            'name': name,
            'email': email,
            'daily_usage': daily_usage,
            'weekly_usage': weekly_usage,
            'monthly_usage': monthly_usage,
            'yearly_usage': yearly_usage,
            'interests': interests,
            'last_active': last_active.isoformat(),  
        }

        txn = client.txn()
        try:
            txn.mutate(set_obj=p)
            txn.commit()
        finally:
            txn.discard()

    # Create fake posts
    for _ in range(50):
        post_id = '_:{}'.format(uuid.uuid4())  
        posts.append(post_id)
        user_id = random.choice(users)
        content = fake.text(max_nb_chars=200)
        content_length = len(content)
        views = random.randint(0, 1000)
        likes = random.randint(0, 500)
        comments = random.randint(0, 200)
        shares = random.randint(0, 100)
        engagement_count = likes + comments + shares
        retention_time = random.uniform(1.0, 5.0)
        metric = random.choice(['likes', 'shares', 'comments'])

        p = {
            'uid': post_id,
            'content': content,
            'content_length': content_length,
            'views': views,
            'likes': likes,
            'comments': comments,
            'shares': shares,
            'engagement_count': engagement_count,
            'retention_time': retention_time,
            'metric': metric,
            'user': {'uid': user_id}
        }

        txn = client.txn()
        try:
            txn.mutate(set_obj=p)
            txn.commit()
        finally:
            txn.discard()

    # Create fake clusters based on user interests
    for _ in range(4):
        cluster_id = '_:{}'.format(uuid.uuid4())  
        clusters.append(cluster_id)
        interest_keywords = random.sample(['sports', 'music', 'technology', 'travel', 'food', 'movies'], 2)
        representative_users = random.sample(users, 10)

        p = {
            'uid': cluster_id,
            'interest_keywords': interest_keywords,
            'representative_users': [{'uid': user} for user in representative_users]
        }

        txn = client.txn()
        try:
            txn.mutate(set_obj=p)
            txn.commit()
        finally:
            txn.discard()

    # Create fake trend data
    for _ in range(30):  # 30 days, 30 weeks, etc.
        trend_id = '_:{}'.format(uuid.uuid4())
        day = random.randint(1, 31)  
        week = random.randint(1, 52)  
        month = random.randint(1, 12)  
        year = random.randint(2023, 2024)
        engagement_count = random.randint(50, 500)
        engagement_percentage = round(random.uniform(0.1, 100), 2) 

        trend = {
            'uid': trend_id,
            'day': day,
            'week': week,
            'month': month,
            'year': year,
            'engagement_count': engagement_count,
            'engagement_percentage': engagement_percentage
        }

        txn = client.txn()
        try:
            txn.mutate(set_obj=trend)
            txn.commit()
        finally:
            txn.discard()


    print("Sample data has been inserted into the database.")