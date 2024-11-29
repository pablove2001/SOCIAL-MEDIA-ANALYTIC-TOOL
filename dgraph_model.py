import pydgraph


class DgraphModel:
    def __init__(self, host="localhost:9080"):
        self.client_stub = None
        self.client = None
        self.host = host

    def connect_to_dgraph(self):
        self.client_stub = pydgraph.DgraphClientStub(self.host)
        self.client = pydgraph.DgraphClient(self.client_stub)
        print("Connected to Dgraph.")

    def close_connection(self):
        if self.client_stub:
            self.client_stub.close()
            print("Connection to Dgraph closed.")

    def set_schema(self):
        schema = """
            type User {
                user_id: string
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
                post_id: string
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
                engagement_id: string
                user: User
                post: Post
                timestamp: datetime
                type: string
            }
            type Trend {
                trend_id: string
                day: int
                week: int
                month: int
                year: int
                engagement_count: int
                engagement_percentage: float
            }
            type Cluster {
                cluster_id: string
                interest_keywords: [string]
                representative_users: [User]
            }
            type Metric {
                metric_id: string
                name: string
                description: string
                top_post: Post
            }
            type Inactivity {
                user: User
                last_active: datetime
                inactivity_duration: int
            }

            user_id: string @index(hash) .
            name: string @index(term) .
            email: string @index(exact) .
            daily_usage: float .
            weekly_usage: float .
            monthly_usage: float .
            yearly_usage: float .
            interests: [string] @index(term) .
            last_active: datetime @index(hour) .
            clusters: [uid] @reverse .
            post_id: string @index(hash) .
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
            trend_id: string .
            day: int @index(int) .
            week: int @index(int) .
            month: int @index(int) .
            year: int @index(int) .
            engagement_percentage: float .
            cluster_id: string .
            interest_keywords: [string] @index(term) .
            representative_users: [uid] .
            metric_id: string .
            description: string .
            top_post: uid .
            inactivity_duration: int .
        """
        op = pydgraph.Operation(schema=schema)
        self.client.alter(op)
        print("Schema with types set successfully.")

    def analyze_platform_usage(client):
        """Analyze platform usage time."""
        query = """
            {
                usageStats(func: has(daily_usage)) {
                    user_id
                    name
                    daily_usage
                    weekly_usage
                    monthly_usage
                    yearly_usage
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))

    def view_daily_engagement_trends(client):
        """View daily engagement trends."""
        query = """
            {
                dailyTrends(func: has(day)) {
                    day: day
                    total_engagement: count(uid)
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))

    def view_weekly_engagement_trends(client):
        """View weekly engagement trends."""
        query = """
            {
                weeklyTrends(func: has(week)) @groupby(week) {
                    count: count(uid)
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))

    def view_monthly_engagement_trends(client):
        """View monthly engagement trends."""
        query = """
            {
                monthlyTrends(func: has(month)) @groupby(month) {
                    count: count(uid)
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))

    def view_yearly_engagement_trends(client):
        """View yearly engagement trends."""
        query = """
            {
                yearlyTrends(func: has(year)) @groupby(year) {
                    count: count(uid)
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))

    def cluster_users_by_interests(client):
        """Cluster users by interests."""
        query = """
            {
                interestClusters(func: has(interests)) {
                    interest_keywords: interests
                    users: ~clusters {
                        user_id
                        name
                    }
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))

    def identify_inactive_users(client):
        """Identify inactive users."""
        query = """
            {
                inactiveUsers(func: le(last_active, "2024-01-01T00:00:00Z")) {
                    user_id
                    name
                    last_active
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))

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

    def find_top_performing_post(client):
        """Find top-performing post by metric."""
        query = """
            {
                topPosts(func: has(metric), orderdesc: engagement_count, first: 1) {
                    post_id
                    metric
                    engagement_count
                }
            }
        """
        res = client.txn(read_only=True).query(query)
        print(res.json.decode("utf-8"))
