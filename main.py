from mongo_model import MongoModel
from cassandra_model import CassandraModel
from dgraph_model import DgraphModel

def print_menu(current_username):
    print("\n=== Social Media Analytics System ===")
    print(f"MongoDB current user: {current_username}")
    print("1. Create user (MongoDB)")
    print("2. Delete current user (MongoDB)")
    print("3. Login (MongoDB)")
    print("4. Logout (MongoDB)")
    print("5. Change password (MongoDB)")
    print("6. Change notifications (MongoDB)")
    print("7. Change language (MongoDB)")
    print("8. Most used language (MongoDB)")
    print("9. Create a post (MongoDB)")
    print("10. Delete a post (MongoDB)")
    print("11. See your posts (MongoDB)")
    print("12. See posts from people (MongoDB)")
    print("13. Get list of users (MongoDB)")
    print("14. Populate database (MongoDB)")
    print("15. Clean database (MongoDB)")
    print("16. Follower Number Analysis (Cassandra)")
    print("17. User Interaction Patterns by Time of Day (Cassandra)")
    print("18. Trend Analysis of Popular Topics (Cassandra)")
    print("19. User Sentiment Analysis (Cassandra)")
    print("20. Content Type Performance Analysis (Cassandra)")
    print("21. Most Engaging Post Types for Specific Hashtags (Cassandra)")
    print("22. Keyword Influence on Engagement (Cassandra)")
    print("23. Follower-to-Engagement Ratio (Cassandra)")
    print("24. Average Response Time to Comments (Cassandra)")
    print("25. Time-to-First-Engagement Analysis (Cassandra)")
    print("26. Top Shared Posts (Cassandra)")
    print("27. Populate Cassandra Database (Cassandra)")
    print("28. Analyze Platform Usage (Dgraph)")
    print("29. View Daily Engagement Trends (Dgraph)")
    print("30. View Weekly Engagement Trends (Dgraph)")
    print("31. View Monthly Engagement Trends (Dgraph)")
    print("32. View Yearly Engagement Trends (Dgraph)")
    print("33. Cluster Users by Interests (Dgraph)")
    print("34. Identify Inactive Users (Dgraph)")
    print("35. Analyze Post Retention (Dgraph)")
    print("36. Find Top Performing Post (Dgraph)")
    print("37. Populate Database with Sample Data (Dgraph)")
    print("0. Exit")


def main():
    mongo_model = MongoModel()

    cassandra_model = CassandraModel()
    cassandra_model.connect_to_cassandra()

    dgraph_model = DgraphModel()
    dgraph_model.connect_to_dgraph()

    while(True):
        print_menu(mongo_model.current_username)
        try:
            option = int(input("Select an option: "))
            if option == 0:
                print("Exiting the program. Goodbye!")
                break
            # MongoDB
            elif option == 1:
                mongo_model.create_user()
            elif option == 2:
                mongo_model.delete_user()
            elif option == 3:
                mongo_model.login()
            elif option == 4:
                mongo_model.logout()
            elif option == 5:
                mongo_model.change_password()
            elif option == 6:
                mongo_model.change_notifications()
            elif option == 7:
                mongo_model.change_language()
            elif option == 8:
                mongo_model.most_used_language()
            elif option == 9:
                mongo_model.create_post()
            elif option == 10:
                mongo_model.delete_a_post()
            elif option == 11:
                mongo_model.see_your_posts()
            elif option == 12:
                mongo_model.see_posts_from_people()
            elif option == 13:
                mongo_model.get_list_of_users()
            elif option == 14:
                mongo_model.populate_database()
            elif option == 15:
                mongo_model.clean_database()
            # Cassandra
            elif option == 16:
                cassandra_model.follower_number_analysis()
            elif option == 17:
                cassandra_model.user_interaction_patterns()
            elif option == 18:
                cassandra_model.trend_analysis_of_topics()
            elif option == 19:
                print("\nPerforming User Sentiment Analysis...")
                sentiment_results = cassandra_model.user_sentiment_analysis()
                for result in sentiment_results:
                    print(f"Post ID: {result['post_id']}")
                    print(f"Content: {result['content']}")
                    print(f"Sentiment: {result['sentiment']}\n")
            elif option == 20:
                cassandra_model.content_type_performance()
            elif option == 21:
                tag = input("Enter the hashtag to analyze: ").strip()
                if tag:
                    print(f"\nFetching most engaging post types for the hashtag: {tag}")
                    cassandra_model.most_engaging_post_types(tag)
                else:
                    print("Hashtag cannot be empty. Please try again.")
            elif option == 22:
                cassandra_model.keyword_influence_on_engagement()
            elif option == 23:
                cassandra_model.follower_to_engagement_ratio()
            elif option == 24:
                cassandra_model.average_response_time_to_comments()
            elif option == 25:
                cassandra_model.time_to_first_engagement()
            elif option == 26:
                cassandra_model.top_shared_posts()
            elif option == 27:
                print("\nPopulating the Cassandra database with random test data...")
                cassandra_model.populate_database()
                print("Database populated successfully!")
            # Dgraph
            elif option == 28:
                dgraph_model.analyze_platform_usage()
            elif option == 29:
                dgraph_model.view_daily_engagement_trends()
            elif option == 30:
                dgraph_model.view_weekly_engagement_trends()
            elif option == 31:
                dgraph_model.view_monthly_engagement_trends()
            elif option == 32:
                dgraph_model.view_yearly_engagement_trends()
            elif option == 33:
                dgraph_model.cluster_users_by_interests()
            elif option == 34:
                dgraph_model.identify_inactive_users()
            elif option == 35:
                dgraph_model.analyze_post_retention()
            elif option == 36:
                dgraph_model.find_top_performing_post()
            elif option == 37:
                # dgraph_model
                pass
            # Else
            else:
                print("Invalid option. Please try again.")
        except ValueError:
            print("Error: Please enter a valid number.")
        except Exception as e:
            print(f"Unexpected error: {e}")
    
    cassandra_model.close_connection()
    dgraph_model.close_connection()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))
