import re
import praw
import time
import json
import pymongo
import logging
from typing import List, Dict
from pymongo import MongoClient
from datetime import datetime, timezone
from prawcore.exceptions import PrawcoreException
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('therapy_qa_collection.log'),
        logging.StreamHandler()
    ]
)

class TherapyDataScraper:
    def __init__(self):
        try:
            # Use the MONGO_URI from environment variables
            self.client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=5000)
            self.db = self.client['therapy_data']
            self.qa_collection = self.db['qa_pairs']
            self.client.server_info()
            logging.info("Successfully connected to MongoDB")
        except Exception as e:
            logging.error(f"MongoDB connection error: {e}")
            raise

        try:
            # Use the Reddit API credentials from environment variables
            self.reddit = praw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                user_agent=os.getenv('REDDIT_USER_AGENT')
            )
            logging.info("Successfully connected to Reddit API")
        except Exception as e:
            logging.error(f"Reddit API connection error: {e}")
            raise

        self.subreddits = [
            "therapy", "mentalhealth", "TalkTherapy", "psychotherapy",
            "CBT", "DBT", "askatherapist", "therapeuticquestions"
        ]
        
        self._setup_indexes()

    def _setup_indexes(self):
        """Set up MongoDB indexes"""
        try:
            self.qa_collection.create_index([("question_id", pymongo.ASCENDING)], unique=True)
            self.qa_collection.create_index([("created_utc", pymongo.ASCENDING)])
            logging.info("MongoDB indexes created successfully")
        except Exception as e:
            logging.error(f"Error creating indexes: {e}")

    def clean_text(self, text: str) -> str:
        """Clean and format text content"""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http[s]?://\S+', '', text)
        # Remove Reddit formatting
        text = re.sub(r'\[.*?\]\(.*?\)', '', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s.,!?-]', '', text)
        return text.strip()

    def is_valid_qa(self, title: str, selftext: str, comment_body: str) -> bool:
        """Validate if the post and comment form a valid Q&A pair"""
        # Check minimum lengths
        if len(title) < 20 or len(comment_body) < 50:
            return False
            
        # Check if content is removed or deleted
        if "[removed]" in selftext or "[deleted]" in selftext:
            return False
        if "[removed]" in comment_body or "[deleted]" in comment_body:
            return False
            
        # Check if it looks like a question (has ? or question-like phrases)
        question_indicators = ["?", "how", "what", "why", "can", "should", "help", "advice"]
        has_question = any(indicator in title.lower() for indicator in question_indicators)
        
        return has_question

    def categorize_content(self, text: str) -> List[str]:
        """Categorize the content based on keywords"""
        categories = []
        
        category_keywords = {
            "anxiety": ["anxiety", "panic", "stress", "worry", "anxious"],
            "depression": ["depression", "depressed", "mood", "sad", "hopeless"],
            "trauma": ["trauma", "ptsd", "abuse", "traumatic"],
            "relationships": ["relationship", "marriage", "partner", "family", "couple"],
            "therapy_types": ["cbt", "dbt", "emdr", "psychodynamic", "behavioral"],
        }
        
        text_lower = text.lower()
        for category, keywords in category_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                categories.append(category)
                
        return categories

    def extract_qa_pair(self, post, comment) -> Dict:
        """Extract and format a Q&A pair from a post and comment"""
        # Combine title and selftext for the question
        question_text = f"{post.title}\n\n{post.selftext}" if post.selftext else post.title
        question_text = self.clean_text(question_text)
        
        answer_text = self.clean_text(comment.body)
        
        categories = self.categorize_content(question_text + " " + answer_text)
        
        return {
            "question_id": f"{post.id}_{comment.id}",
            "therapeutic_modality": self.get_therapeutic_modality(question_text, answer_text),
            "question_text": question_text,
            "answer_text": answer_text,
            "metadata": {
                "topic_or_issue":  str(post.subreddit),
                "complexity_level": self.assess_complexity(question_text, answer_text),
                "modality_specific_tag": self.get_modality_specific_tag(question_text, answer_text)
            },
            "more": {
                # "post_id": str(post.id),
                # "comment_id": str(comment.id),
                "subreddit": str(post.subreddit),
                # "categories": categories,
                # "question_score": post.score,
                # "answer_score": comment.score,
                "source": "reddit",
                "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                "url": f"https://reddit.com{post.permalink}"
            }
        }

    def get_therapeutic_modality(self, question_text: str, answer_text: str) -> str:
        """Determine the therapeutic modality based on the content"""
        if "cbt" in question_text.lower() or "cbt" in answer_text.lower():
            return "CBT"
        elif "dbt" in question_text.lower() or "dbt" in answer_text.lower():
            return "DBT"
        # Add more modality detection logic here
        else:
            return "Unknown"

    def assess_complexity(self, question_text: str, answer_text: str) -> str:
        """Assess the complexity level of the question and answer"""
        if len(question_text.split()) < 30 and len(answer_text.split()) < 100:
            return "Low"
        elif len(question_text.split()) < 50 and len(answer_text.split()) < 200:
            return "Medium"
        else:
            return "High"

    def get_modality_specific_tag(self, question_text: str, answer_text: str) -> str:
        """Determine any modality-specific tags based on the content"""
        if "parts" in question_text.lower() or "parts" in answer_text.lower():
            return "Parts Work"
        # Add more modality-specific tag logic here
        else:
            return "None"

    def scrape_subreddit(self, subreddit_name: str, post_limit: int = 1000) -> List[Dict]:
        """Scrape Q&A pairs from a specific subreddit"""
        qa_pairs = []
        logging.info(f"Scraping from r/{subreddit_name}")
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            # Scrape from different post categories
            for category in ['hot', 'top', 'new']:
                if category == 'top':
                    posts = subreddit.top(time_filter='year', limit=post_limit//3)
                elif category == 'hot':
                    posts = subreddit.hot(limit=post_limit//3)
                else:
                    posts = subreddit.new(limit=post_limit//3)

                for post in posts:
                    if not post.stickied and post.num_comments > 0:
                        # Sort comments by score to get quality answers
                        post.comment_sort = 'top'
                        post.comments.replace_more(limit=0)
                        
                        for comment in post.comments:
                            if (hasattr(comment, 'body') and 
                                comment.score > 3 and  # Minimum score threshold
                                len(comment.body) >= 50 and  # Minimum length threshold
                                self.is_valid_qa(post.title, post.selftext, comment.body)):
                                
                                qa_pair = self.extract_qa_pair(post, comment)
                                qa_pairs.append(qa_pair)
                                
                                # Save to MongoDB
                                try:
                                    if self.qa_collection.count_documents({'question_id': qa_pair['question_id']}) == 0:
                                        self.qa_collection.insert_one(qa_pair)
                                    else:
                                        logging.info(f"Skipping duplicate question with ID: {qa_pair['question_id']}")
                                except Exception as e:
                                    logging.error(f"Error saving to MongoDB: {e}")
                                
                                if len(qa_pairs) % 100 == 0:
                                    logging.info(f"Collected {len(qa_pairs)} Q&A pairs from r/{subreddit_name}")
                                    
                    # Respect Reddit's rate limits
                    time.sleep(2)
                    
        except PrawcoreException as e:
            logging.error(f"Reddit API error while scraping r/{subreddit_name}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while scraping r/{subreddit_name}: {e}")
            
        return qa_pairs

    def scrape_all_subreddits(self, target_count: int = 5000):
        """Scrape data from all configured subreddits"""
        total_pairs = 0
        pairs_per_subreddit = target_count // len(self.subreddits)
        
        for subreddit in self.subreddits:
            qa_pairs = self.scrape_subreddit(subreddit, pairs_per_subreddit)
            total_pairs += len(qa_pairs)
            
            logging.info(f"Completed scraping r/{subreddit}. Total pairs so far: {total_pairs}")
            
            if total_pairs >= target_count:
                break
                
            # Add delay between subreddits
            time.sleep(5)

        logging.info(f"Scraping completed. Total Q&A pairs collected: {total_pairs}")

    def export_to_json(self, filename: str = "therapy_qa_data.json"):
        """Export the collection to a JSON file"""
        try:
            data = list(self.qa_collection.find({}, {'_id': 0}))
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, default=str, indent=2)
            logging.info(f"Successfully exported data to {filename}")
        except Exception as e:
            logging.error(f"Error exporting data: {e}")

def main():
    try:
        # Initialize the scraper
        scraper = TherapyDataScraper()
        
        # Scrape data from all subreddits
        scraper.scrape_all_subreddits(5000)
        
        # Export the data
        scraper.export_to_json()
        
        logging.info("Data collection completed successfully")
        
    except Exception as e:
        logging.error(f"Fatal error in main execution: {e}")

if __name__ == "__main__":
    main()