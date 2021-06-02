def get_hacker_news_stories_query(table):
    query = f"""SELECT stories.id AS story_id,
                      DATE(stories.timestamp) AS story_date,
                      stories.title AS story_title,
                      stories.url AS story_url,
                      stories.text AS story_text,
                      stories.by AS story_author,
                      stories.score AS story_score,
                      stories.ranking AS story_ranking,
                FROM `{table}` stories 
            """
    return query
