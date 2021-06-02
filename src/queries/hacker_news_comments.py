def get_hacker_news_comments_query():
    query = """SELECT comments.id AS comment_id,
                      comments.parent AS comment_story_id,
                      comments.text AS comment_text,
                      comments.by AS comment_author,
                FROM `fh-bigquery.hackernews.commentsV2` as comments 
                WHERE comments.text IS NOT NULL
            """
    return query
