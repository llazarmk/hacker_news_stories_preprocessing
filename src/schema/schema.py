

STORIES_TEXT_PROCESSING_SCHEMA = (
    ('story_id', 'INT64'),
    ('story_date', 'DATE'),
    ('story_title', 'STRING'),
    ('story_url', 'STRING'),
    ('story_url_domain','STRING'),
    ('story_url_category', 'STRING'),
    ('story_author', 'STRING'),
    ('story_score', 'NUMERIC'),
    ('story_text', 'STRING'),
    ('story_ranking', 'NUMERIC'),
    ('story_label_entity', 'STRING'),
    ('story_explained_entity', 'STRING'),
    ('story_spacy_text', 'STRING'),


)

COMMENTS_TEXT_PROCESSING_SCHEMA = (
    ('comment_id', 'INT64'),
    ('comment_story_id', 'INT64'),
    ('comment_text', 'STRING'),
    ('comment_author', 'STRING'),
    ('comment_label_entity', 'STRING'),
    ('comment_explained_entity', 'STRING'),
    ('comment_spacy_text', 'STRING'),
)
