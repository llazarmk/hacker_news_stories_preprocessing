### Hacker news stories_preprocessing

This is one of the first service in a toy project
for article recommender system and is mainly a learning
tool for learning dataflow

Some inspirations and learning for this service are from 
https://github.com/opentargets/library-beam

Hackernews public dataset contains comments and stories  from the hacker news website.
As first step we want to gain an insight from the text and comments. 
We utilize spacy functionalities like,document entity label explanation.

A named entity is a “real-world object”
that’s assigned a name – for example, a person, a country, a product or a book title. 
spaCy can recognize various types of named entities in a document, by asking the model 
for a prediction. Because models are statistical and strongly depend on the examples
they were trained on, this doesn’t always work perfectly and might need some tuning later,
depending on your use case.

Named entities are available as the ents property of a Doc:


```
import spacy

nlp = spacy.load("en_core_web_sm")
doc = nlp("Apple is looking at buying U.K. startup for $1 billion")

for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
    
Apple	0	5	ORG	Companies, agencies, institutions.
U.K.	27	31	GPE	Geopolitical entity, i.e. countries, cities, states.
$1 billion	44	54	MONEY	Monetary values, including unit.
```

https://spacy.io/usage/spacy-101

The result would be then saved in another big query table
for analysis and



- Get the data from 'fh-bigquery.hackernews.storiesV2'

``` 
SELECT stories.id AS story_id,
       DATE(stories.timestamp) AS story_date,
       stories.title AS story_title,
       stories.url AS story_url,
       stories.text AS story_text,
       stories.by AS story_author,
       stories.score AS story_score,
       stories.ranking AS story_ranking, 
FROM `fh-bigquery.hackernews.storiesV2` AS stories
 ```

- The preprocessing step is done in Dataflow with the integration of spacy. 
  Spacy is utilized for cleaning the punctuations and stopwords from the text,
  creating entities and explanation from the text 
  
```
 
  def clean_words(document: spacy.tokens.doc.Doc) -> str:
        word_cleaned = [word.lemma_.lower().strip() for word in document if not word.is_stop and not word.is_punct]
        word_cleaned = " ".join(word_cleaned)
        return word_cleaned
        
  def create_set_entities(document: spacy.tokens.doc.Doc):
      text, label, explained = set(), set(), set()
      for ent in document.ents:
            if ent.label_ != 'CARDINAL' and ent.label_ != 'ORDINAL':
                label.add(ent.label_.lower())
                explained.add(spacy.explain(ent.label_).lower())
       entities = (text, label, explained)
       return entities
```

This is the dataflow ptransform that encapsulate the
spacy functionalities and the url parsing.
```
class HNStoriesProcessing(beam.PTransform):

    def __init__(self,
                 feature_columns: dict = None,
                 batch_size: int = 100,
                 input_column_url: str = 'story_url',
                 output_column_url: str = 'story_url_category'
                 ):
        """
        This ptransform

        :param feature_columns: dict
               input feature columns in the form
               feature_columns = {'id': 'story_id',
                       'feature': 'story_text',
                       'output_prefix': 'story'
                       }
        :param batch_size: int
               value of the batch size in each bundle
        :param input_column_url: str
        :param output_column_url: str
        """
        self.feature_columns = feature_columns
        self.batch_size = batch_size
        self.input_column_url = input_column_url
        self.output_column_url = output_column_url

    def expand(self, pcollection):
        processed_data = (pcollection
                          | "HackerNewsStoryProcessing" >> beam.ParDo(
                    TextAnalyser(feature_columns=self._feature_columns,batch_size=self._batch_size
                                 )))

        processed_data = (processed_data | 'CreateUrlPath' >> beam.ParDo(UrlParser(input_column=self.input_column_url,
                                                                                   output_column=self.output_column_url))
                          | "GetUrlDomain" >> beam.Map(get_element_domain)
                          )
        return processed_data
```
Some explanation here:
- The PCollection abstraction represents a potentially distributed, multi-element data set. You can think of a PCollection as “pipeline” data; Beam transforms use PCollection objects as inputs and outputs. As such, 
  if you want to work with data in your pipeline, it must be in the form of a PCollection.
  
- ParDo is a Beam transform for generic parallel processing.
The ParDo processing paradigm is similar to the “Map” phase of a Map/Shuffle/Reduce-style algorithm:
a ParDo transform considers each element in the input PCollection, 
performs some processing function (your user code) on that element,
and emits zero, one, or multiple elements to an output PCollection.
When you apply a ParDo transform, you’ll need to provide user code in the form of a DoFn object.
DoFn is a Beam SDK class that defines a distributed processing function.
  
In our case we use the TextAnalyser DoFn to distribute the spacy processing logic and the 
url parsing

In case of the url we extract the main domain of the url and the path, we apply some
filtering, mainly we check if the value is all numbers or is just a date
then we are not interested in saving this value
```
https://www.123.com/%7Eguido/Python.html
www.123.com, /%7Eguido/Python.html

```
https://beam.apache.org/documentation/programming-guide/

- stories url is parsed in base url and category for example article/technology etc.  

###  Steps to reproduce a full run
- Python 3.8
  
  https://linuxize.com/post/how-to-install-python-3-8-on-ubuntu-18-04/
  https://www.python.org/downloads/windows/
  
- Choose python3.8 in venv
  
  https://medium.com/hackernoon/installing-multiple-python-versions-on-windows-using-virtualenv-333ed06ef43a
        
- git clone 
  https://github.com/llazarmk/stories_preprocessing/tree/dev
  cd stories_preprocessing
  
- pip install -r requirements.txt

- gcloud auth login
  gcloud auth application-default login
  


