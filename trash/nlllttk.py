import spacy

nlp = spacy.load("en_core_web_sm")

text = "Will Trump cut Ukraine off from Starlink?"
doc = nlp(text)

# NER 기반 특정 개체 추출 (PERSON 및 직책 관련)
entities = [ent.text for ent in doc.ents if ent.label_ in ("PERSON", "NORP", "FAC","GPE","LOC","PRODUCT","EVENT","WORK_OF_ART","ORG", "TITLE")]

print(entities)
