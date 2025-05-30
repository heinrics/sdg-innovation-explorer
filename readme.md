# SDG Tech & Science Explorer

This project is still a work in progress and was inspired by the [EPO Code Fest on SDGs](https://www.epo.org/en/news-events/in-focus/codefest/codefest-spring-2025-classifying-patent-data-sustainable-development).

---

## Introduction

The proposed system uses generative AI to go beyond classifying patents and scientific articles according to SDGs, offering a method for accessing and interacting with technology related to the SDGs in a forward-looking way. 
This is achieved by integrating both patent data and scientific literature, with academic research often leading the way in developing solutions to technical problems. On top of being forward-looking by taking into account the academic literature as well. 
This will offer instant insights on how technology is placed into social and economic systems — a key concern in many SDGs. The system includes an initial patent classification step, followed by search capabilities to retrieve documents and generative AI to provide summaries and answer questions. 
The tool will hopefully motivate and inspire users to contribute to the SDGs and to use technologies related to them.

---

## Modeling Approach

Openly available data including a controlled vocabulary and labeled texts are used to train a classifier for patent and scientific texts: 

1. **Generate training set** using the automated patent landscaping approach proposed by [Abood and Feltenberger (2018)](https://link.springer.com/article/10.1007/s10506-018-9222-4). They propose to identify a seed set of documents belonging to a specific area and to expand this with candidates (e.g., cited and citing documents), where a classifier decides if candidates belong to the technology area.
2. **Training a classifier** based on dense vector representations (e.g., BERT) using a seed set based on openly available controlled vocabularies as well as labeled data for the 17 SDGs. The data includes titles and abstracts from labeled scientific works from OpenAlex, labeled texts from the OSDG Community Dataset, as well as the SDG descriptions and controlled vocabulary from Aurora SDG. This model can be evaluated using a train, validation, and test split.
3. **Classifying the patent corpus** (title and abstract) using the trained model to obtain SDG labels (inference).

The classified patents and scientific texts are then ingested into a text and vector search tool. The user can send queries to this system to retrieve the documents and simultaneously get summaries or answers generated by a large language model based on the returned documents and the user's posed query (RAG):

4. **Ingesting SDG-related documents** into a text and vector search system.
5. **Integrating the retrieval system with an LLM** into a RAG pipeline, enabling users to retrieve documents and receive summaries and answers specific to their queries.

---

## Data

The implementation will rely on the following data:

- Patent titles and abstracts from **Google Patents Research Dataset**
- Scientific work titles, abstracts, and SDG classifications from **OpenAlex**
- SDG descriptions and controlled vocabulary from **Aurora SDG**
- Labeled texts from **OSDG Community Dataset**