from typing import List
from aperturedb.Query import QueryBuilder
from aperturedb.Utils import Utils
import requests

from embeddings import Embedder

DATASET_NAME = "tmdb_5000"

# Entity Labels
MOVIE_ENTITY_LABEL = "Movie"
PROFESSIONAL_ENTITY_LABEL = "Professional"
GENRE_ENTITY_LABEL = "Genre"
PRODUCTION_COMPANY_ENTITY_LABEL = "ProductionCompany"
KEYWORD_ENTITY_LABEL = "Keyword"
SPOKEN_LANGUAGE_ENTITY_LABEL = "SpokenLanguage"
IMAGE_LABEL = "_Image"
CONNECTION_LABEL = "_Connection"
DESCRIPTOR_LABEL = "_Descriptor"
DESCRIPTOR_SET_LABEL = "_DescriptorSet"

# Connection Labels
HAS_CAST_CONNECTION_LABEL = "HasCast"
HAS_CREW_CONNECTION_LABEL = "HasCrew"
HAS_GENRE_CONNECTION_LABEL = "HasGenre"
HAS_PRODUCTION_COMPANY_CONNECTION_LABEL = "HasProductionCompany"
HAS_KEYWORD_CONNECTION_LABEL = "HasKeyword"
HAS_SPOKEN_LANGUAGE_CONNECTION_LABEL = "HasSpokenLanguage"
HAS_IMAGE_CONNECTION_LABEL = "HasImage"
HAS_TAGLINE_EMBEDDING_CONNECTION_LABEL = "HasTaglineEmbedding"
HAS_IMAGE_EMBEDDING_CONNECTION_LABEL = "HasImageEmbedding"

def make_movie_with_all_connections(j: dict, embedder: Embedder, ingest_posters: bool = False, embed_tagline: bool = False) -> List[dict]:
    """
    This is where we create the Commands to create Movie and Professional objects
    and the HasCast connection between them.
    The movie is the root object, and the cast are the children.
    Each call to this function creates a transaction that will be executed in the database.

    Args:
        j (dict): a record from the dataset. The record is a dictionary with the following keys:

    Returns:
        List[dict]: A list of commands to be executed in the database.
    """
    transaction = []
    blobs = []
    movie_parameters = dict(_ref=1, properties=dict(
        id=str(j["tmdb_5000_credits.csv/movie_id"]),
        movie_id=j["tmdb_5000_credits.csv/movie_id"],
        title=str(j["tmdb_5000_credits.csv/title"]),
        budget=j["tmdb_5000_movies.csv/budget"],
        overview=str(j["tmdb_5000_movies.csv/overview"]),
        popularity=j["tmdb_5000_movies.csv/popularity"],
        tagline=str(j["tmdb_5000_movies.csv/tagline"]),
        vote_average=j["tmdb_5000_movies.csv/vote_average"],
        vote_count=j["tmdb_5000_movies.csv/vote_count"],
        dataset_name=DATASET_NAME,
        ## Adding this as there is not way to get the class from entities in ADB.
        label=MOVIE_ENTITY_LABEL,
        uniqueid=str(j["tmdb_5000_credits.csv/title"]).capitalize()
    ), if_not_found=dict(id=["==", str(j["tmdb_5000_credits.csv/movie_id"])]))

    movie = QueryBuilder.add_command(MOVIE_ENTITY_LABEL, movie_parameters)
    transaction.append(movie)

    index = 2
    for cast_info in j["tmdb_5000_credits.csv/cast"]:
        c = cast_info
        cast_parameters = dict(_ref=index, properties=dict(
            id=c["id"],
            name=c["name"],
            gender=c["gender"],
            label=PROFESSIONAL_ENTITY_LABEL,
            uniqueid=c["name"].capitalize(),
            dataset_name=DATASET_NAME
        ), if_not_found=dict(id=["==", c["id"]]))
        professional = QueryBuilder.add_command(PROFESSIONAL_ENTITY_LABEL, cast_parameters)
        transaction.append(professional)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            character=c["character"],
            cast_id=c["cast_id"],
            name=HAS_CAST_CONNECTION_LABEL,
            uniqueid=HAS_CAST_CONNECTION_LABEL
            )
        )
        connection_parameters["class"] = HAS_CAST_CONNECTION_LABEL
        connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
        transaction.append(connection)
        index += 1

    for crew_info in j["tmdb_5000_credits.csv/crew"]:
        c = crew_info
        crew_parameters = dict(_ref=index, properties=dict(
            id=c["id"],
            name=c["name"],
            gender=c["gender"],
            label=PROFESSIONAL_ENTITY_LABEL,
            uniqueid=c["name"].capitalize(),
            dataset_name=DATASET_NAME
        ), if_not_found=dict(id=["==", c["id"]]))
        professional = QueryBuilder.add_command(PROFESSIONAL_ENTITY_LABEL, crew_parameters)
        transaction.append(professional)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            department=c["department"],
            job=c["job"],
            credit_id=c["credit_id"],
            name=HAS_CREW_CONNECTION_LABEL,
            uniqueid=HAS_CREW_CONNECTION_LABEL
            )
        )
        connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
        connection_parameters["class"] = HAS_CREW_CONNECTION_LABEL
        transaction.append(connection)
        index += 1

    for genre in j["tmdb_5000_movies.csv/genres"]:
        genre_parameters = dict(_ref=index, properties=dict(
            id=genre["id"],
            name=genre["name"],
            label=GENRE_ENTITY_LABEL,
            uniqueid=genre["name"].capitalize(),
            dataset_name=DATASET_NAME
        ), if_not_found=dict(id=["==", genre["id"]]))
        genre_command = QueryBuilder.add_command(GENRE_ENTITY_LABEL, genre_parameters)
        transaction.append(genre_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name=HAS_GENRE_CONNECTION_LABEL,
            uniqueid=HAS_GENRE_CONNECTION_LABEL
        ))
        connection_parameters["class"] = HAS_GENRE_CONNECTION_LABEL
        connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
        transaction.append(connection)
        index += 1

    for production_company in j["tmdb_5000_movies.csv/production_companies"]:
        company_parameters = dict(_ref=index, properties=dict(
            id=production_company["id"],
            name=production_company["name"],
            label=PRODUCTION_COMPANY_ENTITY_LABEL,
            uniqueid=production_company["name"].capitalize(),
            dataset_name=DATASET_NAME
        ), if_not_found=dict(id=["==", production_company["id"]]))
        company_command = QueryBuilder.add_command(PRODUCTION_COMPANY_ENTITY_LABEL, company_parameters)
        transaction.append(company_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name=HAS_PRODUCTION_COMPANY_CONNECTION_LABEL,
            uniqueid=HAS_PRODUCTION_COMPANY_CONNECTION_LABEL
        ))
        connection_parameters["class"] = HAS_PRODUCTION_COMPANY_CONNECTION_LABEL
        connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
        transaction.append(connection)
        index += 1

    for keyword in j["tmdb_5000_movies.csv/keywords"]:
        keyword_parameters = dict(_ref=index, properties=dict(
            id=keyword["id"],
            name=keyword["name"],
            label=KEYWORD_ENTITY_LABEL,
            uniqueid=keyword["name"].capitalize(),
            dataset_name=DATASET_NAME
        ), if_not_found=dict(id=["==", keyword["id"]]))
        keyword_command = QueryBuilder.add_command(KEYWORD_ENTITY_LABEL, keyword_parameters)
        transaction.append(keyword_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name=HAS_KEYWORD_CONNECTION_LABEL,
            uniqueid=HAS_KEYWORD_CONNECTION_LABEL
        ))
        connection_parameters["class"] = HAS_KEYWORD_CONNECTION_LABEL
        connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
        transaction.append(connection)
        index += 1

    for spoken_language in j["tmdb_5000_movies.csv/spoken_languages"]:
        language_parameters = dict(_ref=index, properties=dict(
            iso_639_1=spoken_language["iso_639_1"],
            name=spoken_language["name"],
            label=SPOKEN_LANGUAGE_ENTITY_LABEL,
            uniqueid=spoken_language["name"].capitalize(),
            dataset_name=DATASET_NAME
        ), if_not_found=dict(iso_639_1=["==", spoken_language["iso_639_1"]]))
        language_command = QueryBuilder.add_command(SPOKEN_LANGUAGE_ENTITY_LABEL, language_parameters)
        transaction.append(language_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name=HAS_SPOKEN_LANGUAGE_CONNECTION_LABEL,
            uniqueid=HAS_SPOKEN_LANGUAGE_CONNECTION_LABEL
        ))
        connection_parameters["class"] = HAS_SPOKEN_LANGUAGE_CONNECTION_LABEL
        connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
        transaction.append(connection)
        index += 1

    if ingest_posters:
        bucket_prefix = "https://storage.googleapis.com/ad-demos-datasets/tmdb/posters"
        request = requests.get(f"{bucket_prefix}/{movie_parameters['properties']['id']}.jpg")
        if request.status_code == 200:
            image_data = request.content
            image_command = QueryBuilder.add_command(IMAGE_LABEL, dict(
                _ref=index,
                properties=dict(
                    id=movie_parameters['properties']['id'],
                    dataset_name=DATASET_NAME
                ), if_not_found=dict(id=["==", movie_parameters['properties']['id']])
            ))
            transaction.append(image_command)
            connection_parameters = dict(src=1, dst=index, properties=dict(
                name=HAS_IMAGE_CONNECTION_LABEL,
                uniqueid=HAS_IMAGE_CONNECTION_LABEL
            ))
            connection_parameters["class"] = HAS_IMAGE_CONNECTION_LABEL
            connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
            transaction.append(connection)
            blobs.append(image_data)
            index += 1

            image_descriptor = embedder.embed_image(image_data)
            image_descriptor_blob = image_descriptor.tobytes()
            image_descriptor_id = f"image_embedding_{movie_parameters['properties']['id']}"
            image_descriptor_command = QueryBuilder.add_command(DESCRIPTOR_LABEL, dict(
                _ref=index,
                set=embedder.descriptor_set,
                properties=dict(
                    source="image",
                    dataset_name=DATASET_NAME,
                    id=image_descriptor_id,
                ), if_not_found=dict(id=["==", image_descriptor_id])
            ))
            transaction.append(image_descriptor_command)
            blobs.append(image_descriptor_blob)
            connection_parameters = dict(src=index - 1, dst=index, properties=dict(
                name=HAS_IMAGE_EMBEDDING_CONNECTION_LABEL,
                uniqueid=HAS_IMAGE_EMBEDDING_CONNECTION_LABEL
            ))
            connection_parameters["class"] = HAS_IMAGE_EMBEDDING_CONNECTION_LABEL
            connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
            transaction.append(connection)
            index += 1

    if embed_tagline:
        tagline_embedding = embedder.embed_text(movie_parameters['properties']['tagline'])
        tagline_blob = tagline_embedding.tobytes()
        tagline_descriptor_id = f"tagline_embedding_{movie_parameters['properties']['id']}"
        tagline_command = QueryBuilder.add_command(DESCRIPTOR_LABEL, dict(
            _ref=index,
            set=embedder.descriptor_set,
            properties=dict(
                source="tagline",
                id=tagline_descriptor_id,
                dataset_name=DATASET_NAME
            ), if_not_found=dict(id=["==", tagline_descriptor_id])
        ))
        transaction.append(tagline_command)
        blobs.append(tagline_blob)
        connection_parameters = dict(src=1, dst=index, properties=dict(
            name=HAS_TAGLINE_EMBEDDING_CONNECTION_LABEL,
            uniqueid=HAS_TAGLINE_EMBEDDING_CONNECTION_LABEL
        ))
        connection_parameters["class"] = HAS_TAGLINE_EMBEDDING_CONNECTION_LABEL
        connection = QueryBuilder.add_command(CONNECTION_LABEL, connection_parameters)
        transaction.append(connection)

        index += 1


    print(f"Added {index} entities and {len(blobs)} blobs")
    return transaction, blobs

def create_indexes(utils: Utils):
    utils.create_entity_index(MOVIE_ENTITY_LABEL, "id")
    utils.create_entity_index(PROFESSIONAL_ENTITY_LABEL, "id")
    utils.create_entity_index(GENRE_ENTITY_LABEL, "id")
    utils.create_entity_index(PRODUCTION_COMPANY_ENTITY_LABEL, "id")
    utils.create_entity_index(KEYWORD_ENTITY_LABEL, "id")
    utils.create_entity_index(SPOKEN_LANGUAGE_ENTITY_LABEL, "iso_639_1")
    utils.create_entity_index(IMAGE_LABEL, "id")


    utils.create_connection_index(HAS_CAST_CONNECTION_LABEL, "cast_id")
    utils.create_connection_index(HAS_CREW_CONNECTION_LABEL, "crew_id")
    utils.create_connection_index(HAS_GENRE_CONNECTION_LABEL, "id")
    # Demo has embeddings
    utils.create_entity_index(DESCRIPTOR_LABEL, "_create_txn")
    utils.create_entity_index(DESCRIPTOR_LABEL, "id")
    utils.create_entity_index(DESCRIPTOR_SET_LABEL, "_name")
