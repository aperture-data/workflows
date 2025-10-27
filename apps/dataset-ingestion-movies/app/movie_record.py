from typing import List
from aperturedb.Query import QueryBuilder

def make_movie_with_all_connections(j: dict) -> List[dict]:
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
        ## Adding this as there is not way to get the class from entities in ADB.
        label="MOVIE",
        uniqueid=str(j["tmdb_5000_credits.csv/title"]).capitalize()
    ), if_not_found=dict(id=["==", str(j["tmdb_5000_credits.csv/movie_id"])]))

    movie = QueryBuilder.add_command("MOVIE", movie_parameters)
    transaction.append(movie)

    index = 2
    for cast_info in j["tmdb_5000_credits.csv/cast"]:
        c = cast_info
        cast_parameters = dict(_ref=index, properties=dict(
            id=c["id"],
            name=c["name"],
            gender=c["gender"],
            label="PROFESSIONAL",
            uniqueid=c["name"].capitalize()
        ), if_not_found=dict(id=["==", c["id"]]))
        professional = QueryBuilder.add_command("PROFESSIONAL", cast_parameters)
        transaction.append(professional)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            character=c["character"],
            cast_id=c["cast_id"],
            name="CAST",
            uniqueid="CAST"
            )
        )
        connection_parameters["class"] = "CAST"
        connection = QueryBuilder.add_command("_Connection", connection_parameters)
        transaction.append(connection)
        index += 1

    for crew_info in j["tmdb_5000_credits.csv/crew"]:
        c = crew_info
        crew_parameters = dict(_ref=index, properties=dict(
            id=c["id"],
            name=c["name"],
            gender=c["gender"],
            label="PROFESSIONAL",
            uniqueid=c["name"].capitalize()
        ), if_not_found=dict(id=["==", c["id"]]))
        professional = QueryBuilder.add_command("PROFESSIONAL", crew_parameters)
        transaction.append(professional)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            department=c["department"],
            job=c["job"],
            credit_id=c["credit_id"],
            name="CREW",
            uniqueid="CREW"
            )
        )
        connection = QueryBuilder.add_command("_Connection", connection_parameters)
        connection_parameters["class"] = "CREW"
        transaction.append(connection)
        index += 1

    for genre in j["tmdb_5000_movies.csv/genres"]:
        genre_parameters = dict(_ref=index, properties=dict(
            id=genre["id"],
            name=genre["name"],
            label="GENRE",
            uniqueid=genre["name"].capitalize()
        ), if_not_found=dict(id=["==", genre["id"]]))
        genre_command = QueryBuilder.add_command("GENRE", genre_parameters)
        transaction.append(genre_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name="HAS_GENRE",
            uniqueid="HAS_GENRE"
        ))
        connection_parameters["class"] = "HAS_GENRE"
        connection = QueryBuilder.add_command("_Connection", connection_parameters)
        transaction.append(connection)
        index += 1

    for production_company in j["tmdb_5000_movies.csv/production_companies"]:
        company_parameters = dict(_ref=index, properties=dict(
            id=production_company["id"],
            name=production_company["name"],
            label="PRODUCTION_COMPANY",
            uniqueid=production_company["name"].capitalize()
        ), if_not_found=dict(id=["==", production_company["id"]]))
        company_command = QueryBuilder.add_command("PRODUCTION_COMPANY", company_parameters)
        transaction.append(company_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name="HAS_PRODUCTION_COMPANY",
            uniqueid="HAS_PRODUCTION_COMPANY"
        ))
        connection_parameters["class"] = "HAS_PRODUCTION_COMPANY"
        connection = QueryBuilder.add_command("_Connection", connection_parameters)
        transaction.append(connection)
        index += 1

    for keyword in j["tmdb_5000_movies.csv/keywords"]:
        keyword_parameters = dict(_ref=index, properties=dict(
            id=keyword["id"],
            name=keyword["name"],
            label="KEYWORD",
            uniqueid=keyword["name"].capitalize()
        ), if_not_found=dict(id=["==", keyword["id"]]))
        keyword_command = QueryBuilder.add_command("KEYWORD", keyword_parameters)
        transaction.append(keyword_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name="HAS_KEYWORD",
            uniqueid="HAS_KEYWORD"
        ))
        connection_parameters["class"] = "HAS_KEYWORD"
        connection = QueryBuilder.add_command("_Connection", connection_parameters)
        transaction.append(connection)
        index += 1

    for spoken_language in j["tmdb_5000_movies.csv/spoken_languages"]:
        language_parameters = dict(_ref=index, properties=dict(
            iso_639_1=spoken_language["iso_639_1"],
            name=spoken_language["name"],
            label="SPOKEN_LANGUAGE",
            uniqueid=spoken_language["name"].capitalize()
        ), if_not_found=dict(iso_639_1=["==", spoken_language["iso_639_1"]]))
        language_command = QueryBuilder.add_command("SPOKEN_LANGUAGE", language_parameters)
        transaction.append(language_command)

        connection_parameters = dict(src=1, dst=index, properties=dict(
            name="HAS_SPOKEN_LANGUAGE",
            uniqueid="HAS_SPOKEN_LANGUAGE"
        ))
        connection_parameters["class"] = "HAS_SPOKEN_LANGUAGE"
        connection = QueryBuilder.add_command("_Connection", connection_parameters)
        transaction.append(connection)
        index += 1

    return transaction