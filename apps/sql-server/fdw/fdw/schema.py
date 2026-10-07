"""Read property metadata from either GetSchema representation."""


def property_info(value):
    """Accept both GetSchema property formats; indexing is optional metadata."""
    if isinstance(value, dict):
        return value.get("count", 0), value.get("indexed", False), value["type"]
    return tuple(value)


def with_index_metadata(classes, indexes, target):
    """Supply the planner's indexed flag when GetSchema omits it."""
    def enrich(data, definitions):
        if isinstance(data, list):
            return [enrich(item, definitions) for item in data]
        if "matched" not in data:
            return {key: enrich(item, definitions) for key, item in data.items()}
        result = dict(data)
        if data.get("properties") is not None:
            result["properties"] = {
                name: dict(info, indexed=bool(definitions.get(name)))
                if isinstance(info, dict) and "indexed" not in info else info
                for name, info in data["properties"].items()}
        return result

    definitions = indexes.get(target, {})
    return {name: enrich(data, definitions.get(name, {}))
            for name, data in classes.items()}


def needs_index_metadata(classes):
    """Legacy property arrays already contain the planner's indexed flag."""
    def missing(data):
        if isinstance(data, list):
            return any(missing(item) for item in data)
        if "matched" not in data:
            return any(missing(item) for item in data.values())
        return any(isinstance(info, dict) and "indexed" not in info
                   for info in (data.get("properties") or {}).values())
    return any(missing(data) for data in classes.values())
