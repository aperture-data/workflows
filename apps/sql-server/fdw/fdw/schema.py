"""Native typed schema and exact endpoint-scoped index metadata."""
from copy import deepcopy


def property_info(value):
    return value.get("count", 0), value.get("indexed", False), value["type"]


def native_classes(field, schema, indexes):
    classes = {}
    for record in schema.get("nodes" if field == "entities" else "edges", []):
        item = deepcopy(record)
        if field == "entities":
            target = record["type"]
            name = target + ":" + record["class"]
            definitions = [index for index in indexes if index["target"] == target
                           and index["class"].casefold() == record["class"].casefold()]
            item["_type"], item["_class"] = target, record["class"]
        else:
            target = record["target"]
            edge_class = record["class"]
            name = edge_class["connection"]
            definitions = [index for index in indexes if index["target"] == target
                           and all(index["class"][key].casefold() == edge_class[key].casefold()
                                   for key in ("src", "connection", "dst"))]
            item["src"] = target["src"] + ":" + edge_class["src"]
            item["dst"] = target["dst"] + ":" + edge_class["dst"]
        indexed = {index["property"] for index in definitions}
        item["properties"] = {key: dict(info, indexed=key in indexed)
                              for key, info in (record.get("properties") or {}).items()}
        if field == "entities":
            classes[name] = item
        else:
            classes.setdefault(name, []).append(item)
    return classes
