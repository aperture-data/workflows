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
            name = record["class"]
            definitions = [index for index in indexes if isinstance(index["target"], dict)
                           and index["class"].casefold() == name.casefold()
                           and all(index["target"].get(key) == record[key] for key in ("src_type", "dst_type"))
                           and all(index.get(key, "").casefold() == record[key].casefold() for key in ("src_class", "dst_class"))]
            item["src"] = record["src_type"] + ":" + record["src_class"]
            item["dst"] = record["dst_type"] + ":" + record["dst_class"]
        indexed = {index["property"] for index in definitions}
        item["properties"] = {key: dict(info, indexed=key in indexed)
                              for key, info in (record.get("properties") or {}).items()}
        if field == "entities":
            classes[name] = item
        else:
            classes.setdefault(name, []).append(item)
    return classes
