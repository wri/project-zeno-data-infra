from pipelines.aois.name_index.wikidata import parse_identifiers


def test_identifiers_keep_the_item_id_and_text_values_like_na():
    csv = (
        b"item,id\n"
        b"http://www.wikidata.org/entity/Q155,BRA\n"
        b"http://www.wikidata.org/entity/Q1033,NA\n"
    )

    identifiers = parse_identifiers(csv)

    assert identifiers.to_pylist() == [
        {"wikidata_id": "Q155", "id": "BRA"},
        {"wikidata_id": "Q1033", "id": "NA"},
    ]
