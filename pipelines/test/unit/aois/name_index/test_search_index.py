import pandas as pd
import tantivy

from pipelines.aois.name_index.search_index import name_analyzers, write_search_index

WORD_PATTERN = r"[^\s,.;:()\[\]/'\"-]+"


def test_names_are_found_by_any_word_ignoring_case_and_accents(tmp_path):
    records = pd.DataFrame(
        {
            "aoi_type": ["admin"],
            "aoi_id": ["BRA.14"],
            "name": ["Pará"],
            "display_name": ["Pará, Brazil"],
            "alternate_names": [["Estado do Grão-Pará"]],
            "importance": [0.7],
            "area_ha": [1.2e8],
        }
    )

    write_search_index(records, str(tmp_path), WORD_PATTERN)

    index = tantivy.Index.open(str(tmp_path))
    for name, analyzer in name_analyzers(WORD_PATTERN).items():
        index.register_tokenizer(name, analyzer)
    searcher = index.searcher()
    query = tantivy.Query.term_query(index.schema, "alternate_words", "grao")
    (_, address), *_ = searcher.search(query).hits
    assert searcher.doc(address).get_first("display_name") == "Pará, Brazil"
