import pandas as pd

from pipelines.aois.name_index.wikipedia.links import (
    WikidataLink,
    at_admin_level,
    link_to_wikidata,
    wikipedia_details,
    without_revision,
)


def identifiers(*pairs):
    return pd.DataFrame(pairs, columns=["wikidata_id", "id"])


def wikimedia_file(tmp_path, rows):
    path = tmp_path / "wikimedia.csv"
    lines = ["language\ttype\ttitle\timportance\twikidata_id"]
    for row in rows:
        lines.append("\t".join(str(value) for value in row))
    path.write_text("\n".join(lines) + "\n")
    return str(path)


def as_tuples(frame):
    return sorted(frame.itertuples(index=False, name=None))


def test_each_property_links_only_the_aois_its_rule_applies_to():
    gadm = pd.DataFrame(
        {
            "aoi_id": ["BRA", "BRA.14", "BRA.14.3"],
            "adm_level": [0, 1, 2],
            "hasc_1": [None, "BR.PA", "BR.PA"],
            "hasc_2": [None, None, "BR.PA.XX"],
        }
    )

    links = link_to_wikidata(
        {"admin": gadm},
        {
            "P8714": identifiers(("Q1", "BRA.14_1")),
            "P298": identifiers(("Q155", "BRA")),
            "P8119": identifiers(("Q40", "BR.PA"), ("Q41", "BR.PA.XX")),
        },
        [
            WikidataLink("P8714", "admin", wikidata_identifier=without_revision),
            WikidataLink("P298", "admin", applies_to=at_admin_level(0)),
            WikidataLink("P8119", "admin", "hasc_1", at_admin_level(1)),
            WikidataLink("P8119", "admin", "hasc_2", at_admin_level(2)),
        ],
    )

    assert as_tuples(links) == [
        ("admin", "BRA", "Q155"),
        ("admin", "BRA.14", "Q1"),
        ("admin", "BRA.14", "Q40"),
        ("admin", "BRA.14.3", "Q41"),
    ]


def test_importance_is_each_aois_best_known_linked_item(tmp_path):
    links = pd.DataFrame(
        {
            "aoi_type": ["admin", "admin"],
            "aoi_id": ["BRA.14", "BRA.14"],
            "wikidata_id": ["Q1", "Q40"],
        }
    )
    wikimedia = wikimedia_file(
        tmp_path,
        [
            ("en", "a", "Pará", 0.6, "Q1"),
            ("pt", "a", "Pará", 0.7, "Q1"),
            ("en", "a", "Pará_region", 0.65, "Q40"),
        ],
    )

    importance, _ = wikipedia_details(links, wikimedia)

    assert as_tuples(importance) == [("admin", "BRA.14", "Q1", 0.7)]


def test_alternate_names_are_cleaned_article_titles_of_the_chosen_item(tmp_path):
    links = pd.DataFrame(
        {"aoi_type": ["admin"], "aoi_id": ["BRA.14"], "wikidata_id": ["Q1"]}
    )
    wikimedia = wikimedia_file(
        tmp_path,
        [
            ("en", "a", "Pará_(state)", 0.6, "Q1"),
            ("pt", "a", "Pará", 0.7, "Q1"),
            ("fr", "a", "État_du_Pará", 0.5, "Q1"),
            ("en", "r", "History_of_Pará", 0.6, "Q1"),
        ],
    )

    _, alternate_names = wikipedia_details(links, wikimedia)

    assert as_tuples(alternate_names) == [
        ("admin", "BRA.14", "Pará"),
        ("admin", "BRA.14", "État du Pará"),
    ]
