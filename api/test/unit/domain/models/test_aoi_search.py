from app.domain.models.aoi_search import AoiSearch


def test_text_after_the_first_comma_is_the_containing_place():
    search = AoiSearch.parse(" Lebanon, Pennsylvania, USA ", None, 10)

    assert (search.place, search.containing_place, search.text) == (
        "Lebanon",
        "Pennsylvania, USA",
        "Lebanon, Pennsylvania, USA",
    )
