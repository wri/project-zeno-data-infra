from app.domain.models.aoi_match import AoiMatch
from app.domain.models.aoi_search import AoiCandidate, MatchKind, NameMatch
from app.domain.services.aoi_name_ranking import AoiNameRanking


def candidate(
    aoi_id,
    kind,
    importance,
    corrected=False,
    within_containing_place=False,
    area_ha=1.0,
    aoi_type="admin",
):
    return AoiCandidate(
        aoi=AoiMatch(aoi_type, aoi_id, aoi_id, aoi_id),
        match=NameMatch(kind, corrected),
        within_containing_place=within_containing_place,
        importance=importance,
        area_ha=area_ha,
    )


def ranked_ids(candidates, limit=10):
    return [match.aoi_id for match in AoiNameRanking().best(candidates, limit)]


def test_a_well_known_place_can_outrank_a_better_spelled_obscure_one():
    assert ranked_ids(
        [
            candidate("obscure", MatchKind.EXACT_NAME, importance=0.1),
            candidate("famous", MatchKind.NAME_PREFIX, importance=0.9),
        ]
    ) == ["famous", "obscure"]


def test_places_within_the_containing_place_rank_first():
    assert ranked_ids(
        [
            candidate("elsewhere", MatchKind.EXACT_NAME, importance=0.8),
            candidate(
                "within",
                MatchKind.EXACT_NAME,
                importance=0.5,
                within_containing_place=True,
            ),
        ]
    ) == ["within", "elsewhere"]


def test_matches_through_corrected_words_count_for_less():
    assert ranked_ids(
        [
            candidate("corrected", MatchKind.NAME_WORDS, 0.5, corrected=True),
            candidate("spelled", MatchKind.NAME_WORDS, 0.5),
        ]
    ) == ["spelled", "corrected"]


def test_custom_aois_rank_above_other_aois_matching_as_well():
    assert ranked_ids(
        [
            candidate("famous", MatchKind.NAME_PREFIX, importance=0.9),
            candidate("mine", MatchKind.NAME_PREFIX, 0.0, aoi_type="custom_aoi"),
        ]
    ) == ["mine", "famous"]


def test_each_aoi_is_ranked_once_by_its_best_match_with_ties_to_larger_areas():
    assert ranked_ids(
        [
            candidate("small", MatchKind.NAME_PREFIX, 0.5, area_ha=1.0),
            candidate("large", MatchKind.NAME_WORDS, 0.5, area_ha=9.0),
            candidate("large", MatchKind.NAME_PREFIX, 0.5, area_ha=9.0),
            candidate("other", MatchKind.NAME_WORDS, 0.5),
        ],
        limit=2,
    ) == ["large", "small"]
