from pipelines.aois.name_index.links import WikidataLink
from pipelines.aois.name_index.name_records import NameSpec


def name_specs() -> tuple[NameSpec, ...]:
    """How each searchable AOI type is named. AOIs without a Wikipedia
    importance fall back to one by type: admin areas down to level 2 first,
    then protected areas, then smaller admin areas, then other AOIs, since
    people are most likely to know names in that order."""
    return (
        NameSpec(
            aoi_type="admin",
            source="gadm",
            name="[name_0, name_1, name_2, name_3, name_4, name_5][adm_level + 1]",
            # the area's name followed by each larger area containing it
            display_name=(
                "array_to_string(list_reverse("
                "[name_0, name_1, name_2, name_3, name_4, name_5][1:adm_level + 1]"
                "), ', ')"
            ),
            # GADM separates variant names with "|" and writes "NA" for none
            alternate_names=(
                "list_filter(CASE adm_level"
                " WHEN 1 THEN list_concat("
                "string_split(coalesce(varname_1, ''), '|'), [nl_name_1])"
                " WHEN 2 THEN list_concat("
                "string_split(coalesce(varname_2, ''), '|'), [nl_name_2])"
                " WHEN 3 THEN list_concat("
                "string_split(coalesce(varname_3, ''), '|'), [nl_name_3])"
                " WHEN 4 THEN string_split(coalesce(varname_4, ''), '|')"
                " ELSE [] END, alternate_name -> alternate_name <> 'NA')"
            ),
            # levels 3-5 are mostly unlinked towns, kept below protected areas
            fallback_importance="[0.5, 0.45, 0.35, 0.14, 0.13, 0.12][adm_level + 1]",
            excluded="is_disputed",
        ),
        NameSpec(
            aoi_type="key_biodiversity_area",
            source="kba",
            name="natname",
            display_name="concat_ws(', ', natname, country)",
            alternate_names="[intname]",
            fallback_importance="0.1",
        ),
        NameSpec(
            aoi_type="protected_area",
            source="wdpa",
            name="name",
            display_name="concat_ws(', ', name, desig_eng, iso3)",
            alternate_names="[name_eng]",
            fallback_importance="0.15",
        ),
        NameSpec(
            aoi_type="indigenous_land",
            source="landmark",
            name="name",
            display_name=(
                "concat_ws(', ', name, nullif(ethncty_1, ''), category, country)"
            ),
            alternate_names="[ethncty_1]",
            fallback_importance="0.1",
        ),
    )


def wikidata_links() -> tuple[WikidataLink, ...]:
    """Wikidata identifier properties that match our AOI ids or attributes:
    GADM ids (with a "_1" revision suffix), ISO 3166-1 alpha-3 and ISO 3166-2
    codes, HASC codes and WDPA ids."""
    return (
        WikidataLink(
            "P8714", "admin", wikidata_identifier=r"regexp_replace(id, '_\d+$', '')"
        ),
        WikidataLink("P298", "admin", applies_to="adm_level = 0"),
        WikidataLink("P300", "admin", "iso_1", "adm_level = 1"),
        WikidataLink("P8119", "admin", "hasc_1", "adm_level = 1"),
        WikidataLink("P8119", "admin", "hasc_2", "adm_level = 2"),
        WikidataLink("P809", "protected_area", "site_id"),
    )
