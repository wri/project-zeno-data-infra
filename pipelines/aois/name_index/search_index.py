import pyarrow as pa
import tantivy


def name_analyzers(word_pattern: str) -> dict[str, tantivy.TextAnalyzer]:
    """How names are compared, ignoring case and accents: "words" splits text
    into the words `word_pattern` matches, and "whole" keeps it as a single
    term. Search builds the same analyzers from the index's metadata."""
    return {
        "words": folded(tantivy.Tokenizer.regex(word_pattern)),
        "whole": folded(tantivy.Tokenizer.raw()),
    }


def folded(tokenizer: tantivy.Tokenizer) -> tantivy.TextAnalyzer:
    return (
        tantivy.TextAnalyzerBuilder(tokenizer)
        .filter(tantivy.Filter.lowercase())
        .filter(tantivy.Filter.ascii_fold())
        .build()
    )


def search_schema() -> tantivy.Schema:
    """Names and alternate names are indexed both whole, to match the whole
    search text, and as words. Importance orders results."""
    builder = tantivy.SchemaBuilder()
    builder.add_text_field("aoi_type", stored=True, tokenizer_name="raw")
    builder.add_text_field("aoi_id", stored=True, tokenizer_name="raw")
    builder.add_text_field("name", stored=True, tokenizer_name="whole")
    builder.add_text_field("name_words", tokenizer_name="words")
    builder.add_text_field("display_name", stored=True, tokenizer_name="words")
    builder.add_text_field("alternate_names", tokenizer_name="whole")
    builder.add_text_field("alternate_words", tokenizer_name="words")
    builder.add_float_field("importance", stored=True, fast=True)
    builder.add_float_field("area_ha", stored=True, fast=True)
    return builder.build()


def write_search_index(records: pa.Table, directory: str, word_pattern: str) -> str:
    """Writes a Tantivy index of the AOI name records to a local directory."""
    index = tantivy.Index(search_schema(), path=directory)
    for name, analyzer in name_analyzers(word_pattern).items():
        index.register_tokenizer(name, analyzer)
    writer = index.writer(heap_size=1_000_000_000, num_threads=1)
    for batch in records.to_batches():
        for record in batch.to_pylist():
            writer.add_document(search_document(record))
    writer.commit()
    writer.wait_merging_threads()
    return directory


def search_document(record: dict) -> tantivy.Document:
    return tantivy.Document(
        aoi_type=record["aoi_type"],
        aoi_id=record["aoi_id"],
        name=record["name"],
        name_words=record["name"],
        display_name=record["display_name"],
        alternate_names=record["alternate_names"],
        alternate_words=record["alternate_names"],
        importance=record["importance"],
        area_ha=record["area_ha"] or 0.0,
    )
