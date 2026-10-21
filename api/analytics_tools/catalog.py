from dataclasses import dataclass
from typing import Any

ANALYSIS_PATH_SUFFIX = "/analytics"


class UnknownDatasetError(ValueError):
    pass


@dataclass(frozen=True)
class DatasetSummary:
    name: str
    title: str


@dataclass(frozen=True)
class DatasetDetail:
    name: str
    title: str
    description: str
    path: str
    request_schema: dict[str, Any]


class ApiCatalog:
    """Datasets offered by the analytics API, read from its OpenAPI document.

    Each endpoint that creates an analysis is one dataset, named after the
    path segment before the analysis suffix.
    """

    def __init__(self, openapi_document: dict[str, Any]):
        self.openapi_document = openapi_document
        self.creation_operations = self.find_creation_operations()

    def datasets(self) -> list[DatasetSummary]:
        summaries = []
        for name, (_, operation) in self.creation_operations.items():
            summaries.append(DatasetSummary(name, operation.get("summary", "")))
        return summaries

    def describe(self, name: str) -> DatasetDetail:
        if name not in self.creation_operations:
            available = ", ".join(self.creation_operations)
            raise UnknownDatasetError(
                f"Unknown dataset '{name}'. Available datasets: {available}"
            )
        path, operation = self.creation_operations[name]
        schema = operation["requestBody"]["content"]["application/json"]["schema"]
        return DatasetDetail(
            name=name,
            title=operation.get("summary", ""),
            description=operation.get("description", ""),
            path=path,
            request_schema=self.inline_references(schema, set()),
        )

    def find_creation_operations(self) -> dict[str, tuple[str, dict[str, Any]]]:
        operations = {}
        for path, path_item in self.openapi_document.get("paths", {}).items():
            if path.endswith(ANALYSIS_PATH_SUFFIX) and "post" in path_item:
                name = path.removesuffix(ANALYSIS_PATH_SUFFIX).rsplit("/", 1)[-1]
                operations[name] = (path, path_item["post"])
        return operations

    def inline_references(self, node: Any, visiting: set[str]) -> Any:
        """Replace schema references with their definitions so a client sees
        one self-contained schema. Recursive references are left as-is."""
        if isinstance(node, list):
            return [self.inline_references(item, visiting) for item in node]
        if not isinstance(node, dict):
            return node

        reference = node.get("$ref")
        if reference is not None and reference not in visiting:
            return self.inline_references(
                self.resolve(reference), visiting | {reference}
            )

        inlined = {}
        for key, value in node.items():
            inlined[key] = self.inline_references(value, visiting)
        return inlined

    def resolve(self, reference: str) -> Any:
        node: Any = self.openapi_document
        for part in reference.removeprefix("#/").split("/"):
            node = node[part]
        return node
