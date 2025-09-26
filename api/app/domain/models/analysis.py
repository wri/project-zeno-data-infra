class Analysis:
    def __init__(self, result, metadata, status):
        self.result = result
        self.metadata = metadata
        self.status = status

    def __eq__(self, other):
        if not isinstance(other, Analysis):
            return NotImplemented  # Or raise TypeError
        return (
            self.result == other.result
            and self.metadata == other.metadata
            and self.status == other.status
        )

    def __repr__(self):
        return f"Analytics({self.result}, {self.metadata}, {self.status})"
