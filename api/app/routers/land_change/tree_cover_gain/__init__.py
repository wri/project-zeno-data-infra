from .tree_cover_gain import create

create.__doc__ = """
    # Primary Dataset
    | Dataset Key Aspects | Detailed Description |
    |---------------------|----------------------|
    | **Title** | Tree cover gain |
    | **Description** <br> A descriptive summary of what the dataset is and key details, such as information on geographic coverage, temporal range, and the data sources used. |   |
    | **Function / usage notes** <br> The intended use of the dataset. How might users employ this data in order to have some impact in the world? |  |
    | **Providers**<br/>Who created the data? |  |
    | **Citation**<br/>How should people cite this dataset? |  |
    | **Methodology** <br> How was this dataset created? |  |
    | **Cautions**<br>What should be kept in mind when using this dataset? |    |
    | **Resolution** |  |
    | **Geographic Coverage** |  |
    | **Update Frequency** |  |
    | **Content Date** |  |
    | **Keywords**<br/>A list of keyword tags related to this dataset so that the LLM better identify contexts where it is relevant. | tree cover gain |

    -----

    **Key Features:**
    - 🆔 Deterministic UUID generation using SHA-1 hashing (UUIDv5)
    - 💾 Automatic payload storage in temporary storage
    - 🔗 Returns a URL to check analytics status
    - ♻️ Idempotent: Identical payloads return the same resource ID

    **Flow:**
    1. Accepts `TreeCoverGainAnalyticsIn` payload
    2. Generates UUID based on payload content
    3. Stores payload as JSON file
    4. Returns resource URL for status checking
    """
