from .tree_cover import create

create.__doc__ = """
    # Primary Dataset
    | Dataset Key Aspects | Detailed Description |
    |---------------------|----------------------|
    | **Title** | Tree Cover |
    | **Description** <br> A descriptive summary of what the dataset is and key details, such as information on geographic coverage, temporal range, and the data sources used. | Tree Cover (Hansen/UMD/GLAD) provides global percent tree canopy cover at 30-meter resolution for the year 2000, based on Landsat 7 imagery. It represents the density of vegetation over 5 meters tall, including both natural forests and plantations. |
    | **Function / usage notes** <br> The intended use of the dataset. How might users employ this data in order to have some impact in the world? | This dataset is useful for establishing historical baselines and comparing tree cover density across different landscapes. |
    | **Providers**<br/>Who created the data? | UMD/Google/USGS/NASA |
    | **Citation**<br/>How should people cite this dataset? | Hansen, M. C., P. V. Potapov, R. Moore, M. Hancher, S. A. Turubanova, A. Tyukavina, D. Thau, S. V. Stehman, S. J. Goetz, T. R. Loveland, A. Kommareddy, A. Egorov, L. Chini, C. O. Justice, and J. R. G. Townshend. 2013. “High-Resolution Global Maps of 21st-Century Forest Cover Change.” Science 342 (15 November): 850–53. doi: 10.1126/science.1244693 |
    | **Methodology** <br> How was this dataset created? | Data in this layer were generated using multispectral satellite imagery from the Landsat 7 thematic mapper plus (ETM+) sensor. The clear surface observations from over 600,000 images were analyzed using Google Earth Engine, a cloud platform for earth observation and data analysis, to determine per pixel tree cover using a supervised learning algorithm. |
    | **Cautions**<br>What should be kept in mind when using this dataset? |  For the purpose of this study, “tree cover” was defined as all vegetation taller than 5 meters in height. “Tree cover” is the biophysical presence of trees and may take the form of natural forests or plantations existing over a range of canopy densities. |
    | **Resolution** | 30 × 30 meters |
    | **Geographic Coverage** | Global land (excluding Antarctica and Arctic islands) |
    | **Update Frequency** |  |
    | **Content Date** | 2000 |
    | **Keywords**<br/>A list of keyword tags related to this dataset so that the LLM better identify contexts where it is relevant. | tree cover |

    -----

    **Key Features:**
    - 🆔 Deterministic UUID generation using SHA-1 hashing (UUIDv5)
    - 💾 Automatic payload storage in temporary storage
    - 🔗 Returns a URL to check analytics status
    - ♻️ Idempotent: Identical payloads return the same resource ID

    **Flow:**
    1. Accepts `TreeCoverAnalyticsIn` payload
    2. Generates UUID based on payload content
    3. Stores payload as JSON file
    4. Returns resource URL for status checking
    """  # noqa: E501
