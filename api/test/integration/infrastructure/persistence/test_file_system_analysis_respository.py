import pytest
import uuid
import shutil
from app.infrastructure.persistence.file_system_analysis_repository import FileSystemAnalysisRepository
from app.models.common.analysis import AnalysisStatus
from app.domain.models.analysis import Analysis

TEST_DIRECTORY = 'integration_tests'
DUMMY_UUID = uuid.UUID('c9787f41-b194-4589-ae53-f45ef290ce6f')

class TestLoadingAnalysis:
    @pytest.fixture(autouse=True)
    def cleanup_before_each_test(self):
        """Auto-used fixture to clean up /tmp/integration_tests before each test"""
        test_dir = f"/tmp/{TEST_DIRECTORY}"
        try:
            shutil.rmtree(test_dir)  # Recursively delete directory
        except FileNotFoundError:
            pass  # Ignore if directory doesn't exist
        yield  # Test runs here


    @pytest.mark.asyncio
    async def test_analysis_is_empty_if_doesnt_exist(self):
        analysis_repository = FileSystemAnalysisRepository(TEST_DIRECTORY)
        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)
        assert analysis_result == Analysis(result=None, metadata=None, status=AnalysisStatus.pending)

    @pytest.mark.asyncio
    async def test_store_saved_analysis_for_first_time(self):
        analysis_repository = FileSystemAnalysisRepository(TEST_DIRECTORY)
        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={ "val1": 12, "val2": "test", "val3": { "key": "value"}},
                status=AnalysisStatus.saved
            )
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=["data1", "data2"],
            metadata={ "val1": 12, "val2": "test", "val3": { "key": "value"}},
            status=AnalysisStatus.saved
        )

    @pytest.mark.asyncio
    async def test_store_analysis_twice_does_not_append_data(self):
        analysis_repository = FileSystemAnalysisRepository(TEST_DIRECTORY)
        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={ "val1": 12, "val2": "test", "val3": { "key": "value"}},
                status=AnalysisStatus.saved
            )
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={ "val1": 12, "val2": "test", "val3": { "key": "value"}},
                status=AnalysisStatus.saved
            )
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=["data1", "data2"],
            metadata={ "val1": 12, "val2": "test", "val3": { "key": "value"}},
            status=AnalysisStatus.saved
        )

