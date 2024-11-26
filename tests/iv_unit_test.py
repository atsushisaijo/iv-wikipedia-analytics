import pytest
from src.iv_wikipedia_etl import ApiConfig, IvWikipediaData

"""
unit testing has been applied over the configuration parameter 
"""


class TestWikiConfig:
    """
    test if configuration is overridden
    """
    def test_default_config(self):
        config = ApiConfig()
        assert config.base_url == "https://en.wikipedia.org/w/api.php"
        assert config.limit == 500
        assert config.tumbling_window_size == 30

    def test_custom_config(self):
        """test if overriding works"""
        config = ApiConfig(
            limit=400,
            tumbling_window_size=29
        )
        assert config.limit == 400
        assert config.tumbling_window_size == 29

class TestIvWikipediaData:
    """
    test if parameter is always created as expected
    """
    @pytest.fixture
    def iv_pipeline(self):
        """create only test instance"""
        return IvWikipediaData()

    def test_get_api_params(self, iv_pipeline):
        params = iv_pipeline._get_api_params("2024-10-31T00:30:00Z", "2024-10-31T00:00:00Z")
        assert params["rcstart"] == "2024-10-31T00:30:00Z"
        assert params["rcend"] == "2024-10-31T00:00:00Z"
        assert params["rclimit"] == 500
